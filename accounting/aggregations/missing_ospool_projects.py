import sys
import json
import argparse
import importlib

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path
from functools import lru_cache

from functions import get_ospool_aps, get_topology_project_data, send_email, OSPOOL_COLLECTORS, NON_OSPOOL_RESOURCES

import elasticsearch
from elasticsearch_dsl import Search, A, Q


EMAIL_ARGS = {
    "--from": {"dest": "from_addr", "default": "no-reply@chtc.wisc.edu"},
    "--reply-to": {"default": "ospool-reports@g-groups.wisc.edu"},
    "--to": {"action": "append", "default": []},
    "--cc": {"action": "append", "default": []},
    "--bcc": {"action": "append", "default": []},
    "--smtp-server": {},
    "--smtp-username": {},
    "--smtp-password-file": {"type": Path}
}

ELASTICSEARCH_ARGS = {
    "--es-host": {"default": "localhost:9200"},
    "--es-url-prefix": {},
    "--es-index": {},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}

PROJECT_TOPOLOGY_DATA = get_topology_project_data()
PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE = Path().home() / "JobAccounting" / "ospool-host-map.pkl"

PROJECT_NAME_SCRIPT_SRC = """
String res;
if (doc.containsKey("ProjectName") && doc["ProjectName.keyword"].size() > 0) {
    res = doc["ProjectName.keyword"].value;
} else if (doc.containsKey("projectname") && doc["projectname.keyword"].size() > 0) {
    res = doc["projectname.keyword"].value;
} else {
    res = "ProjectName undefined";
}
emit(res);
"""

RESOURCE_NAME_SCRIPT_SRC = """
String res;
if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
    res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
} else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
    res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
} else {
    res = "Resource name not defined";
}
emit(res);
"""


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--start", type=valid_date)
    parser.add_argument("--end", type=valid_date)

    return parser.parse_args()


def connect(
        es_host="localhost:9200",
        es_user="",
        es_pass="",
        es_use_https=False,
        es_ca_certs=None,
        es_url_prefix=None,
        **kwargs,
    ) -> elasticsearch.Elasticsearch:
    # Returns Elasticsearch client

    # Split off port from host if included
    if ":" in es_host and len(es_host.split(":")) == 2:
        [es_host, es_port] = es_host.split(":")
        es_port = int(es_port)
    elif ":" in es_host:
        print(f"Ambiguous hostname:port in given host: {es_host}")
        sys.exit(1)
    else:
        es_port = 9200
    es_client = {
        "host": es_host,
        "port": es_port
    }

    # Include username and password if both are provided
    if (not es_user) ^ (not es_pass):
        print("Only one of es_user and es_pass have been defined")
        print("Connecting to Elasticsearch anonymously")
    elif es_user and es_pass:
        es_client["http_auth"] = (es_user, es_pass)

    if es_url_prefix:
        es_client["url_prefix"] = es_url_prefix

    # Only use HTTPS if CA certs are given or if certifi is available
    if es_use_https:
        if es_ca_certs is not None:
            es_client["ca_certs"] = str(es_ca_certs)
        elif importlib.util.find_spec("certifi") is not None:
            pass
        else:
            print("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
            sys.exit(1)
        es_client["use_ssl"] = True
        es_client["verify_certs"] = True

    es_client.update(kwargs)

    return elasticsearch.Elasticsearch([es_client])


def get_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("range", RemoteWallClockTime={"gt": 0}) \
                .query(~Q("terms", JobUniverse=[7, 12]))

    runtime_mappings = {
        "runtime_mappings": {
            "ProjectNameFixed": {
                "type": "keyword",
                "script": {
                    "source": PROJECT_NAME_SCRIPT_SRC,
                }
            },
            "ResourceName": {
                "type": "keyword",
                "script": {
                    "source": RESOURCE_NAME_SCRIPT_SRC,
                }
            }
        }
    }
    query.update_from_dict(runtime_mappings)

    # filter out jobs that did not run in the OSPool
    is_ospool_job = \
        (
            Q("terms", ScheddName__keyword=list(get_ospool_aps(include_jupyter_aps=False, pickled_ap_collector_hosts_cache=PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE))) |
            Q("terms", LastRemotePool__keyword=list(OSPOOL_COLLECTORS))
        ) & ~(
            Q("terms", ResourceName=list(NON_OSPOOL_RESOURCES)) |
            Q("exists", field="TargetAnnexName") |
            Q("term", ResourceName="Local Job")
        )
    query = query.query(is_ospool_job)

    last_seen_agg = A(
        "max",
        field="RecordTime",
    )

    aps_agg = A(
        "terms",
        field="ScheddName.keyword",
        size=8,
    )

    project_name_agg = A(
        "terms",
        field="ProjectNameFixed",
        size=512,
    )
    project_name_agg.metric("last_seen", last_seen_agg)
    project_name_agg.bucket("aps", aps_agg)

    query.aggs.bucket("project_name", project_name_agg)

    return query


def print_error(d, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")


def convert_buckets_to_dict(buckets: list):
    bucket_data = {}
    for bucket in buckets:
        row = {}
        bucket_name = "UNKNOWN"
        if not isinstance(bucket, dict):
            bucket = bucket.to_dict()
        for key, value in bucket.items():
            if key == "key":
                bucket_name = value
            elif key == "doc_count":
                row["value"] = value
            elif isinstance(value, dict) and "value" in value:
                row[key] = value["value"]
            elif isinstance(value, dict) and "buckets" in value:
                row[key] = convert_buckets_to_dict(value["buckets"])
        bucket_data[bucket_name] = row
    return bucket_data


def main():
    args = parse_args()
    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args.pop("es_password_file").open().read().rstrip()
    index = es_args.pop("es_index", "adstash-ospool-transfer-*")

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)
    days = (args.end - args.start).days

    es = connect(**es_args, timeout=30 + int(10 * (days**0.75)))
    es.info()

    base_query = get_query(
        client=es,
        index=index,
        start=args.start,
        end=args.end
    )

    print(f"{datetime.now()} - Running queries")
    try:
        result = base_query.execute()
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err
    print(f"{datetime.now()} - Done.")

    project_name_data = convert_buckets_to_dict(result.aggregations.project_name.buckets)

    missing_projects = []
    for project_name, d in project_name_data.items():
        if project_name.lower() not in PROJECT_TOPOLOGY_DATA:
            missing_projects.append({
                "project_name": project_name,
                "jobs": d["value"],
                "last_seen": datetime.fromtimestamp(int(d["last_seen"])),
                "num_aps": len(d["aps"]),
                "ap_list": ", ".join(list(d["aps"].keys()))
            })

    missing_projects.sort(key=itemgetter("last_seen"), reverse=True)

    text_style = "border: 1px solid black"
    numeric_style = "text-align: right; border: 1px solid black"
    lr = "\n"
    tab = "\t"
    html_table = []
    html_table.append('<table style="border-collapse: collapse">')
    html_table.append("\t<tr>")
    table_header = ["Last Seen", "Project", "Num Jobs", "Num APs", "AP List"]
    html_table.append(f'''{tab}{tab}<th style="{text_style}">{f'</th>{lr}{tab}{tab}<th style="{text_style}">'.join(table_header)}</th>{lr}''')
    html_table.append("\t</tr>")
    for missing_project in missing_projects:
        html_table.append("\t<tr>")
        html_table.append(f'\t\t<td style="{numeric_style}">{missing_project["last_seen"].strftime(r"%Y-%m-%d")}</td>')
        html_table.append(f'\t\t<td style="{text_style}">{missing_project["project_name"]}</td>')
        html_table.append(f'\t\t<td style="{numeric_style}">{missing_project["jobs"]}</td>')
        html_table.append(f'\t\t<td style="{numeric_style}">{missing_project["num_aps"]}</td>')
        html_table.append(f'\t\t<td style="{text_style}">{missing_project["ap_list"]}</td>')
        html_table.append("\t</tr>")
    html_table.append("</table>")

    html = f"<html>\n<head></head>\n<body>\n{lr.join(html_table)}\n</body>\n</html>"

    send_email(
        subject=f"OSPool Unmapped Projects {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
        from_addr=args.from_addr,
        to_addrs=args.to,
        html=html,
        cc_addrs=args.cc,
        bcc_addrs=args.cc,
        reply_to_addr=args.reply_to,
        smtp_server=args.smtp_server,
        smtp_username=args.smtp_username,
        smtp_password_file=args.smtp_password_file,
    )


if __name__ == "__main__":
    main()

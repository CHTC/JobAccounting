import sys
import json
import argparse
import importlib

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email

import elasticsearch
from elasticsearch_dsl import Search, A


EMAIL_ARGS = {
    "--from": {"dest": "from_addr", "default": "no-reply@chtc.wisc.edu"},
    "--reply-to": {"default": "chtc-reports@g-groups.wisc.edu"},
    "--to": {"action": "append", "default": []},
    "--cc": {"action": "append", "default": []},
    "--bcc": {"action": "append", "default": []},
    "--smtp-server": {},
    "--smtp-username": {},
    "--smtp-password-file": {"type": Path}
}

ELASTICSEARCH_ARGS = {
    "--es-host": {},
    "--es-url-prefix": {},
    "--es-index": {},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    },
    "--es-timeout": {"type": int},
}

UWDF_FILES_TRANSFERRED_SRC = r"""
long files = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (doc.containsKey(key) && doc[key + ".keyword"].size() > 0) {
    def matcher = /PelicanFilesCountTotal\D+(\d+)[,}]/i.matcher(doc[key + ".keyword"].value);
    while (matcher.find()) {
      files += Long.parseLong(matcher.group(1));
    }
  }
}
emit(files);
"""

UWDF_GB_TRANSFERRED_SRC = r"""
double gb = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (doc.containsKey(key) && doc[key + ".keyword"].size() > 0) {
    def matcher = /PelicanSizeBytesTotal\D+(\d+)[,}]/i.matcher(doc[key + ".keyword"].value);
    while (matcher.find()) {
      gb += (double)Long.parseLong(matcher.group(1)) / 1e9;
    }
  }
}
emit(gb);
"""

PROJECT_NAME_NORMALIZE_SRC = r"""
String project = "UNKNOWN";
for (key in ["ProjectName", "Projectname", "projectname"]) {
  if (doc.containsKey(key) && doc[key + ".keyword"].size() > 0) {
    project = doc[key + ".keyword"].value;
    break;
  }
}
emit(project);
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
                .filter("wildcard", TransferInput__keyword="*pelican://chtc.wisc.edu/*") \
                .filter("range", NumJobStarts={"gt": 0}) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \

    runtime_mappings = {
        "runtime_mappings": {
            "FilesTransferred": {
                "type": "long",
                "script": {
                    "source": UWDF_FILES_TRANSFERRED_SRC,
                }
            },
            "GbTransferred": {
                "type": "double",
                "script": {
                    "source": UWDF_GB_TRANSFERRED_SRC,
                }
            },
            "ProjectNameNormed": {
                "type": "keyword",
                "script": {
                    "source": PROJECT_NAME_NORMALIZE_SRC,
                }
            }
        }
    }
    query.update_from_dict(runtime_mappings)

    project_agg = A("terms", field="ProjectNameNormed", size=512)
    files_agg = A("sum", field="FilesTransferred")
    gb_agg = A("sum", field="GbTransferred")

    project_agg.metric("files", files_agg)
    project_agg.metric("gb", gb_agg)

    query.aggs.bucket("project", project_agg)

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
    index = es_args.pop("es_index", "chtc-schedd-*")

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)
    days = (args.end - args.start).days

    es_args["timeout"] = es_args.pop("es_timeout", None)
    if not es_args["timeout"]:
        es_args["timeout"] = 60 + int(10 * (days**0.75))
    es = connect(**es_args)
    es.info()

    query = get_query(client=es, index=index, start=args.start, end=args.end)

    print(f"{datetime.now()} - Running query")
    try:
        result = query.execute()
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err
    print(f"{datetime.now()} - Done.")

    projects = convert_buckets_to_dict(result.aggregations.project.buckets)

    data = []
    total = {
        "project": "TOTALS",
        "jobs": 0,
        "files_transferred": 0,
        "gb_transferred": 0,
    }
    for project, d in projects.items():
        row = {
            "project": project,
            "jobs": int(d["value"]),
            "files_transferred": int(d["files"]),
            "gb_transferred": d["gb"],
        }
        for k, v in row.items():
            if k == "project":
                continue
            total[k] += v
        data.append(row)
    data.append(total)
    data.sort(key=itemgetter("files_transferred"), reverse=True)

    css = """
    h1 {text-align: center;}
    table {border-collapse: collapse;}
    th, td {border: 1px solid black;}
    td.text {text-align: left;}
    td.num {text-align: right;}
"""
    html = ["<html>"]

    html.append("<head>")
    html.append(f"<style>{css}</style>")
    html.append("</head>")

    html.append("<body>")

    html.append(f"<h1>UWDF usage from CHTC jobs completed {args.start} to {args.end}</h1>")

    cols = ["project", "files_transferred", "gb_transferred", "jobs"]
    hdrs = ["Project", "Files"            , "GBs"           , "Jobs"]
    fmts = ["s"      , ",d"               , ",.3f"          , ",d"]
    stys = ["text" if fmt == "s" else "num" for fmt in fmts]

    hdrs = dict(zip(cols, hdrs))
    fmts = dict(zip(cols, fmts))
    stys = dict(zip(cols, stys))

    html.append('<table>')

    html.append("\t<tr>")
    for col in cols:
        html.append(f"\t\t<th>{hdrs[col]}</th>")
    html.append("\t</tr>")
    for row in data:
        html.append('\t<tr>')
        for col in cols:
            try:
                html.append(f'\t\t<td class="{stys[col]}">{row[col]:{fmts[col]}}</td>')
            except ValueError:
                html.append(f"\t\t<td>{row[col]}</td>")
        html.append("\t</tr>")
    html.append("</table>")

    html.append("</body>")

    html.append("</html>")

    send_email(
        subject=f"{(args.end - args.start).days}-day CHTC UWDF Usage Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
        from_addr=args.from_addr,
        to_addrs=args.to,
        html="\n".join(html),
        cc_addrs=args.cc,
        bcc_addrs=args.cc,
        reply_to_addr=args.reply_to,
        smtp_server=args.smtp_server,
        smtp_username=args.smtp_username,
        smtp_password_file=args.smtp_password_file,
    )


if __name__ == "__main__":
    main()

import sys
import time
import json
import argparse
import importlib

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email, get_uwdf_director_servers

import elasticsearch
from elasticsearch_dsl import Search, A, Q


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

JOB_ID_SCRIPT_SRC = """
    long cluster_id = 0;
    long proc_id = 0;
    String schedd = "UNKNOWN";
    String job_id;
    if (doc.containsKey("ClusterId")) {
        cluster_id = doc["ClusterId"].value;
    }
    if (doc.containsKey("ProcId")) {
        proc_id = doc["ProcId"].value;
    }
    if (doc.containsKey("ScheddName")) {
        schedd = doc["ScheddName"].value;
    }
    job_id = String.format("%s#%d.%d", new def[] {schedd, cluster_id, proc_id});
    emit(job_id.hashCode());
"""

UWDF_DIRECTOR_SERVERS = {}


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
    parser.add_argument("--cache-dir", type=Path, default=Path())

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


# def connect8(
#         es_host="localhost:9200",
#         es_user="",
#         es_pass="",
#         es_use_https=False,
#         es_ca_certs=None,
#         es_url_prefix=None,
#         **kwargs,
#     ) -> elasticsearch.Elasticsearch:
#     # Returns Elasticsearch client

#     es_client = {"hosts": f"http{'s' if es_use_https else ''}://{es_host}{f'/{es_url_prefix}' if es_url_prefix else ''}"}

#     # Include username and password if both are provided
#     if (not es_user) ^ (not es_pass):
#         print("Only one of es_user and es_pass have been defined")
#         print("Connecting to Elasticsearch anonymously")
#     elif es_user and es_pass:
#         es_client["basic_auth"] = (es_user, es_pass,)

#     # Only use HTTPS if CA certs are given or if certifi is available
#     if es_use_https:
#         if es_ca_certs is not None:
#             es_client["ca_certs"] = str(es_ca_certs)
#         elif importlib.util.find_spec("certifi") is not None:
#             pass
#         else:
#             print("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
#             sys.exit(1)

#     es_client.update(kwargs)
#     return elasticsearch.Elasticsearch(**es_client)


def get_endpoint_types(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> dict:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("term", TransferProtocol="pelican") \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("exists", field="Endpoint") \
                .query(~Q("term", Endpoint=""))
    if start > datetime(2025, 4, 18):  # added indexing to TransferUrl after 2025-04-18
        query = query.query(Q("prefix", TransferUrl__indexed="pelican://chtc.wisc.edu/"))
    else:
        raise RuntimeError("Cannot look up TransferUrls starting with pelican://chtc.wisc.edu before 2025-04-18")
    endpoint_agg = A(
        "terms",
        field="Endpoint",
        size=128,
    )
    transfer_type_agg = A(
        "terms",
        field="TransferType",
        size=2,
    )
    endpoint_agg.bucket("transfer_type", transfer_type_agg)
    query.aggs.bucket("endpoint", endpoint_agg)

    try:
        result = query.execute()
        time.sleep(1)
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err

    endpoints = {bucket["key"]: bucket for bucket in result.aggregations.endpoint.buckets}
    endpoint_types = {"cache": set(), "origin": set()}
    for endpoint, bucket in endpoints.items():
        endpoint_type = UWDF_DIRECTOR_SERVERS.get(f"https://{endpoint}", {"type": ""}).get("type", "")
        if (
            endpoint_type.lower() == "origin" or
            "origin" in endpoint.split(".")[0] or
            "upload" in [xbucket["key"] for xbucket in bucket.transfer_type.buckets]
        ):
            endpoint_types["origin"].add(endpoint)
        else:
            endpoint_types["cache"].add(endpoint)

    return endpoint_types


def get_endpoint_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("term", TransferProtocol="pelican") \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("exists", field="Endpoint") \
                .query(~Q("term", Endpoint=""))
    if start > datetime(2025, 4, 18):  # added indexing to TransferUrl after 2025-04-18
        query = query.query(Q("prefix", TransferUrl__indexed="pelican://chtc.wisc.edu/"))
    else:
        raise RuntimeError("Cannot look up TransferUrls starting with pelican://chtc.wisc.edu before 2025-04-18")

    runtime_mappings = {
        "runtime_mappings": {
            "JobId": {
                "type": "long",
                "script": {
                    "source": JOB_ID_SCRIPT_SRC,
                }
            }
        }
    }
    query.update_from_dict(runtime_mappings)

    return query


def get_director_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("term", TransferProtocol="pelican") \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("term", FinalAttempt=True)
    if start > datetime(2025, 4, 18):  # added indexing to TransferUrl after 2025-04-18
        query = query.query(Q("prefix", TransferUrl__indexed="pelican://chtc.wisc.edu/"))
    else:
        raise RuntimeError("Cannot look up TransferUrls starting with pelican://chtc.wisc.edu before 2025-04-18")

    runtime_mappings = {
        "runtime_mappings": {
            "JobId": {
                "type": "long",
                "script": {
                    "source": JOB_ID_SCRIPT_SRC,
                }
            }
        }
    }
    query.update_from_dict(runtime_mappings)

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


if __name__ == "__main__":
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

    UWDF_DIRECTOR_SERVERS = get_uwdf_director_servers(cache_file=args.cache_dir / "uwdf_director_servers.pickle")

    es_args["timeout"] = es_args.pop("es_timeout", None)
    if not es_args["timeout"]:
        es_args["timeout"] = 60 + int(10 * (days**0.75))
    es = connect(**es_args)
    es.info()

    endpoint_types = get_endpoint_types(
        client=es,
        index=index,
        start=args.start,
        end=args.end,
    )

    base_endpoint_query = get_endpoint_query(
        client=es,
        index=index,
        start=args.start,
        end=args.end
    )

    base_director_query = get_director_query(
        client=es,
        index=index,
        start=args.start,
        end=args.end,
    )

    endpoint_agg = A(
        "terms",
        field="Endpoint",
        size=64,
    )

    transfer_type_agg = A(
        "terms",
        field="TransferType",
        size=2,
    )

    num_jobs_agg = A(
        "cardinality",
        field="JobId",
        precision_threshold=16384,
    )

    endpoint_agg.metric("unique_jobs", num_jobs_agg)

    transfer_type_agg.metric("endpoint", endpoint_agg)
    transfer_type_agg.metric("unique_jobs", num_jobs_agg)

    uwdf_filter = Q("terms", Endpoint=list(endpoint_types["cache"] | endpoint_types["origin"]))
    success_filter = Q("term", TransferSuccess=True)
    final_attempt_failure_filter = Q("term", FinalAttempt=True) & Q("term", TransferSuccess=False)
    all_attempt_failure_filter = Q("term", FinalAttempt=False) | final_attempt_failure_filter
    filenotfound_attempt_failure_filter = Q("term", DebugErrorType="Specification.FileNotFound")
    no_endpoint_filter = ~Q("exists", field="Endpoint")

    base_endpoint_query = base_endpoint_query.query(uwdf_filter)

    success_query = base_endpoint_query.query(success_filter)

    all_attempt_failure_query = base_endpoint_query.query(all_attempt_failure_filter)

    final_attempt_failure_query = base_endpoint_query.query(final_attempt_failure_filter)

    filenotfound_attempt_failure_query = all_attempt_failure_query.query(filenotfound_attempt_failure_filter)

    final_filenotfound_attempt_failure_query = final_attempt_failure_query.query(filenotfound_attempt_failure_filter)

    notfilenotfound_attempt_failure_query = all_attempt_failure_query.query(~filenotfound_attempt_failure_filter)

    final_notfilenotfound_attempt_failure_query = final_attempt_failure_query.query(~filenotfound_attempt_failure_filter)

    director_failure_query = base_director_query.query(no_endpoint_filter)

    base_endpoint_query.aggs.bucket("transfer_type", transfer_type_agg)
    base_endpoint_query.aggs.metric("unique_jobs", num_jobs_agg)

    success_query.aggs.bucket("transfer_type", transfer_type_agg)
    success_query.aggs.metric("unique_jobs", num_jobs_agg)

    final_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    final_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    all_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    all_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    filenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    filenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    final_filenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    final_filenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    notfilenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    notfilenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    final_notfilenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    final_notfilenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    base_director_query.aggs.bucket("transfer_type", transfer_type_agg)
    base_director_query.aggs.metric("unique_jobs", num_jobs_agg)

    director_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    director_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    print(f"{datetime.now()} - Running queries")
    try:
        all_attempts = base_endpoint_query.execute()
        time.sleep(1)

        success_attempts = success_query.execute()
        time.sleep(1)

        final_failed_attempts = final_attempt_failure_query.execute()
        time.sleep(1)

        all_failed_attempts = all_attempt_failure_query.execute()
        time.sleep(1)

        filenotfound_failed_attempts = filenotfound_attempt_failure_query.execute()
        time.sleep(1)

        final_filenotfound_failed_attempts = final_filenotfound_attempt_failure_query.execute()
        time.sleep(1)

        notfilenotfound_failed_attempts = notfilenotfound_attempt_failure_query.execute()
        time.sleep(1)

        final_notfilenotfound_failed_attempts = final_notfilenotfound_attempt_failure_query.execute()
        time.sleep(1)

        director_all_attempts = base_director_query.execute()
        time.sleep(1)

        director_failed_attempts = director_failure_query.execute()
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err
    print(f"{datetime.now()} - Done.")

    all_transfer_type_data = convert_buckets_to_dict(all_attempts.aggregations.transfer_type.buckets)

    success_transfer_type_data = convert_buckets_to_dict(success_attempts.aggregations.transfer_type.buckets)

    final_failed_transfer_type_data = convert_buckets_to_dict(final_failed_attempts.aggregations.transfer_type.buckets)

    all_failed_transfer_type_data = convert_buckets_to_dict(all_failed_attempts.aggregations.transfer_type.buckets)

    filenotfound_failed_transfer_type_data = convert_buckets_to_dict(filenotfound_failed_attempts.aggregations.transfer_type.buckets)

    final_filenotfound_failed_transfer_type_data = convert_buckets_to_dict(final_filenotfound_failed_attempts.aggregations.transfer_type.buckets)

    notfilenotfound_failed_transfer_type_data = convert_buckets_to_dict(notfilenotfound_failed_attempts.aggregations.transfer_type.buckets)

    final_notfilenotfound_failed_transfer_type_data = convert_buckets_to_dict(final_notfilenotfound_failed_attempts.aggregations.transfer_type.buckets)

    director_all_transfer_type_data = convert_buckets_to_dict(director_all_attempts.aggregations.transfer_type.buckets)

    director_failed_transfer_type_data = convert_buckets_to_dict(director_failed_attempts.aggregations.transfer_type.buckets)

    empty_row = {"value": 0, "unique_jobs": 0}

    endpoint_data = {"download": [], "upload": []}
    for transfer_type, transfer_type_data in all_transfer_type_data.items():
        for endpoint in transfer_type_data["endpoint"]:
            server_info = UWDF_DIRECTOR_SERVERS.get(f"https://{endpoint}")
            endpoint_institution = ""
            if server_info:
                endpoint_name = server_info.get("name")
            else:
                endpoint_name = "Not currently found*"
            row = {
                "endpoint": endpoint,
                "endpoint_institution": endpoint_institution,
                "endpoint_name": endpoint_name,
                "endpoint_type": UWDF_DIRECTOR_SERVERS.get(f"https://{endpoint}", {"type": ""}).get("type", "") or "Cache*",
            }
            for attempt_type, attempt_data in {
                "total_attempts": all_transfer_type_data,
                "success_attempts": success_transfer_type_data,
                "final_failed_attempts": final_failed_transfer_type_data,
                "all_failed_attempts": all_failed_transfer_type_data,
                "filenotfound_failed_attempts": filenotfound_failed_transfer_type_data,
                "final_filenotfound_failed_attempts": final_filenotfound_failed_transfer_type_data,
                "notfilenotfound_failed_attempts": notfilenotfound_failed_transfer_type_data,
                "final_notfilenotfound_failed_attempts": final_notfilenotfound_failed_transfer_type_data,
            }.items():
                try:
                    row[attempt_type] = attempt_data[transfer_type]["endpoint"][endpoint]["value"]
                    row[f"{attempt_type}_jobs"] = attempt_data[transfer_type]["endpoint"][endpoint]["unique_jobs"]
                except KeyError:
                    row[attempt_type] = 0
                    row[f"{attempt_type}_jobs"] = 0
            row["total_final_attempts"] = row["final_failed_attempts"] + row["success_attempts"]
            row["pct_failed_attempts"] = row["all_failed_attempts"] / max(row["total_attempts"], row["all_failed_attempts"], 1)
            row["pct_failed_attempts_not404"] = row["notfilenotfound_failed_attempts"] / max(row["total_attempts"], row["notfilenotfound_failed_attempts"], 1)
            row["pct_failed_attempts_404"] = row["filenotfound_failed_attempts"] / max(row["total_attempts"], row["filenotfound_failed_attempts"], 1)
            row["pct_jobs_affected_not404"] = row["final_notfilenotfound_failed_attempts"] / max(row["total_final_attempts"], row["final_notfilenotfound_failed_attempts"], 1)
            row["pct_jobs_affected_404"] = row["final_filenotfound_failed_attempts"] / max(row["total_final_attempts"], row["final_filenotfound_failed_attempts"], 1)
            row["pct_uniq_jobs_affected"] = row["all_failed_attempts_jobs"] / max(row["total_attempts_jobs"], row["all_failed_attempts_jobs"], 1)
            endpoint_data[transfer_type].append(row)

    endpoint_data_totals = {}
    for transfer_type in ("download", "upload"):
        endpoint_data_totals[transfer_type] = {
            "endpoint": "",
            "endpoint_institution": "",
            "endpoint_name": "TOTALS",
            "endpoint_type": "",
        }
        for attempt_type, attempt_data in {
            "total_attempts": all_transfer_type_data,
            "success_attempts": success_transfer_type_data,
            "final_failed_attempts": final_failed_transfer_type_data,
            "all_failed_attempts": all_failed_transfer_type_data,
            "filenotfound_failed_attempts": filenotfound_failed_transfer_type_data,
            "final_filenotfound_failed_attempts": final_filenotfound_failed_transfer_type_data,
            "notfilenotfound_failed_attempts": notfilenotfound_failed_transfer_type_data,
            "final_notfilenotfound_failed_attempts": final_notfilenotfound_failed_transfer_type_data,
            "director_attempts": director_all_transfer_type_data,
            "director_failed_attempts": director_failed_transfer_type_data,
        }.items():
            try:
                endpoint_data_totals[transfer_type][attempt_type] = attempt_data[transfer_type]["value"]
                endpoint_data_totals[transfer_type][f"{attempt_type}_jobs"] = attempt_data[transfer_type]["unique_jobs"]
            except KeyError:
                endpoint_data_totals[transfer_type][attempt_type] = 0
                endpoint_data_totals[transfer_type][f"{attempt_type}_jobs"] = 0
        endpoint_data_totals[transfer_type]["total_final_attempts"] = endpoint_data_totals[transfer_type]["final_failed_attempts"] + endpoint_data_totals[transfer_type]["success_attempts"]
        endpoint_data_totals[transfer_type]["pct_failed_attempts"] = endpoint_data_totals[transfer_type]["all_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_attempts"], endpoint_data_totals[transfer_type]["all_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_failed_attempts_not404"] = endpoint_data_totals[transfer_type]["notfilenotfound_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_attempts"], endpoint_data_totals[transfer_type]["notfilenotfound_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_failed_attempts_404"] = endpoint_data_totals[transfer_type]["filenotfound_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_attempts"], endpoint_data_totals[transfer_type]["filenotfound_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_jobs_affected_not404"] = endpoint_data_totals[transfer_type]["final_notfilenotfound_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_final_attempts"], endpoint_data_totals[transfer_type]["final_notfilenotfound_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_jobs_affected_404"] = endpoint_data_totals[transfer_type]["final_filenotfound_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_final_attempts"], endpoint_data_totals[transfer_type]["final_filenotfound_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_uniq_jobs_affected"] = endpoint_data_totals[transfer_type]["all_failed_attempts_jobs"] / max(endpoint_data_totals[transfer_type]["total_attempts_jobs"], endpoint_data_totals[transfer_type]["all_failed_attempts_jobs"], 1)
        endpoint_data_totals[transfer_type]["pct_failed_director_attempts"] = endpoint_data_totals[transfer_type]["director_failed_attempts"] / max(endpoint_data_totals[transfer_type]["director_attempts"], endpoint_data_totals[transfer_type]["director_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["failed_director_attempts_per_job"] = endpoint_data_totals[transfer_type]["director_failed_attempts"] / max(endpoint_data_totals[transfer_type]["director_attempts_jobs"], endpoint_data_totals[transfer_type]["director_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_jobs_affected_director"] = endpoint_data_totals[transfer_type]["director_failed_attempts_jobs"] / max(endpoint_data_totals[transfer_type]["director_attempts_jobs"], endpoint_data_totals[transfer_type]["director_failed_attempts_jobs"], 1)
        endpoint_data[transfer_type].sort(key=itemgetter("total_attempts"), reverse=True)
        endpoint_data[transfer_type].insert(0, endpoint_data_totals[transfer_type])

    warn_threshold = 0.05
    err_threshold = 0.15

    styles = {
        "h1": "text-align: center",
        "table": "border-collapse: collapse",
        "td": "border: 1px solid black",
        "td.text": "text-align: left",
        "td.numeric": "text-align: right",
        "tr.warn": "background-color: #ffc",
        "tr.err": "background-color: #fcc",
    }

    html = ["<html>"]

    html.append("<head>")
    html.append("</head>")

    html.append("<body>")

    html.append(f"<h1 style={styles['h1']}>CHTC UWDF report from {args.start} to {args.end}</h1>")

    html.append(f'<span style="color: yellow">Yellow</span> rows where Percent Failed Attempts (Non-404 for input transfer) are above {warn_threshold:.0%}</span><br>')
    html.append(f'<span style="color: red">Red</span> rows where Percent Failed Attempts (Non-404 for input transfer) are above {err_threshold:.0%}</span><br>')
    html.append('<span style="font-weight: bold">*Not currently found</span> means that the endpoint was not reporting to the director at the time the report was generated.')

    ### ENDPOINT DOWNLOAD TABLE

    html.append("<h2>Per UWDF endpoint download (i.e. input transfer) statistics</h2>")

    columns = [
        ("Endpoint Name", "endpoint_name", "s"),
        ("Total Attempts", "total_attempts", ",d"),
        ("Total Uniq Jobs", "total_attempts_jobs", ",d"),
        ("Successful Transfers", "success_attempts", ",d"),
        ("Uniq Jobs w/1+ Success", "success_attempts_jobs", ",d"),
        ("Non-404 Failed Attempts", "notfilenotfound_failed_attempts", ",d"),
        ("Non-404 Pct Attempts Failed", "pct_failed_attempts_not404", ".1%"),
        ("Non-404 Pct Jobs Interrupted", "pct_jobs_affected_not404", ".1%"),
        ("404 Failed Attempts", "filenotfound_failed_attempts", ",d"),
        ("404 Pct Attempts Failed", "pct_failed_attempts_404", ".1%"),
        ("404 Pct Jobs Interrupted", "pct_jobs_affected_404", ".1%"),
        ("Total Failed Attempts", "all_failed_attempts", ",d"),
        ("Total Jobs Interrupted", "final_failed_attempts", ",d"),
        ("Non-404 Jobs Interrupted", "final_notfilenotfound_failed_attempts", ",d"),
        ("404 Jobs Interrupted", "final_filenotfound_failed_attempts", ",d"),
        ("Endpoint Hostname", "endpoint", "s"),
        ("Endpoint Type", "endpoint_type", "s"),
    ]
    columns = [{
            "name": name,
            "key": key,
            "format": fmt,
            "style": "td.text" if fmt == "s" else "td.numeric",
        } for name, key, fmt in columns]

    html.append(f'''<table style="{styles['table']}">''')

    html.append("\t<tr>")
    for col in columns:
        html.append(f'''\t\t<th style="{styles["td"]}">{col['name']}</th>''')
    html.append("\t</tr>")
    for row in endpoint_data["download"]:
        row_style = ""
        if row["pct_failed_attempts_not404"] > err_threshold:
            row_style = styles["tr.err"]
        elif row["pct_failed_attempts_not404"] > warn_threshold:
            row_style = styles["tr.warn"]
        html.append(f'\t<tr style="{row_style}">')
        for col in columns:
            try:
                html.append(f'''\t\t<td style="{styles['td']}; {styles[col["style"]]}">{row[col["key"]]:{col["format"]}}</td>''')
            except ValueError:
                html.append(f'''\t\t<td style="{styles['td']}">{row[col["key"]]}</td>''')
        html.append("\t</tr>")
    html.append("</table>")

    ### ENDPOINT UPLOAD TABLE

    html.append("<h2>Per UWDF origin upload (i.e. output transfer) statistics</h2>")

    columns = [
        ("Origin Name", "endpoint_name", "s"),
        ("Total Attempts", "total_attempts", ",d"),
        ("Total Uniq Jobs", "total_attempts_jobs", ",d"),
        ("Successful Transfers", "success_attempts", ",d"),
        ("Uniq Jobs w/1+ Success", "success_attempts_jobs", ",d"),
        ("Total Failed Attempts", "all_failed_attempts", ",d"),
        ("Pct Attempts Failed", "pct_failed_attempts", ".1%"),
        ("Uniq Jobs Interrupted", "final_failed_attempts_jobs", ",d"),
        ("Pct Uniq Jobs Interrupted", "pct_uniq_jobs_affected", ".1%"),
        ("Origin Hostname", "endpoint", "s"),
    ]
    columns = [{
            "name": name,
            "key": key,
            "format": fmt,
            "style": "td.text" if fmt == "s" else "td.numeric",
        } for name, key, fmt in columns]

    html.append(f'''<table style="{styles['table']}">''')

    html.append("\t<tr>")
    for col in columns:
        html.append(f'''\t\t<th style="{styles["td"]}">{col['name']}</th>''')
    html.append("\t</tr>")
    for row in endpoint_data["upload"]:
        row_style = ""
        if row["pct_failed_attempts"] > err_threshold:
            row_style = styles["tr.err"]
        elif row["pct_failed_attempts"] > warn_threshold:
            row_style = styles["tr.warn"]
        html.append(f'\t<tr style="{row_style}">')
        for col in columns:
            try:
                html.append(f'''\t\t<td style="{styles['td']}; {styles[col["style"]]}">{row[col["key"]]:{col["format"]}}</td>''')
            except ValueError:
                html.append(f'''\t\t<td style="{styles['td']}">{row[col["key"]]}</td>''')
        html.append("\t</tr>")
    html.append("</table>")

    ### DIRECTOR DOWNLOAD TABLE

    html.append("<h2><strong>UWDF director</strong> download (i.e. input transfer) statistics</h2>")

    columns = [
        ("", "endpoint_name", "s"),
        ("Contact Attempts", "director_attempts", ",d"),
        ("Total Uniq Jobs", "director_attempts_jobs", ",d"),
        ("Failed Contact Attempts", "director_failed_attempts", ",d"),
        ("Pct Contact Attempts Failed", "pct_failed_director_attempts", ".1%"),
        ("Uniq Jobs Interrupted", "director_failed_attempts_jobs", ",d"),
        ("Pct Uniq Jobs Interrupted", "pct_jobs_affected_director", ".1%"),
    ]
    columns = [{
            "name": name,
            "key": key,
            "format": fmt,
            "style": "td.text" if fmt == "s" else "td.numeric",
        } for name, key, fmt in columns]

    html.append(f'''<table style="{styles['table']}">''')

    html.append("\t<tr>")
    for col in columns:
        html.append(f'''\t\t<th style="{styles["td"]}">{col['name']}</th>''')
    html.append("\t</tr>")
    for row in endpoint_data["download"]:
        if not (row["endpoint_name"] == "TOTALS"):
            continue
        row_style = ""
        if row["pct_failed_director_attempts"] > err_threshold:
            row_style = styles["tr.err"]
        elif row["pct_failed_director_attempts"] > warn_threshold:
            row_style = styles["tr.warn"]
        html.append(f'\t<tr style="{row_style}">')
        for col in columns:
            try:
                html.append(f'''\t\t<td style="{styles['td']}; {styles[col["style"]]}">{row[col["key"]]:{col["format"]}}</td>''')
            except ValueError:
                html.append(f'''\t\t<td style="{styles['td']}">{row[col["key"]]}</td>''')
        html.append("\t</tr>")
    html.append("</table>")

    ### DIRECTOR UPLOAD TABLE

    html.append("<h2><strong>UWDF director</strong> upload (i.e. output transfer) statistics</h2>")

    html.append('<p>Note: Some of these errors could be from the user specifying a missing output file.')

    columns = [
        ("", "endpoint_name", "s"),
        ("Contact Attempts", "director_attempts", ",d"),
        ("Total Uniq Jobs", "director_attempts_jobs", ",d"),
        ("Failed Contact Attempts", "director_failed_attempts", ",d"),
        ("Pct Contact Attempts Failed", "pct_failed_director_attempts", ".1%"),
        ("Uniq Jobs Interrupted", "director_failed_attempts_jobs", ",d"),
        ("Pct Uniq Jobs Interrupted", "pct_jobs_affected_director", ".1%"),
    ]
    columns = [{
            "name": name,
            "key": key,
            "format": fmt,
            "style": "td.text" if fmt == "s" else "td.numeric",
        } for name, key, fmt in columns]

    html.append(f'''<table style="{styles['table']}">''')

    html.append("\t<tr>")
    for col in columns:
        html.append(f'''\t\t<th style="{styles["td"]}">{col['name']}</th>''')
    html.append("\t</tr>")
    for row in endpoint_data["upload"]:
        if not (row["endpoint_name"] == "TOTALS"):
            continue
        row_style = ""
        if row["pct_failed_director_attempts"] > err_threshold:
            row_style = styles["tr.err"]
        elif row["pct_failed_director_attempts"] > warn_threshold:
            row_style = styles["tr.warn"]
        html.append(f'\t<tr style="{row_style}">')
        for col in columns:
            try:
                html.append(f'''\t\t<td style="{styles['td']}; {styles[col["style"]]}">{row[col["key"]]:{col["format"]}}</td>''')
            except ValueError:
                html.append(f'''\t\t<td style="{styles['td']}">{row[col["key"]]}</td>''')
        html.append("\t</tr>")
    html.append("</table>")

    html.append("<p>All transfer attempts in this report refer to <strong>UWDF object</strong> transfer attempts</p>")

    html.append("""<p>Currently restricted to CHTC APs that flock to the OSPool, see e.g.:<br>
<span style="font-family: monospace; font-size: 6pt; white-space: pre;"><pre style="font-size: 6pt">$ condor_status -pool cm-1.ospool.osg-htc.org -schedd -const 'regexp(".*\.chtc\.wisc\.edu", CollectorHost)'</pre></style></p>""")

    legend = {
        "Total Attempts": "Number of transfer attempts (multiple transfer attempts can be made per object per plugin invocation)",
        "Total Uniq Jobs": "Number of unique job IDs that attempted at least one transfer",
        "Successful Transfers": "Number of transfer attempts that succeeded",
        "Uniq Jobs w/1+ Success": "Number of unique job IDs that had at least one successful transfer attempt",
        "Total Failed Attempts": "Number of transfer attempts that failed (multiple failed transfer attempts can be made per object per plugin invocation)",
        "Pct Attempts Failed": "Total Failed Attempts / Total Attempts (as a percentage)",
        "Total Jobs Interrupted": "Number of final transfer attempts that failed (i.e. number of jobs start/end attempts that failed due to file transfer error)",
        "Contact Attempts": "Number of UWDF plugin invocations, each of which attempt to contact the director",
        "Failed Contact Attempts": "Number of UWDF plugin invocations that failed to contact the director",
        "Pct Contact Attempts Failed": "Failed Contact Attempts / Contact Attempts (as a percentage)",
        "Uniq Jobs Interrupted": "Number of unique job IDs that logged at least one failed transfer (or director contact) attempt that resulted in the job activation being terminated (i.e. the transfer plugin ran out of retries)",
        "Pct Uniq Jobs Interrupted": "Uniq Jobs Interrupted / Total Uniq Jobs (as a percentage)",
        "Non-404 Failed Attempts": "Number of transfer attempts that failed due to an error other than FileNotFound",
        "Non-404 Pct Attempts Failed": "Non-404 Failed Attempts / Total Attempts (as a percentage)",
        "Non-404 Jobs Interrupted": "Number of final transfer attempts that failed due to an error other than FileNotFound",
        "Non-404 Pct Jobs Interrupted": "Non-404 Jobs Interrupted / Total Jobs Interrupted (as a percentage)",
        "404 Failed Attempts": "Number of transfer attempts that failed due to FileNotFound",
        "404 Pct Attempts Failed": "404 Failed Attempts / Total Attempts (as a percentage)",
        "404 Jobs Interrupted": "Number of final transfer attempts that failed due to FileNotFound",
        "404 Pct Jobs Interrupted": "404 Jobs Interrupted / Total Jobs Interrupted (as a percentage)",
    }
    html.append("<ul>")
    for term, definition in legend.items():
        html.append(f"\t<li><strong>{term}</strong>: {definition}</li>")
    html.append("</ul>")

    html.append("</body>")

    html.append("</html>")

    send_email(
        subject=f"{(args.end - args.start).days}-day CHTC UWDF Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
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

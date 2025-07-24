import sys
import time
import json
import argparse
import importlib

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email, get_osdf_director_servers, get_topology_resource_data

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

OSDF_DIRECTOR_SERVERS = {}
TOPOLOGY_RESOURCE_DATA = {}


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
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("exists", field="Endpoint") \
                .query(~Q("term", Endpoint=""))
    if start > datetime(2025, 4, 18):  # added indexing to TransferUrl after 2025-04-18
        query = query.query(Q("prefix", TransferUrl__indexed="osdf://") | Q("prefix", TransferUrl__indexed="pelican://osg-htc.org"))
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
        endpoint_type = OSDF_DIRECTOR_SERVERS.get(f"https://{endpoint}", {"type": ""}).get("type", "")
        if (
            endpoint_type.lower() == "origin" or
            "origin" in endpoint.split(".")[0] or
            "upload" in [xbucket["key"] for xbucket in bucket.transfer_type.buckets]
        ):
            endpoint_types["origin"].add(endpoint)
        else:
            endpoint_types["cache"].add(endpoint)

    return endpoint_types


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
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("exists", field="Endpoint") \
                .query(~Q("term", Endpoint=""))
    if start > datetime(2025, 4, 18):  # added indexing to TransferUrl after 2025-04-18
        query = query.query(Q("prefix", TransferUrl__indexed="osdf://") | Q("prefix", TransferUrl__indexed="pelican://osg-htc.org"))

    # filter out CHTC jobs that did not run in the OSPool
    has_resource_name = Q("exists", field="machineattrglidein_resourcename0.indexed") & ~Q("term", machineattrglidein_resourcename0__indexed="Undefined")
    chtc_local = (Q("wildcard", ScheddName="*.wisc.edu") | Q("wildcard", ScheddName="*.glbrc.org") | Q("wildcard", ScheddName="*.nmrbox.org")) & ~has_resource_name
    query = query.query(~chtc_local)

    # filter out weird resource names
    query = query.query(~Q("term", machineattrglidein_resourcename0__indexed="2"))

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

    OSDF_DIRECTOR_SERVERS = get_osdf_director_servers(cache_file=args.cache_dir / "osdf_director_servers.pickle")
    TOPOLOGY_RESOURCE_DATA = get_topology_resource_data(cache_file=args.cache_dir / "topology_resource_data.pickle")

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

    base_query = get_query(
        client=es,
        index=index,
        start=args.start,
        end=args.end
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

    resource_name_agg = A(
        "terms",
        field="machineattrglidein_resourcename0",
        size=256,
        missing="UNKNOWN",
    )

    endpoint_agg.metric("unique_jobs", num_jobs_agg)

    resource_name_agg.metric("unique_jobs", num_jobs_agg)

    transfer_type_agg.metric("resource_name", resource_name_agg)
    transfer_type_agg.metric("endpoint", endpoint_agg)
    transfer_type_agg.metric("unique_jobs", num_jobs_agg)

    osdf_filter = Q("terms", Endpoint=list(endpoint_types["cache"] | endpoint_types["origin"]))
    success_filter = Q("term", TransferSuccess=True)
    final_attempt_failure_filter = Q("term", FinalAttempt=True) & Q("term", TransferSuccess=False)
    all_attempt_failure_filter = Q("term", FinalAttempt=False) | final_attempt_failure_filter
    filenotfound_attempt_failure_filter = Q("term", DebugErrorType="Specification.FileNotFound")

    base_query = base_query.query(osdf_filter)
    success_query = base_query.query(success_filter)

    final_attempt_failure_query = base_query.query(final_attempt_failure_filter)

    all_attempt_failure_query = base_query.query(all_attempt_failure_filter)

    filenotfound_attempt_failure_query = base_query.query(filenotfound_attempt_failure_filter)

    final_filenotfound_attempt_failure_query = filenotfound_attempt_failure_query.query(final_attempt_failure_filter)

    base_query.aggs.bucket("transfer_type", transfer_type_agg)
    base_query.aggs.bucket("resource_name", resource_name_agg)
    base_query.aggs.metric("unique_jobs", num_jobs_agg)

    success_query.aggs.bucket("transfer_type", transfer_type_agg)
    success_query.aggs.bucket("resource_name", resource_name_agg)
    success_query.aggs.metric("unique_jobs", num_jobs_agg)

    final_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    final_attempt_failure_query.aggs.bucket("resource_name", resource_name_agg)
    final_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    all_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    all_attempt_failure_query.aggs.bucket("resource_name", resource_name_agg)
    all_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    filenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    filenotfound_attempt_failure_query.aggs.bucket("resource_name", resource_name_agg)
    filenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    final_filenotfound_attempt_failure_query.aggs.bucket("transfer_type", transfer_type_agg)
    final_filenotfound_attempt_failure_query.aggs.bucket("resource_name", resource_name_agg)
    final_filenotfound_attempt_failure_query.aggs.metric("unique_jobs", num_jobs_agg)

    print(f"{datetime.now()} - Running queries")
    try:
        all_attempts = base_query.execute()
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
    except Exception as err:
        try:
            print_error(err.info)
        except Exception:
            pass
        raise err
    print(f"{datetime.now()} - Done.")

    all_transfer_type_data = convert_buckets_to_dict(all_attempts.aggregations.transfer_type.buckets)
    all_resource_name_data = convert_buckets_to_dict(all_attempts.aggregations.resource_name.buckets)

    success_transfer_type_data = convert_buckets_to_dict(success_attempts.aggregations.transfer_type.buckets)
    success_resource_name_data = convert_buckets_to_dict(success_attempts.aggregations.resource_name.buckets)

    final_failed_transfer_type_data = convert_buckets_to_dict(final_failed_attempts.aggregations.transfer_type.buckets)
    final_failed_resource_name_data = convert_buckets_to_dict(final_failed_attempts.aggregations.resource_name.buckets)

    all_failed_transfer_type_data = convert_buckets_to_dict(all_failed_attempts.aggregations.transfer_type.buckets)
    all_failed_resource_name_data = convert_buckets_to_dict(all_failed_attempts.aggregations.resource_name.buckets)

    filenotfound_failed_transfer_type_data = convert_buckets_to_dict(filenotfound_failed_attempts.aggregations.transfer_type.buckets)
    filenotfound_failed_resource_name_data = convert_buckets_to_dict(filenotfound_failed_attempts.aggregations.resource_name.buckets)

    final_filenotfound_failed_transfer_type_data = convert_buckets_to_dict(final_filenotfound_failed_attempts.aggregations.transfer_type.buckets)
    final_filenotfound_failed_resource_name_data = convert_buckets_to_dict(final_filenotfound_failed_attempts.aggregations.resource_name.buckets)

    empty_row = {"value": 0, "unique_jobs": 0}

    endpoint_data = {"download": [], "upload": []}
    for transfer_type, transfer_type_data in all_transfer_type_data.items():
        for endpoint in transfer_type_data["endpoint"]:
            server_info = OSDF_DIRECTOR_SERVERS.get(f"https://{endpoint}")
            endpoint_institution = ""
            if server_info:
                endpoint_name = server_info.get("name")
                if endpoint_name:
                    endpoint_institution = TOPOLOGY_RESOURCE_DATA.get(endpoint_name.lower(), {"institution": f"Unmapped endpoint {endpoint_name}"})["institution"]
                else:
                    endpoint_name = "Unnamed endpoint"
            else:
                endpoint_name = "Not currently found*"
            row = {
                "endpoint": endpoint,
                "endpoint_institution": endpoint_institution,
                "endpoint_name": endpoint_name,
                "endpoint_type": OSDF_DIRECTOR_SERVERS.get(f"https://{endpoint}", {"type": ""}).get("type", "") or "Cache*",
            }
            for attempt_type, attempt_data in {
                "total_attempts": all_transfer_type_data,
                "success_attempts": success_transfer_type_data,
                "final_failed_attempts": final_failed_transfer_type_data,
                "all_failed_attempts": all_failed_transfer_type_data,
                "filenotfound_failed_attempts": filenotfound_failed_transfer_type_data,
                "final_filenotfound_failed_attempts": final_filenotfound_failed_transfer_type_data,
            }.items():
                try:
                    row[attempt_type] = attempt_data[transfer_type]["endpoint"][endpoint]["value"]
                    row[f"{attempt_type}_jobs"] = attempt_data[transfer_type]["endpoint"][endpoint]["unique_jobs"]
                except KeyError:
                    row[attempt_type] = 0
                    row[f"{attempt_type}_jobs"] = 0
            row["pct_failed_attempts"] = row["all_failed_attempts"] / max(row["total_attempts"], row["all_failed_attempts"], 1)
            row["pct_failed_attempts_404"] = row["filenotfound_failed_attempts"] / max(row["all_failed_attempts"], 1)
            row["failed_attempts_per_job"] = row["all_failed_attempts"] / max(row["total_attempts_jobs"], row["all_failed_attempts"], 1)
            row["pct_jobs_affected"] = row["final_failed_attempts_jobs"] / max(row["total_attempts_jobs"], row["final_failed_attempts_jobs"], 1)
            row["pct_jobs_affected_404"] = row["final_filenotfound_failed_attempts_jobs"] / max(row["final_failed_attempts_jobs"], 1)
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
        }.items():
            try:
                endpoint_data_totals[transfer_type][attempt_type] = attempt_data[transfer_type]["value"]
                endpoint_data_totals[transfer_type][f"{attempt_type}_jobs"] = attempt_data[transfer_type]["unique_jobs"]
            except KeyError:
                endpoint_data_totals[transfer_type][attempt_type] = 0
                endpoint_data_totals[transfer_type][f"{attempt_type}_jobs"] = 0
        endpoint_data_totals[transfer_type]["pct_failed_attempts"] = endpoint_data_totals[transfer_type]["all_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_attempts"], endpoint_data_totals[transfer_type]["all_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_failed_attempts_404"] = endpoint_data_totals[transfer_type]["filenotfound_failed_attempts"] / max(endpoint_data_totals[transfer_type]["all_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["failed_attempts_per_job"] = endpoint_data_totals[transfer_type]["all_failed_attempts"] / max(endpoint_data_totals[transfer_type]["total_attempts_jobs"], endpoint_data_totals[transfer_type]["all_failed_attempts"], 1)
        endpoint_data_totals[transfer_type]["pct_jobs_affected"] = endpoint_data_totals[transfer_type]["final_failed_attempts_jobs"] / max(endpoint_data_totals[transfer_type]["total_attempts_jobs"], endpoint_data_totals[transfer_type]["final_failed_attempts_jobs"], 1)
        endpoint_data_totals[transfer_type]["pct_jobs_affected_404"] = endpoint_data_totals[transfer_type]["final_filenotfound_failed_attempts_jobs"] / max(endpoint_data_totals[transfer_type]["final_failed_attempts_jobs"], 1)
        endpoint_data[transfer_type].sort(key=itemgetter("total_attempts"), reverse=True)
        endpoint_data[transfer_type].insert(0, endpoint_data_totals[transfer_type])

    resource_name_data = {"download": [], "upload": []}
    for transfer_type, transfer_type_data in all_transfer_type_data.items():
        for resource_name in transfer_type_data["resource_name"]:
            resource_info = TOPOLOGY_RESOURCE_DATA.get(resource_name.lower())
            resource_institution = ""
            if resource_info:
                resource_institution = resource_info.get("institution")
            elif resource_name != "UNKNOWN":
                resource_institution = f"Unmapped resource {resource_name}"
            row = {
                "resource_name": resource_name,
                "resource_institution": resource_institution,
            }
            for attempt_type, attempt_data in {
                "total_attempts": all_transfer_type_data,
                "success_attempts": success_transfer_type_data,
                "final_failed_attempts": final_failed_transfer_type_data,
                "all_failed_attempts": all_failed_transfer_type_data,
                "filenotfound_failed_attempts": filenotfound_failed_transfer_type_data,
                "final_filenotfound_failed_attempts": final_filenotfound_failed_transfer_type_data,
            }.items():
                try:
                    row[attempt_type] = attempt_data[transfer_type]["resource_name"][resource_name]["value"]
                    row[f"{attempt_type}_jobs"] = attempt_data[transfer_type]["resource_name"][resource_name]["unique_jobs"]
                except KeyError:
                    row[attempt_type] = 0
                    row[f"{attempt_type}_jobs"] = 0
            row["pct_failed_attempts"] = row["all_failed_attempts"] / max(row["total_attempts"], row["all_failed_attempts"], 1)
            row["pct_failed_attempts_404"] = row["filenotfound_failed_attempts"] / max(row["all_failed_attempts"], 1)
            row["failed_attempts_per_job"] = row["all_failed_attempts"] / max(row["total_attempts_jobs"], row["all_failed_attempts"], 1)
            row["pct_jobs_affected"] = row["final_failed_attempts_jobs"] / max(row["total_attempts_jobs"], row["final_failed_attempts_jobs"], 1)
            row["pct_jobs_affected_404"] = row["final_filenotfound_failed_attempts_jobs"] / max(row["final_failed_attempts_jobs"], 1)
            resource_name_data[transfer_type].append(row)

    resource_name_data_totals = {}
    for transfer_type in ("download", "upload"):
        resource_name_data_totals[transfer_type] = {
            "resource_name": "TOTALS",
            "resource_institution": "",
        }
        for attempt_type, attempt_data in {
            "total_attempts": all_transfer_type_data,
            "success_attempts": success_transfer_type_data,
            "final_failed_attempts": final_failed_transfer_type_data,
            "all_failed_attempts": all_failed_transfer_type_data,
            "filenotfound_failed_attempts": filenotfound_failed_transfer_type_data,
            "final_filenotfound_failed_attempts": final_filenotfound_failed_transfer_type_data,
        }.items():
            try:
                resource_name_data_totals[transfer_type][attempt_type] = attempt_data[transfer_type]["value"]
                resource_name_data_totals[transfer_type][f"{attempt_type}_jobs"] = attempt_data[transfer_type]["unique_jobs"]
            except KeyError:
                resource_name_data_totals[transfer_type][attempt_type] = 0
                resource_name_data_totals[transfer_type][f"{attempt_type}_jobs"] = 0
        resource_name_data_totals[transfer_type]["pct_failed_attempts"] = resource_name_data_totals[transfer_type]["all_failed_attempts"] / max(resource_name_data_totals[transfer_type]["total_attempts"], resource_name_data_totals[transfer_type]["all_failed_attempts"], 1)
        resource_name_data_totals[transfer_type]["pct_failed_attempts_404"] = resource_name_data_totals[transfer_type]["filenotfound_failed_attempts"] / max(resource_name_data_totals[transfer_type]["all_failed_attempts"], 1)
        resource_name_data_totals[transfer_type]["failed_attempts_per_job"] = resource_name_data_totals[transfer_type]["all_failed_attempts"] / max(resource_name_data_totals[transfer_type]["total_attempts_jobs"], resource_name_data_totals[transfer_type]["all_failed_attempts"], 1)
        resource_name_data_totals[transfer_type]["pct_jobs_affected"] = resource_name_data_totals[transfer_type]["final_failed_attempts_jobs"] / max(resource_name_data_totals[transfer_type]["total_attempts_jobs"], resource_name_data_totals[transfer_type]["final_failed_attempts_jobs"], 1)
        resource_name_data_totals[transfer_type]["pct_jobs_affected_404"] = resource_name_data_totals[transfer_type]["final_filenotfound_failed_attempts_jobs"] / max(resource_name_data_totals[transfer_type]["final_failed_attempts_jobs"], 1)
        resource_name_data[transfer_type].sort(key=itemgetter("pct_failed_attempts"), reverse=True)
        resource_name_data[transfer_type].insert(0, resource_name_data_totals[transfer_type])

    warn_threshold = 0.05
    err_threshold = 0.15

    css = """
    h1 {text-align: center;}
    table {border-collapse: collapse;}
    th, td {border: 1px solid black;}
    td.text {text-align: left;}
    td.num {text-align: right;}
    tr.warn {background-color: #ffc;}
    tr.err {background-color: #fcc;}
"""
    html = ["<html>"]

    html.append("<head>")
    html.append(f"<style>{css}</style>")
    html.append("</head>")

    html.append("<body>")

    html.append(f"<h1>OSPool OSDF report from {args.start} to {args.end}</h1>")

    html.append(f'<span style="color: yellow">Yellow</span> rows where Percent Failed Attempts are above {warn_threshold:.0%}</span><br>')
    html.append(f'<span style="color: red">Red</span> rows where Percent Failed Attempts are above {err_threshold:.0%}</span><br>')
    html.append('<span style="font-weight: bold">*Not currently found</span> means that the endpoint was not reporting to the director at the time the report was generated.')

    ### ENDPOINT DOWNLOAD TABLE

    html.append("<h2>Per OSDF endpoint download (i.e. input transfer) statistics</h2>")

    cols = ["endpoint_name",    "endpoint_institution", "total_attempts", "total_attempts_jobs", "success_attempts",    "success_attempts_jobs", "all_failed_attempts", "filenotfound_failed_attempts", "pct_failed_attempts", "pct_failed_attempts_404", "failed_attempts_per_job", "all_failed_attempts_jobs",    "final_failed_attempts_jobs", "pct_jobs_affected",    "pct_jobs_affected_404",    "endpoint",          "endpoint_type"]
    hdrs = ["Endpoint Name",    "Endpoint Institution", "Total Attempts", "Total Jobs",          "Successful Attempts", "Successful Jobs",       "Failed Attempts",     "404'd Attempts",               "Pct Attempts Failed", "Pct Failed Attempts 404", "Failed Attempts per Job", "Num Jobs w/ Failed Attempts", "Num Jobs Interrupted",       "Pct Jobs Interrupted", "Pct Interrupted Jobs 404", "Endpoint Hostname", "Endpoint Type"]
    fmts = ["s",                "s",                    ",d",             ",d",                  ",d",                  ",d",                    ",d",                  ",d",                           ".1%",                 ".1%",                     ",.2f",                    ",d",                          ",d",                         ".1%",                  ".1%",                      "s",                 "s"]
    stys = ["text" if fmt == "s" else "num" for fmt in fmts]

    hdrs = dict(zip(cols, hdrs))
    fmts = dict(zip(cols, fmts))
    stys = dict(zip(cols, stys))

    html.append('<table>')

    html.append("\t<tr>")
    for col in cols:
        html.append(f"\t\t<th>{hdrs[col]}</th>")
    html.append("\t</tr>")
    for row in endpoint_data["download"]:
        row_class = ""
        if row["pct_failed_attempts"] > err_threshold:
            row_class = "err"
        elif row["pct_failed_attempts"] > warn_threshold:
            row_class = "warn"
        html.append(f'\t<tr class="{row_class}">')
        for col in cols:
            try:
                html.append(f'\t\t<td class="{stys[col]}">{row[col]:{fmts[col]}}</td>')
            except ValueError:
                html.append(f"\t\t<td>{row[col]}</td>")
        html.append("\t</tr>")
    html.append("</table>")

    ### RESOURCE DOWNLOAD TABLE

    html.append("<h2>Per OSPool resource download (i.e. input transfer) statistics</h2>")

    cols = ["resource_name", "resource_institution", "total_attempts", "total_attempts_jobs", "success_attempts",    "success_attempts_jobs", "all_failed_attempts", "filenotfound_failed_attempts", "pct_failed_attempts", "pct_failed_attempts_404", "failed_attempts_per_job", "all_failed_attempts_jobs",    "final_failed_attempts_jobs", "pct_jobs_affected",    "pct_jobs_affected_404"]
    hdrs = ["Resource Name", "Resource Institution", "Total Attempts", "Total Jobs",          "Successful Attempts", "Successful Jobs",       "Failed Attempts",     "404'd Attempts",               "Pct Attempts Failed", "Pct Failed Attempts 404", "Failed Attempts per Job", "Num Jobs w/ Failed Attempts", "Num Jobs Interrupted",       "Pct Jobs Interrupted", "Pct Interrupted Jobs 404"]
    fmts = ["s",             "s",                    ",d",             ",d",                  ",d",                  ",d",                    ",d",                  ",d",                           ".1%",                 ".1%",                     ",.2f",                    ",d",                          ",d",                         ".1%",                  ".1%"]
    stys = ["text" if fmt == "s" else "num" for fmt in fmts]

    hdrs = dict(zip(cols, hdrs))
    fmts = dict(zip(cols, fmts))
    stys = dict(zip(cols, stys))

    html.append('<table>')

    html.append("\t<tr>")
    for col in cols:
        html.append(f"\t\t<th>{hdrs[col]}</th>")
    html.append("\t</tr>")
    for row in resource_name_data["download"]:
        row_class = ""
        if row["pct_failed_attempts"] > err_threshold:
            row_class = "err"
        elif row["pct_failed_attempts"] > warn_threshold:
            row_class = "warn"
        html.append(f'\t<tr class="{row_class}">')
        for col in cols:
            try:
                html.append(f'\t\t<td class="{stys[col]}">{row[col]:{fmts[col]}}</td>')
            except ValueError:
                html.append(f"\t\t<td>{row[col]}</td>")
        html.append("\t</tr>")
    html.append("</table>")

    ### ENDPOINT UPLOAD TABLE

    html.append("<h2>Per OSDF origin upload (i.e. output transfer) statistics</h2>")

    cols = ["endpoint_name", "endpoint_institution", "total_attempts", "total_attempts_jobs", "success_attempts",    "success_attempts_jobs", "all_failed_attempts", "pct_failed_attempts", "failed_attempts_per_job", "all_failed_attempts_jobs",    "final_failed_attempts_jobs", "pct_jobs_affected",    "endpoint"]
    hdrs = ["Origin Name",   "Origin Institution",   "Total Attempts", "Total Jobs",          "Successful Attempts", "Successful Jobs",       "Failed Attempts",     "Pct Attempts Failed", "Failed Attempts per Job", "Num Jobs w/ Failed Attempts", "Num Jobs Interrupted",       "Pct Jobs Interrupted", "Origin Hostname"]
    fmts = ["s",             "s",                    ",d",             ",d",                  ",d",                  ",d",                    ",d",                  ".1%",                 ",.2f",                    ",d",                          ",d",                         ".1%",                  "s"]
    stys = ["text" if fmt == "s" else "num" for fmt in fmts]

    hdrs = dict(zip(cols, hdrs))
    fmts = dict(zip(cols, fmts))
    stys = dict(zip(cols, stys))

    html.append('<table>')

    html.append("\t<tr>")
    for col in cols:
        html.append(f"\t\t<th>{hdrs[col]}</th>")
    html.append("\t</tr>")
    for row in endpoint_data["upload"]:
        row_class = ""
        if row["pct_failed_attempts"] > err_threshold:
            row_class = "err"
        elif row["pct_failed_attempts"] > warn_threshold:
            row_class = "warn"
        html.append(f'\t<tr class="{row_class}">')
        for col in cols:
            try:
                html.append(f'\t\t<td class="{stys[col]}">{row[col]:{fmts[col]}}</td>')
            except ValueError:
                html.append(f"\t\t<td>{row[col]}</td>")
        html.append("\t</tr>")
    html.append("</table>")

    ### RESOURCE UPLOAD TABLE

    html.append("<h2>Per OSPool resource upload (transfer output) statistics</h2>")

    cols = ["resource_name", "resource_institution", "total_attempts", "total_attempts_jobs", "success_attempts",    "success_attempts_jobs", "all_failed_attempts", "pct_failed_attempts", "failed_attempts_per_job", "all_failed_attempts_jobs",    "final_failed_attempts_jobs", "pct_jobs_affected"]
    hdrs = ["Resource Name", "Resource Institution", "Total Attempts", "Total Jobs",          "Successful Attempts", "Successful Jobs",       "Failed Attempts",     "Pct Attempts Failed", "Failed Attempts per Job", "Num Jobs w/ Failed Attempts", "Num Jobs Interrupted",       "Pct Jobs Interrupted"]
    fmts = ["s",             "s",                    ",d",             ",d",                  ",d",                  ",d",                    ",d",                  ".1%",                 ",.2f",                    ",d",                          ",d",                         ".1%"]
    stys = ["text" if fmt == "s" else "num" for fmt in fmts]

    hdrs = dict(zip(cols, hdrs))
    fmts = dict(zip(cols, fmts))
    stys = dict(zip(cols, stys))

    html.append('<table>')

    html.append("\t<tr>")
    for col in cols:
        html.append(f"\t\t<th>{hdrs[col]}</th>")
    html.append("\t</tr>")
    for row in resource_name_data["upload"]:
        row_class = ""
        if row["pct_failed_attempts"] > err_threshold:
            row_class = "err"
        elif row["pct_failed_attempts"] > warn_threshold:
            row_class = "warn"
        html.append(f'\t<tr class="{row_class}">')
        for col in cols:
            try:
                html.append(f'\t\t<td class="{stys[col]}">{row[col]:{fmts[col]}}</td>')
            except ValueError:
                html.append(f"\t\t<td>{row[col]}</td>")
        html.append("\t</tr>")
    html.append("</table>")

    legend = {
        "Total Attempts": "Number of OSDF object transfer attempts (multiple transfer attempts can be made per object per plugin invocation)",
        "Total Jobs": "Number of unique job IDs that attempted at least one OSDF object transfer",
        "Successful Attempts": "Number of OSDF object transfer attempts that succeeded",
        "Successful Jobs": "Number of unique job IDs that had at least one successful OSDF object transfer attempt",
        "Failed Attempts": "Number of OSDF object transfer attempts that failed (multiple failed transfer attempts can be made per object per plugin invocation)",
        "404'd Attempts" : "Number of OSDF object transfer attempts that failed due to FileNotFound error",
        "Pct Attempts Failed": "Failed Attempts / Total Attempts (as a percentage)",
        "Pct Failed Attempts 404": "404'd Attempts / Failed Attempts (as a percentage)",
        "Failed Attempts per Job": "Failed Attempts / Total Jobs",
        "Num Jobs w/ Failed Attempts": "Number of unique job IDs that logged at least one failed OSDF object transfer attempt",
        "Num Jobs Interrupted": "Number of unique job IDs that logged at least one failed OSDF object transfer attempt that resulted in the job activation being terminated (i.e. the transfer plugin ran out of retries)",
        "Pct Jobs Interrupted": "Num Jobs Interrupted / Total Jobs (as a percentage)",
        "Pct Interrupted Jobs 404": "Num Jobs Interrupted due to FileNotFound error / Num Jobs Interrupted(as a percentage)",
    }
    html.append("<ul>")
    for term, definition in legend.items():
        html.append(f"\t<li><strong>{term}</strong>: {definition}</li>")
    html.append("</ul>")

    html.append("</body>")

    html.append("</html>")

    send_email(
        subject=f"{(args.end - args.start).days}-day OSPool OSDF Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
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

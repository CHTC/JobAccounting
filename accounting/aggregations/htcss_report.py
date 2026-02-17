import sys
import json
import argparse
import importlib

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path
from functools import lru_cache

from functions import send_email

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
    "--es-index": {"default": "osg-schedd-*"},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}


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


@lru_cache(maxsize=1)
def get_vacate_reasons(
        client: elasticsearch.Elasticsearch,
        index: str
    ) -> set:

    index_mappings = client.indices.get_field_mapping(
        index=index,
        fields="NumVacatesByReason.*",
    )

    vacate_reasons_sets = [set(index_mapping["mappings"].keys()) for index_mapping in index_mappings.values() if index_mapping["mappings"]]
    vacate_reasons = set()
    for vacate_reasons_set in vacate_reasons_sets:
        vacate_reasons = vacate_reasons | vacate_reasons_set

    # {NumVacatesByReason.TransferInputError, NumVacatesByReason.TransferOutputError, ...}

    remove_reasons = set()
    for vacate_reason in vacate_reasons:
        if vacate_reason.startswith("NumVacatesByReason.TransferInputError") and len(vacate_reason) > len("NumVacatesByReason.TransferInputError"):
            remove_reasons.add(vacate_reason)
        elif vacate_reason.startswith("NumVacatesByReason.TransferOutputError") and len(vacate_reason) > len("NumVacatesByReason.TransferOutputError"):
            remove_reasons.add(vacate_reason)
    vacate_reasons = vacate_reasons - remove_reasons

    return vacate_reasons


@lru_cache(maxsize=1)
def get_vacate_reasons_pre_execution(
        client: elasticsearch.Elasticsearch,
        index: str
    ) -> set:

    index_mappings = client.indices.get_field_mapping(
        index=index,
        fields="NumVacatesByReasonPreExecution.*",
    )

    vacate_reasons_sets = [set(index_mapping["mappings"].keys()) for index_mapping in index_mappings.values() if index_mapping["mappings"]]
    vacate_reasons = set()
    for vacate_reasons_set in vacate_reasons_sets:
        vacate_reasons = vacate_reasons | vacate_reasons_set

    # {NumVacatesByReasonPreExecution.TransferInputError, NumVacatesByReasonPreExecution.TransferOutputError, ...}

    remove_reasons = set()
    for vacate_reason in vacate_reasons:
        if vacate_reason.startswith("NumVacatesByReasonPreExecution.TransferInputError") and len(vacate_reason) > len("NumVacatesByReasonPreExecution.TransferInputError"):
            remove_reasons.add(vacate_reason)
        elif vacate_reason.startswith("NumVacatesByReasonPreExecution.TransferOutputError") and len(vacate_reason) > len("NumVacatesByReasonPreExecution.TransferOutputError"):
            remove_reasons.add(vacate_reason)
    vacate_reasons = vacate_reasons - remove_reasons

    return vacate_reasons


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

    # filter out jobs that did not run in the OSPool
    is_ospool_job = \
        Q("wildcard", ScheddName__keyword="*.osg-htc.org") | \
        Q("terms", ScheddName__keyword=["amundsen.grid.uchicago.edu", "comses.sol.rc.asu.edu", "huxley-osgsub-001.sdmz.amnh.org", "osg-moller.jlab.org", "grid-submitter.icecube.wisc.edu", "osg-solid.jlab.org", "ospool-eht.chtc.wisc.edu", "scott.grid.uchicago.edu"]) | \
        Q("terms", LastRemotePool__keyword=["cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"])
    query = query.query(is_ospool_job)

    shadow_starts_agg = A(
        "sum",
        field="NumShadowStarts",
    )

    job_starts_agg = A(
        "sum",
        field="NumJobStarts",
    )

    condor_version_agg = A(
        "terms",
        field="CondorVersion.keyword",
        size=16,
    )

    num_vacates_pre_execution_agg = A(
        "sum",
        field="NumVacatesPreExecution",
    )

    # resource_name_agg = A(
    #     "terms",
    #     field="MATCH_EXP_JOBGLIDEIN_ResourceName.keyword",
    #     size=256,
    # )
    # resource_name_agg.metric("shadow_starts", shadow_starts_agg)
    # resource_name_agg.metric("job_starts", job_starts_agg)
    # resource_name_agg.bucket("condor_version", condor_version_agg)

    ap_name_agg = A(
        "terms",
        field="ScheddName.keyword",
        size=64,
    )
    ap_name_agg.metric("shadow_starts", shadow_starts_agg)
    ap_name_agg.metric("job_starts", job_starts_agg)
    ap_name_agg.bucket("condor_version", condor_version_agg)
    ap_name_agg.metric("num_vacates_pre_execution", num_vacates_pre_execution_agg)

    for vacate_reason in get_vacate_reasons(client, index):
        vacate_reason_agg = A("sum", field=vacate_reason)
        # resource_name_agg.metric(vacate_reason, vacate_reason_agg)
        ap_name_agg.metric(vacate_reason, vacate_reason_agg)
        query.aggs.metric(vacate_reason, vacate_reason_agg)

    for vacate_reason in get_vacate_reasons_pre_execution(client, index):
        vacate_reason_pre_execution_agg = A("sum", field=vacate_reason)
        ap_name_agg.metric(vacate_reason, vacate_reason_pre_execution_agg)
        query.aggs.metric(vacate_reason, vacate_reason_pre_execution_agg)

    query.aggs.metric("shadow_starts", shadow_starts_agg)
    query.aggs.metric("job_starts", job_starts_agg)
    # query.aggs.bucket("resource_name", resource_name_agg)
    query.aggs.bucket("ap_name", ap_name_agg)
    query.aggs.metric("num_vacates_pre_execution", num_vacates_pre_execution_agg)

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

    # resource_name_data = convert_buckets_to_dict(result.aggregations.resource_name.buckets)
    ap_name_data = convert_buckets_to_dict(result.aggregations.ap_name.buckets)

    vacate_reasons = get_vacate_reasons(es, index)
    vacate_reasons_pre_execution = get_vacate_reasons_pre_execution(es, index)

    # resource_name_metrics = []
    # for resource_name, d in resource_name_data.items():
    #     metrics = {
    #         "Name": resource_name,
    #         "Version": ".".join(str(x) for x in max(tuple(int(x) for x in cv.split()[1].split(".")) for cv in d["condor_version"].keys())),
    #         "Jobs": d["value"],
    #         "% Shadows w/o Exec": 1 - d["job_starts"] / max(d["shadow_starts"], 1),
    #         "Most Common Vacate Reason": "None",
    #         "% MCVR": 0,
    #     }
    #     if tuple(int(x) for x in metrics["Version"].split(".")) >= (24, 11, 1):
    #         if d["shadow_starts"] > d["job_starts"]:
    #             max_vacate_reason, max_vacate_reason_n = max({vr: d[vr] for vr in vacate_reasons}.items(), key=itemgetter(1))
    #             metrics["Most Common Vacate Reason"] = max_vacate_reason.split(".", maxsplit=1)[-1]
    #             metrics["% MCVR"] = max_vacate_reason_n / max(d["shadow_starts"], 1)
    #     else:
    #         metrics["Most Common Vacate Reason"] = "-"
    #     resource_name_metrics.append(metrics)
    # resource_name_metrics.sort(key=itemgetter("% Shadows w/o Exec"), reverse=True)

    ap_name_metrics = []
    for ap_name, d in ap_name_data.items():
        metrics = {
            "Name": ap_name,
            "Version": ".".join(str(x) for x in max(tuple(int(x) for x in cv.split()[1].split(".")) for cv in d["condor_version"].keys())),
            "Jobs": d["value"],
            "% Shadows w/o Exec": 1 - d["job_starts"] / max(d["shadow_starts"], 1),
            "Most Common Vacate Reason": "None",
            "% MCVR": 0,
            "Vacates Pre Execution": d["num_vacates_pre_execution"],
            "Most Common Vacate Reason Pre Execution": "None",
            "% MCVRPE": 0,
        }
        if tuple(int(x) for x in metrics["Version"].split(".")) >= (24, 11, 1):
            if d["shadow_starts"] > d["job_starts"]:
                max_vacate_reason, max_vacate_reason_n = max({vr: d[vr] for vr in vacate_reasons}.items(), key=itemgetter(1))
                metrics["Most Common Vacate Reason"] = max_vacate_reason.split(".", maxsplit=1)[-1]
                metrics["% MCVR"] = max_vacate_reason_n / max(d["shadow_starts"], 1)
        else:
            metrics["Most Common Vacate Reason"] = "-"
        if tuple(int(x) for x in metrics["Version"].split(".")) >= (25, 3, 0):
            if d["num_vacates_pre_execution"] > 0:
                max_vacate_reason_pre_execution, max_vacate_reason_pre_execution_n = max({vr: d[vr] for vr in vacate_reasons_pre_execution}.items(), key=itemgetter(1))
                metrics["Most Common Vacate Reason Pre Execution"] = max_vacate_reason_pre_execution.split(".", maxsplit=1)[-1]
                metrics["% MCVRPE"] = max_vacate_reason_pre_execution_n / max(d["shadow_starts"], 1)
        else:
            metrics["Most Common Vacate Reason Pre Execution"] = "-"
        ap_name_metrics.append(metrics)
    ap_name_metrics.sort(key=itemgetter("Jobs"), reverse=True)

    total_metrics = {
        "Name": "TOTALS",
        "Jobs": result.hits.total.value,
        "% Shadows w/o Exec": 1 - result.aggregations.job_starts.value/max(result.aggregations.shadow_starts.value, 1),
    }

    text = []

    text.append(f"OSPool Shadow/Execution Report {args.start.strftime(r'%Y-%m-%d %H:%M')} to {args.end.strftime(r'%Y-%m-%d %H:%M')}")
    text.append("")

    text.append(f"{'':<22} {'%SnE':>6} {'Jobs':>9}")
    text.append(80*"-")
    text.append(f"{total_metrics['Name']:<22.22} {total_metrics['% Shadows w/o Exec']:>6.1%} {total_metrics['Jobs']:>9,d}")
    text.append("")

    text.append(f"{'AP':<22} {'%SnE':>6} {'Jobs':>9} {'Version':<8} {'Most Common Vac Reason':<23} {'%Shdws':>7}")
    text.append(80*"-")
    for m in ap_name_metrics:
        text.append(f"""{m['Name']:<22.22} {m['% Shadows w/o Exec']:>6.1%} {m['Jobs']:>9,d} {m['Version']:<8} {m['Most Common Vacate Reason']:<24.24} {f'{m["% MCVR"]:>6.1%}' if m['Most Common Vacate Reason'] != "-" else ''}""")
    text.append("")

    text.append(f"{'AP':<22} {'%SnE':>6} {'Jobs':>9} {'Version':<8} {'Most Common VPE Reason':<23} {'%Shdws':>7}")
    text.append(80*"-")
    for m in ap_name_metrics:
        text.append(f"""{m['Name']:<22.22} {m['% Shadows w/o Exec']:>6.1%} {m['Jobs']:>9,d} {m['Version']:<8} {m['Most Common Vacate Reason Pre Execution']:<24.24} {f'{m["% MCVRPE"]:>6.1%}' if m['Most Common Vacate Reason Pre Execution'] != "-" else ''}""")
    text.append("")

    # text.append(f"{'GLIDEIN_ResourceName':<22} {'%SnE':>6} {'Jobs':>9} {'':<8} {'Most Common Reason':<24} {'% Shdws':>6}")
    # text.append(80*"-")
    # for m in resource_name_metrics:
    #     text.append(f"{m['Name']:<22.22} {m['% Shadows w/o Exec']:>6.1%} {m['Jobs']:>9,d} {'':8} {m['Most Common Vacate Reason']:<24.24} {m['% MCVR']:>6.1%}")
    # text.append("")

    text.append("Legend")
    text.append("\t%SnE: Percent of shadow starts that did not result in job execution")
    text.append("\tJobs: Number of completed jobs (i.e. that hit the history file)")
    text.append("\tVersion: Latest version at submit time found for any job on this AP")
    text.append("\tMost Common Vac Reason: Most common vacate reason across all shadows")
    text.append("\tMost Common VPE Reason: Most common vacate reason pre execution across all shadows")
    text.append("\t%Shdws: Percent of all shadows that vacated (total or pre execution) for the most common reason")
    text.append('\t"-": All jobs were submitted pre-24.11.1/25.3.0 and have no NumVacateReasons/PreExecution')

    lr = "\n"
    html = f'''<!DOCTYPE html>
<div style="font-family: monospace; white-space: pre;">
<pre>
{lr.join(text)}
</pre>
</div>
'''

    send_email(
        subject=f"{(args.end - args.start).days}-day OSPool Shadow/Execution Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
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

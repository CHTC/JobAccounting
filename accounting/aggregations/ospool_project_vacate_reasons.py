#!/usr/bin/env python3
"""
Report vacate reasons for a given project over a time period.

Queries job history docs in Elasticsearch, dynamically discovering all
vacate reason subfields from the index mappings for NumVacatesByReason
and NumVacatesByReasonPreExecution. Runs sum aggregations on each
subfield and presents a table sorted by total vacates, with columns
for total, pre-execution, and during-execution counts and percentages.

Also reports summary stats: total jobs finished, NumShadowStarts,
NumJobStarts, NumVacates, and pre-execution vacate counts.

Requires:
elasticsearch>=7,<8
elasticsearch_dsl>=7,<8
tabulate
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import elasticsearch
    from elasticsearch_dsl import Q, Search
    from tabulate import tabulate
except ImportError:
    print("""`elasticsearch` and `elasticsearch_dsl` 7.x libraries and the tabulate library are required,
please consider activating a virtual environment and running:

python3 -m pip install 'elasticsearch>=7,<8' 'elasticsearch_dsl>=7,<8' tabulate
""", file=sys.stderr)
    sys.exit(1)

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


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def connect(
        es_host: str = "localhost:9200",
        es_user: str = "",
        es_pass: str = "",
        es_use_https: bool = False,
        es_ca_certs: str | None = None,
        es_url_prefix: str | None = None,
        es_timeout: int = 120,
        **kwargs,
    ) -> elasticsearch.Elasticsearch:
    """Create and return an Elasticsearch client."""
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
        "port": es_port,
    }

    if (not es_user) ^ (not es_pass):
        print("Only one of es_user and es_pass have been defined")
        print("Connecting to Elasticsearch anonymously")
    elif es_user and es_pass:
        es_client["http_auth"] = (es_user, es_pass)

    if es_url_prefix:
        es_client["url_prefix"] = es_url_prefix

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

    es_client["timeout"] = es_timeout
    es_client.update(kwargs)

    return elasticsearch.Elasticsearch([es_client])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report vacate reasons for a given project over a time period.",
    )

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    parser.add_argument("--start", type=valid_date, default=yesterday, help="Start date (YYYY-MM-DD, default: yesterday)")
    parser.add_argument("--end", type=valid_date, default=today, help="End date (YYYY-MM-DD, default: today)")
    parser.add_argument("project", help="ProjectName to filter on")

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    return parser.parse_args()


def get_vacate_reason_fields(es: elasticsearch.Elasticsearch, index: str) -> tuple[list[str], list[str]]:
    """Fetch index mappings and extract subfields of NumVacatesByReason and NumVacatesByReasonPreExecution."""
    mappings = es.indices.get_mapping(index=index)

    during_reasons = set()
    pre_reasons = set()

    for idx_mapping in mappings.values():
        properties = idx_mapping.get("mappings", {}).get("properties", {})

        nvbr = properties.get("NumVacatesByReason", {})
        for subfield in nvbr.get("properties", {}):
            during_reasons.add(subfield)

        nvbr_pre = properties.get("NumVacatesByReasonPreExecution", {})
        for subfield in nvbr_pre.get("properties", {}):
            pre_reasons.add(subfield)

    return sorted(during_reasons), sorted(pre_reasons)


def get_query(es: elasticsearch.Elasticsearch, index: str, project: str, start: datetime, end: datetime, during_reasons: list[str], pre_reasons: list[str]) -> Search:
    """Build search with sum aggregations for each vacate reason subfield."""
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())

    s = Search(using=es, index=index).extra(size=0, track_scores=False, track_total_hits=True)
    s = s.filter("range", RecordTime={"gte": start_epoch, "lt": end_epoch, "format": "epoch_second"})
    s = s.filter(Q("term", ProjectName=project))
    s = s.filter(~Q("terms", JobUniverse=[7, 12]))

    s.aggs.metric("num_shadow_starts", "sum", field="NumShadowStarts")
    s.aggs.metric("num_job_starts", "sum", field="NumJobStarts")
    s.aggs.metric("num_vacates", "sum", field="NumVacates")
    s.aggs.bucket("job_status", "terms", field="JobStatus")

    for reason in during_reasons:
        s.aggs.metric(
            f"total_{reason}",
            "sum",
            field=f"NumVacatesByReason.{reason}",
        )

    for reason in pre_reasons:
        s.aggs.metric(
            f"pre_{reason}",
            "sum",
            field=f"NumVacatesByReasonPreExecution.{reason}",
        )

    return s


def print_results(result: Search, during_reasons: list[str], pre_reasons: list[str]) -> None:
    """Print vacate reason counts in a readable table.

    NumVacatesByReason is the total across all vacates.
    During-execution = total - pre-execution.
    """
    JOB_STATUS_NAMES = {3: "Removed", 4: "Completed"}

    total_jobs = result.hits.total.value
    shadow_starts = int(result.aggregations.num_shadow_starts["value"] or 0)
    job_starts = int(result.aggregations.num_job_starts["value"] or 0)
    num_vacates = int(result.aggregations.num_vacates["value"] or 0)
    pre_exec_failures = shadow_starts - job_starts

    status_counts = {b["key"]: b["doc_count"] for b in result.aggregations.job_status.buckets}

    print(f"\n  {'Total jobs:':<22} {total_jobs:>15,}")
    for code, name in sorted(JOB_STATUS_NAMES.items()):
        count = status_counts.get(code, 0)
        print(f"    {name + ':':<20} {count:>15,}")
    print(f"  {'NumShadowStarts:':<22} {shadow_starts:>15,}")
    print(f"  {'NumJobStarts:':<22} {job_starts:>15,}")
    print(f"  {'NumVacates:':<22} {num_vacates:>15,}")
    print(f"  {'NumPreExecVacates:':<22} {pre_exec_failures:>15,}")

    headers = ["Vacate Reason", "Total", "%", "PreExec", "%", "During", "%"]
    rows = []

    all_reasons = sorted(set(during_reasons) | set(pre_reasons))

    for reason in all_reasons:
        total_key = f"total_{reason}"
        pre_key = f"pre_{reason}"

        total_val = int(result.aggregations[total_key]["value"] or 0) if reason in during_reasons else 0
        pre_val = int(result.aggregations[pre_key]["value"] or 0) if reason in pre_reasons else 0
        during_val = total_val - pre_val

        during_vacates = num_vacates - pre_exec_failures
        total_pct = 100 * total_val / num_vacates if num_vacates else 0
        pre_pct = 100 * pre_val / pre_exec_failures if pre_exec_failures else 0
        during_pct = 100 * during_val / during_vacates if during_vacates else 0

        if total_val > 0 or pre_val > 0:
            rows.append([
                reason,
                f"{total_val:,}", f"{total_pct:.1f}%",
                f"{pre_val:,}", f"{pre_pct:.1f}%",
                f"{during_val:,}", f"{during_pct:.1f}%",
            ])

    rows.sort(key=lambda r: int(r[1].replace(",", "")), reverse=True)

    print()
    print(tabulate(rows, headers=headers, tablefmt="simple", colalign=("left", "right", "right", "right", "right", "right", "right")))
    print()


def main():
    args = parse_args()

    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args.pop("es_password_file").open().read().rstrip()
    index = es_args.pop("es_index", "path-schedd-*")

    if not es_args.get("es_timeout"):
        days = (args.end - args.start).days
        es_args["es_timeout"] = 60 + int(10 * (days**0.75))

    es = connect(**es_args)
    es.info()

    print(f"Fetching vacate reason fields from index: {index}")
    during_reasons, pre_reasons = get_vacate_reason_fields(es, index)
    print(f"Found {len(during_reasons)} total reasons, {len(pre_reasons)} pre-execution reasons")

    if not during_reasons and not pre_reasons:
        print("No NumVacatesByReason fields found in index mappings.", file=sys.stderr)
        sys.exit(1)

    query = get_query(es, index, args.project, args.start, args.end, during_reasons, pre_reasons)
    result = query.execute()

    print(f"\nVacate reason report for project '{args.project}'")
    print(f"Period: {args.start.strftime('%Y-%m-%d')} to {args.end.strftime('%Y-%m-%d')}")
    print_results(result, during_reasons, pre_reasons)


if __name__ == "__main__":
    main()

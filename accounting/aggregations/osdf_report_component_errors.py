import sys
import time
import json
import argparse
import urllib.request

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email, get_osdf_endpoint_data
from metric_functions import valid_date, connect, print_es_error, EMAIL_ARGS, ELASTICSEARCH_ARGS

import yaml
import elasticsearch
from elasticsearch_dsl import Search, A, Q


ENDPOINT_SECTIONS = {"cache", "origin", "cache-hits", "cache-misses"}

INPUT_COMPONENTS = [
    "cache", "origin", "institution", "namespace", "director404",
    "site", "owner", "project", "ap", "error_type",
    "site_endpoint", "endpoint_namespace", "cache-hits", "cache-misses",
]

OUTPUT_COMPONENTS = [
    "origin", "institution", "namespace", "director404",
    "site", "owner", "project", "ap", "error_type",
    "site_endpoint", "endpoint_namespace",
]

INPUT_DISPLAY_NAMES = {
    "cache": "Source Cache",
    "origin": "Source Origin",
    "institution": "Source Institution",
    "namespace": "Namespace",
    "director404": "Director/404",
    "site": "Target Site",
    "owner": "Owner",
    "project": "Project",
    "ap": "AP",
    "error_type": "Error Type",
    "site_endpoint": "(Source, Target)",
    "endpoint_namespace": "(Source, Namespace)",
    "cache-hits": "Cache (Hits)",
    "cache-misses": "Cache (Misses)",
}

OUTPUT_DISPLAY_NAMES = {
    "origin": "Target Origin",
    "institution": "Target Institution",
    "namespace": "Namespace",
    "director404": "Director/404",
    "site": "Source Site",
    "owner": "Owner",
    "project": "Project",
    "ap": "AP",
    "error_type": "Error Type",
    "site_endpoint": "(Source, Target)",
    "endpoint_namespace": "(Target, Namespace)",
}

NAMESPACE_REGISTRY_URL = "https://osdf-registry.osg-htc.org/api/v1.0/registry_ui/namespaces"
NAMESPACE_CACHE_FILE = Path(__file__).parent / "osdf_namespaces.json"


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
    parser.add_argument("--pelican-error-codes", type=Path, default=Path(__file__).parent / "error_codes.yaml")

    return parser.parse_args()


def get_endpoint_types(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        osdf_endpoint_data: dict = None,
        transfer_type: str = None,
    ) -> dict:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}) \
                .filter("exists", field="Endpoint") \
                .filter(~Q("term", Endpoint="")) \
                .filter(Q("wildcard", TransferUrl="*osdf://*") | Q("wildcard", TransferUrl="*pelican://osg-htc.org*"))

    if transfer_type:
        query = query.filter("term", TransferType=transfer_type)

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
            print_es_error(err.info)
        except Exception:
            pass
        raise err

    endpoints = {bucket["key"]: bucket for bucket in result.aggregations.endpoint.buckets}
    endpoint_types = {"cache": set(), "origin": set()}

    if transfer_type == "upload":
        for endpoint in endpoints:
            endpoint_types["origin"].add(endpoint)
    else:
        for endpoint, bucket in endpoints.items():
            endpoint_type = (osdf_endpoint_data or {}).get(endpoint, {"type": ""}).get("type", "")
            if (
                endpoint_type.lower() == "origin" or
                "origin" in endpoint.split(".")[0] or
                "upload" in [xbucket["key"] for xbucket in bucket.transfer_type.buckets]
            ):
                endpoint_types["origin"].add(endpoint)
            else:
                endpoint_types["cache"].add(endpoint)

    return endpoint_types


def get_namespace_list(cache_file: Path = NAMESPACE_CACHE_FILE) -> list:
    try:
        with urllib.request.urlopen(NAMESPACE_REGISTRY_URL, timeout=10) as response:
            data = json.loads(response.read())
        namespaces = [
            entry["prefix"] for entry in data
            if not entry["prefix"].startswith(("/caches/", "/origins/"))
        ]
        cache_file.write_text(json.dumps(namespaces))
        return namespaces
    except Exception:
        return json.loads(cache_file.read_text())


def build_namespace_script(namespaces: list) -> str:
    # Sort longest-first for correct prefix matching
    sorted_ns = sorted(namespaces, key=len, reverse=True)
    # TransferUrl is either osdf://[/]{ns}/... or pelican://osg-htc.org/{ns}/...
    # For pelican, the namespace path starts after the third "/" (skip the host).
    # For osdf, the client is lenient about slash count (2-4 seen in practice),
    # so strip all slashes after "://" and prepend a single "/".
    # Result is always /{ns}/... matching the registry prefix format.
    lines = [
        "String url = doc['TransferUrl'].value;",
        "int protoIdx = url.indexOf('://');",
        "if (protoIdx < 0) { emit('UNKNOWN'); return; }",
        "String rest = url.substring(protoIdx + 3);",
        "String path;",
        "if (doc['TransferProtocol'].value == 'pelican') {",
        "  int slashIdx = rest.indexOf('/');",
        "  if (slashIdx < 0) { emit('UNKNOWN'); return; }",
        "  path = rest.substring(slashIdx);",
        "} else {",
        "  while (rest.startsWith('/')) { rest = rest.substring(1); }",
        "  path = '/' + rest;",
        "}",
    ]
    for ns in sorted_ns:
        escaped = ns.replace("'", "\\'")
        lines.append(f"if (path.startsWith('{escaped}/')) {{ emit('{escaped}'); return; }}")
    lines.append("emit('UNKNOWN');")
    return "\n".join(lines)


def get_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        endpoint_types: dict,
        transfer_type: str = None,
        namespaces: list = None,
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}) \
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter(Q("term", TransferSuccess=False) | Q("term", FinalAttempt=False)) \
                .filter(Q("wildcard", TransferUrl="*osdf://*") | Q("wildcard", TransferUrl="*pelican://osg-htc.org*"))

    if transfer_type:
        query = query.filter("term", TransferType=transfer_type)

    # filter out jobs that did not run in the OSPool
    has_resource_name = Q("exists", field="machineattrglidein_resourcename0") & ~Q("terms", machineattrglidein_resourcename0=["Undefined", "2"])
    query = query.filter(has_resource_name)

    director404_agg = A("filter", filter=~Q("exists", field="Endpoint"))
    origin_agg = A("terms", field="Endpoint", size=len(endpoint_types["origin"]) or 1, include=list(endpoint_types["origin"]))
    site_agg = A("terms", field="machineattrglidein_site0", size=256)
    owner_agg = A("terms", field="Owner", size=128, missing="Undefined")
    project_agg = A("terms", field="ProjectName", size=512, missing="Undefined")
    ap_agg = A("terms", field="ScheddName", size=128)
    error_type_agg = A("terms", field="ErrorType", size=128, missing="Undefined")

    director404_agg.bucket("debug_error_type", A("terms", field="DebugErrorType", size=128))
    site_agg.bucket("endpoint", A("terms", field="Endpoint", size=128))

    if transfer_type != "upload":
        cache_agg = A("terms", field="Endpoint", size=len(endpoint_types["cache"]) or 1, include=list(endpoint_types["cache"]))
        cache_agg.bucket("hit_or_miss", A("terms", field="CacheHit", size=3))
        # Count implicit cache misses: CacheHit undefined but error is transfer-related
        implicit_miss_filter = ~Q("exists", field="CacheHit") & ~Q("terms", ErrorType=["Authorization", "Specification", "Resolution", "Contact"])
        cache_agg.bucket("implicit_miss", A("filter", filter=implicit_miss_filter))
        query.aggs.bucket("cache", cache_agg)

    query.aggs.bucket("origin", origin_agg)
    query.aggs.bucket("site", site_agg)
    query.aggs.bucket("owner", owner_agg)
    query.aggs.bucket("project", project_agg)
    query.aggs.bucket("ap", ap_agg)
    query.aggs.bucket("director404", director404_agg)
    query.aggs.bucket("error_type", error_type_agg)

    if namespaces:
        ns_script = build_namespace_script(namespaces)
        query.update_from_dict({"runtime_mappings": {
            "namespace": {"type": "keyword", "script": {"source": ns_script}},
        }})
        query.aggs.bucket("namespace", A("terms", field="namespace", size=len(namespaces) + 1))
        endpoint_ns_agg = A("terms", field="Endpoint", size=128)
        endpoint_ns_agg.bucket("namespace", A("terms", field="namespace", size=len(namespaces) + 1))
        query.aggs.bucket("endpoint_namespace", endpoint_ns_agg)

    return query


def enrich_endpoint_name(endpoint: str, osdf_endpoint_data: dict) -> str:
    """Return 'NAME (Institution)' for an endpoint, or the raw endpoint if unknown."""
    info = osdf_endpoint_data.get(endpoint, {})
    name = info.get("name")
    institution = info.get("institution")
    if name and institution:
        return f"{name} ({institution})"
    elif name:
        return name
    return endpoint


def build_top_components_summary(component_totals: dict, totals: dict, total_failures: int, osdf_endpoint_data: dict, display_names: dict) -> list:
    """Build a summary of top contributors per component, sorted by signal clarity."""
    summary = []
    max_components = 3
    is_output = "cache" not in display_names

    for comp, comp_list in component_totals.items():
        section_total = totals.get(comp, 0)
        half_section = section_total / 2
        num_with_failures = len(comp_list)
        avg_pct_of_section = 1 / num_with_failures if num_with_failures > 0 else 0

        top_entries = []
        cumulative = 0
        for key, *_, count in comp_list[:max_components]:
            if key is None:
                display_key = "(unknown)"
            elif comp in ENDPOINT_SECTIONS:
                display_key = osdf_endpoint_data.get(key, {}).get("name") or key
            elif comp == "site_endpoint":
                parts = key.split("..", 1)
                ep_name = osdf_endpoint_data.get(parts[1], {}).get("name") or parts[1] if len(parts) == 2 else key
                if is_output:
                    display_key = f"({parts[0]}, {ep_name})" if len(parts) == 2 else key
                else:
                    display_key = f"({ep_name}, {parts[0]})" if len(parts) == 2 else key
            elif comp == "endpoint_namespace":
                parts = key.split("..", 1)
                ep_name = osdf_endpoint_data.get(parts[0], {}).get("name") or parts[0] if len(parts) == 2 else key
                display_key = f"({ep_name}, {parts[1]})" if len(parts) == 2 else key
            else:
                display_key = key
            top_entries.append((display_key, count, count / section_total if section_total > 0 else 0))
            cumulative += count
            if cumulative >= half_section:
                break

        top1_pct_of_section = comp_list[0][1] / section_total if comp_list and section_total > 0 else 0
        top1_pct_of_total = comp_list[0][1] / total_failures if comp_list and total_failures > 0 else 0

        zero_message = ""
        if section_total == 0 and comp == "origin" and not is_output:
            zero_message = "No errors in direct reads"

        summary.append({
            "key": comp,
            "component": display_names.get(comp, comp),
            "total_failures": section_total,
            "avg_pct_of_section": avg_pct_of_section,
            "top_entries": top_entries,
            "zero_message": zero_message,
            "top1_pct_of_section": top1_pct_of_section,
            "top1_pct_of_total": top1_pct_of_total,
        })

    summary.sort(key=lambda s: s["top1_pct_of_total"], reverse=True)
    return summary


def format_section_data(comp: str, comp_list: list, section_total: int, total_failures: int, osdf_endpoint_data: dict, display_names: dict) -> list:
    """Format a component's breakdown into a list of row dicts for rendering."""
    is_output = "cache" not in display_names
    rows = []
    for key, count in comp_list:
        if key is None:
            name = "(unknown)"
        elif comp in ENDPOINT_SECTIONS:
            name = enrich_endpoint_name(key, osdf_endpoint_data)
        elif comp == "site_endpoint":
            parts = key.split("..", 1)
            if len(parts) == 2:
                enriched = enrich_endpoint_name(parts[1], osdf_endpoint_data)
                if is_output:
                    name = f"({parts[0]}, {enriched})"
                else:
                    name = f"({enriched}, {parts[0]})"
            else:
                name = key
        elif comp == "endpoint_namespace":
            parts = key.split("..", 1)
            if len(parts) == 2:
                enriched = enrich_endpoint_name(parts[0], osdf_endpoint_data)
                name = f"({enriched}, {parts[1]})"
            else:
                name = key
        else:
            name = key
        rows.append({
            "name": name,
            "failures": count,
            "pct_of_section": count / section_total if section_total > 0 else 0,
            "pct_of_total": count / total_failures if total_failures > 0 else 0,
        })
    return rows


def load_pelican_error_codes(yaml_path: Path) -> list:
    """Load Pelican error codes from multi-document YAML file."""
    try:
        with open(yaml_path) as f:
            docs = list(yaml.safe_load_all(f))
        return [doc for doc in docs if doc is not None]
    except (FileNotFoundError, OSError):
        return []


def process_direction(
        es: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        osdf_endpoint_data: dict,
        transfer_type: str,
        display_names: dict,
        components: list,
        namespaces: list = None,
    ) -> dict:
    """Run query and build summary/sections for one transfer direction."""

    endpoint_types = get_endpoint_types(
        client=es,
        index=index,
        start=start,
        end=end,
        osdf_endpoint_data=osdf_endpoint_data,
        transfer_type=transfer_type,
    )

    query = get_query(
        client=es,
        index=index,
        start=start,
        end=end,
        endpoint_types=endpoint_types,
        transfer_type=transfer_type,
        namespaces=namespaces if "namespace" in components else None,
    )

    try:
        result = query.execute()
    except Exception as err:
        try:
            print_es_error(err.info)
        except Exception:
            pass
        raise err

    total_failures = result.hits.total.value

    totals = {
        "origin": sum(bucket.doc_count for bucket in result.aggregations.origin.buckets),
        "director404": result.aggregations.director404.doc_count,
        "site": sum(bucket.doc_count for bucket in result.aggregations.site.buckets),
        "owner": sum(bucket.doc_count for bucket in result.aggregations.owner.buckets),
        "project": sum(bucket.doc_count for bucket in result.aggregations.project.buckets),
        "ap": sum(bucket.doc_count for bucket in result.aggregations.ap.buckets),
        "error_type": sum(bucket.doc_count for bucket in result.aggregations.error_type.buckets),
    }
    totals["site_endpoint"] = totals["site"]

    component_totals = {
        "origin":      [(b.key, b.doc_count) for b in result.aggregations.origin.buckets],
        "director404": [(b.key, b.doc_count) for b in result.aggregations.director404.debug_error_type.buckets],
        "site":        [(b.key, b.doc_count) for b in result.aggregations.site.buckets],
        "owner":       [(b.key, b.doc_count) for b in result.aggregations.owner.buckets],
        "project":     [(b.key, b.doc_count) for b in result.aggregations.project.buckets],
        "ap":          [(b.key, b.doc_count) for b in result.aggregations.ap.buckets],
        "error_type":  [(b.key, b.doc_count) for b in result.aggregations.error_type.buckets],
        "site_endpoint": [
            (f"{site_bucket.key}..{endpoint_bucket.key}", endpoint_bucket.doc_count)
            for site_bucket in result.aggregations.site.buckets
            for endpoint_bucket in site_bucket.endpoint.buckets
        ],
    }

    if "cache" in components:
        totals["cache"] = sum(bucket.doc_count for bucket in result.aggregations.cache.buckets)
        component_totals["cache"] = [(b.key, b.doc_count) for b in result.aggregations.cache.buckets]
        totals["cache-hits"] = sum(
            hit_bucket.doc_count
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "true"
        )
        # Cache misses = explicit (CacheHit=false) + implicit (CacheHit undefined, transfer-related error)
        explicit_misses = {}
        for cache_bucket in result.aggregations.cache.buckets:
            for hit_bucket in cache_bucket.hit_or_miss.buckets:
                if hit_bucket.key_as_string == "false":
                    explicit_misses[cache_bucket.key] = hit_bucket.doc_count
        implicit_misses = {
            cache_bucket.key: cache_bucket.implicit_miss.doc_count
            for cache_bucket in result.aggregations.cache.buckets
            if cache_bucket.implicit_miss.doc_count > 0
        }
        all_miss_endpoints = set(explicit_misses) | set(implicit_misses)
        component_totals["cache-misses"] = [
            (ep, explicit_misses.get(ep, 0) + implicit_misses.get(ep, 0))
            for ep in all_miss_endpoints
        ]
        totals["cache-misses"] = sum(count for _, count in component_totals["cache-misses"])

        component_totals["cache-hits"] = [
            (cache_bucket.key, hit_bucket.doc_count)
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "true"
        ]

    if namespaces and "namespace" in components:
        totals["namespace"] = sum(bucket.doc_count for bucket in result.aggregations.namespace.buckets)
        component_totals["namespace"] = [(b.key, b.doc_count) for b in result.aggregations.namespace.buckets]

    if namespaces and "endpoint_namespace" in components:
        component_totals["endpoint_namespace"] = [
            (f"{ep_bucket.key}..{ns_bucket.key}", ns_bucket.doc_count)
            for ep_bucket in result.aggregations.endpoint_namespace.buckets
            for ns_bucket in ep_bucket.namespace.buckets
        ]
        totals["endpoint_namespace"] = sum(bucket.doc_count for bucket in result.aggregations.endpoint_namespace.buckets)

    # Institution-level rollup
    institution_totals = {}
    endpoint_lists = [component_totals.get("origin", [])]
    if "cache" in components:
        endpoint_lists.append(component_totals.get("cache", []))
    for endpoint, count in [item for sublist in endpoint_lists for item in sublist]:
        inst = osdf_endpoint_data.get(endpoint, {}).get("institution", "Unknown")
        institution_totals[inst] = institution_totals.get(inst, 0) + count
    component_totals["institution"] = list(institution_totals.items())
    totals["institution"] = sum(totals.get(k, 0) for k in ["cache", "origin"] if k in totals)

    # Filter to only requested components
    component_totals = {k: v for k, v in component_totals.items() if k in components}
    totals = {k: v for k, v in totals.items() if k in components}

    for key in component_totals:
        component_totals[key].sort(key=itemgetter(-1), reverse=True)

    summary = build_top_components_summary(component_totals, totals, total_failures, osdf_endpoint_data, display_names)
    sections = {}
    for comp in component_totals:
        sections[comp] = format_section_data(comp, component_totals[comp], totals[comp], total_failures, osdf_endpoint_data, display_names)

    return {
        "sections": sections,
        "summary": summary,
        "total_failures": total_failures,
    }


def render_text(direction_results: list, start: datetime, end: datetime):
    """Print formatted text tables to stdout."""
    try:
        from tabulate import tabulate
    except ImportError:
        print("WARNING: tabulate not installed, falling back to TSV", file=sys.stderr)
        tabulate = None

    days = (end - start).days
    print(f"\n{days}-day OSDF Component Error Report {start.strftime(r'%Y-%m-%d')} to {end.strftime(r'%Y-%m-%d')}")

    # Summary tables first
    for dr in direction_results:
        print(f"\n{'=' * 60}")
        print(f"  {dr['label']}")
        print(f"{'=' * 60}")
        print(f"Total failed transfers: {dr['total_failures']:,}\n")

        summary_rows = []
        for s in dr["summary"]:
            top_strs = [f"{pct:6.1%} {name}" for name, count, pct in s["top_entries"]]
            contributors = s["zero_message"] or (", ".join(top_strs) if top_strs else "-")
            summary_rows.append([
                s["component"],
                f"{s['total_failures']:,}",
                f"{s['top1_pct_of_total']:.1%}",
                contributors,
                f"{s['avg_pct_of_section']:.1%}",
            ])
        headers = ["Component", "Failures", "Top-1 Share of Total", "Top Contributors (% of section)", "Avg % per Item"]
        if tabulate:
            print(tabulate(summary_rows, headers=headers, tablefmt="simple"))
        else:
            print("\t".join(headers))
            for row in summary_rows:
                print("\t".join(row))
        print()

    # Per-component detail tables
    for dr in direction_results:
        print(f"\n{'=' * 60}")
        print(f"  {dr['label']} - Details")
        print(f"{'=' * 60}")

        for s in dr["summary"]:
            rows = dr["sections"][s["key"]]
            if not rows:
                continue
            print(f"--- {s['component']} ---")
            print(f"Total failures: {s['total_failures']:,}")
            table_rows = [
                [r["name"], f"{r['failures']:,}", f"{r['pct_of_section']:.1%}", f"{r['pct_of_total']:.1%}"]
                for r in rows
            ]
            col_headers = ["Name", "Failures", "% of Section", "% of Total"]
            if tabulate:
                print(tabulate(table_rows, headers=col_headers, tablefmt="simple"))
            else:
                print("\t".join(col_headers))
                for row in table_rows:
                    print("\t".join(row))
            print()


def render_error_codes_html(error_codes: list, styles: dict) -> list:
    """Render Pelican error codes as an HTML appendix table."""
    html = []
    html.append(f'<h2 style="{styles["h2"]}">Pelican Error Codes Reference</h2>')
    html.append(f'<table style="{styles["table"]}">')
    html.append("\t<tr>")
    for col in ["Code", "Type", "Description", "Retryable"]:
        html.append(f'\t\t<th style="{styles["td"]}">{col}</th>')
    html.append("\t</tr>")
    for ec in error_codes:
        retryable_str = "Yes" if ec.get("retryable") else "No"
        html.append("\t<tr>")
        html.append(f'\t\t<td style="{styles["td"]}; {styles["td.numeric"]}">{ec["code"]}</td>')
        html.append(f'\t\t<td style="{styles["td"]}; {styles["td.text"]}">{ec["type"]}</td>')
        html.append(f'\t\t<td style="{styles["td"]}; {styles["td.text"]}">{ec.get("description", "")}</td>')
        html.append(f'\t\t<td style="{styles["td"]}; {styles["td.text"]}">{retryable_str}</td>')
        html.append("\t</tr>")
    html.append("</table>")
    return html


def render_html(direction_results: list, start: datetime, end: datetime, error_codes: list = None) -> str:
    """Build an HTML report string."""
    styles = {
        "h1": "text-align: center",
        "h2": "margin-top: 1.5em",
        "h3": "margin-top: 1.2em",
        "table": "border-collapse: collapse",
        "td": "border: 1px solid black; padding: 4px 8px",
        "td.text": "text-align: left",
        "td.numeric": "text-align: right",
        "tr.warn": "background-color: #ffc",
        "tr.err": "background-color: #fcc",
    }

    # Build error code lookup for title tooltips
    error_code_descriptions = {}
    if error_codes:
        for ec in error_codes:
            error_code_descriptions[ec["type"]] = ec.get("description", "")
            # Also map the top-level type (e.g. "Transfer" from "Transfer.CacheOverloaded")
            top_level = ec["type"].split(".")[0]
            if top_level not in error_code_descriptions:
                error_code_descriptions[top_level] = ""

    days = (end - start).days
    html = ["<html>", "<head></head>", "<body>"]
    html.append(f'<h1 style="{styles["h1"]}">{days}-day OSDF Component Error Report {start.strftime(r"%Y-%m-%d")} to {end.strftime(r"%Y-%m-%d")}</h1>')

    summary_columns = [
        ("Component", "component", "s"),
        ("Failures", "total_failures", ",d"),
        ("Top-1 Share of Total", "top1_pct_of_total", ".1%"),
        ("Top Contributors (% of section)", "top_contributors", "s"),
        ("Avg % per Item", "avg_pct_of_section", ".1%"),
    ]

    section_columns = [
        ("Name", "name", "s"),
        ("Failures", "failures", ",d"),
        ("% of Section", "pct_of_section", ".1%"),
        ("% of Total", "pct_of_total", ".1%"),
    ]

    max_rows = 10

    # Summary tables first
    for dr in direction_results:
        html.append(f'<h2 style="{styles["h2"]}">{dr["label"]}</h2>')
        html.append(f"<p>Total failed transfers: {dr['total_failures']:,}</p>")

        html.append(f'<table style="{styles["table"]}">')
        html.append("\t<tr>")
        for name, _, _ in summary_columns:
            html.append(f'\t\t<th style="{styles["td"]}">{name}</th>')
        html.append("\t</tr>")
        for s in dr["summary"]:
            top_strs = [f"{pct:6.1%} {name},".replace(" ", "&nbsp;") for name, count, pct in s["top_entries"]]
            row_data = {
                "component": s["component"],
                "total_failures": s["total_failures"],
                "avg_pct_of_section": s["avg_pct_of_section"],
                "top1_pct_of_total": s["top1_pct_of_total"],
                "top_contributors": s["zero_message"] or ("<br>".join(top_strs).rstrip(",") if top_strs else "-"),
            }
            html.append("\t<tr>")
            for _, key, fmt in summary_columns:
                style = "td.text" if fmt == "s" else "td.numeric"
                try:
                    html.append(f'\t\t<td style="{styles["td"]}; {styles[style]}">{row_data[key]:{fmt}}</td>')
                except (ValueError, TypeError):
                    html.append(f'\t\t<td style="{styles["td"]}">{row_data[key]}</td>')
            html.append("\t</tr>")
        html.append("</table>")

    # Per-component detail tables
    for dr in direction_results:
        html.append(f'<h2 style="{styles["h2"]}">{dr["label"]} - Details</h2>')

        for s in dr["summary"]:
            all_rows = dr["sections"][s["key"]]
            if not all_rows:
                continue
            top_rows = all_rows[:max_rows]
            remaining = all_rows[max_rows:]
            is_error_type = s["key"] == "error_type"

            html.append(f'<h3 style="{styles["h3"]}">{s["component"]}</h3>')
            html.append(f"<p>Total failures: {s['total_failures']:,}</p>")
            html.append(f'<table style="{styles["table"]}">')
            html.append("\t<tr>")
            for name, _, _ in section_columns:
                html.append(f'\t\t<th style="{styles["td"]}">{name}</th>')
            html.append("\t</tr>")
            for row in top_rows:
                row_style = ""
                if row["pct_of_section"] > 0.15:
                    row_style = styles["tr.err"]
                elif row["pct_of_section"] > 0.05:
                    row_style = styles["tr.warn"]
                html.append(f'\t<tr style="{row_style}">')
                for _, key, fmt in section_columns:
                    style = "td.text" if fmt == "s" else "td.numeric"
                    value = row[key]
                    # Add tooltip for error type names
                    title_attr = ""
                    if is_error_type and key == "name" and value in error_code_descriptions and error_code_descriptions[value]:
                        desc = error_code_descriptions[value].replace('"', '&quot;')
                        title_attr = f' title="{desc}"'
                    try:
                        html.append(f'\t\t<td style="{styles["td"]}; {styles[style]}"{title_attr}>{value:{fmt}}</td>')
                    except (ValueError, TypeError):
                        html.append(f'\t\t<td style="{styles["td"]}"{title_attr}>{value}</td>')
                html.append("\t</tr>")
            if remaining:
                rest_failures = sum(r["failures"] for r in remaining)
                rest_pct_section = sum(r["pct_of_section"] for r in remaining)
                rest_pct_total = sum(r["pct_of_total"] for r in remaining)
                rest_row = {
                    "name": f"{len(remaining)} more...",
                    "failures": rest_failures,
                    "pct_of_section": rest_pct_section,
                    "pct_of_total": rest_pct_total,
                }
                html.append("\t<tr>")
                for _, key, fmt in section_columns:
                    style = "td.text" if fmt == "s" else "td.numeric"
                    try:
                        html.append(f'\t\t<td style="{styles["td"]}; {styles[style]}">{rest_row[key]:{fmt}}</td>')
                    except (ValueError, TypeError):
                        html.append(f'\t\t<td style="{styles["td"]}">{rest_row[key]}</td>')
                html.append("\t</tr>")
            html.append("</table>")

    # Pelican error codes appendix
    if error_codes:
        html.extend(render_error_codes_html(error_codes, styles))

    html.append("</body>")
    html.append("</html>")
    return "\n".join(html)


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

    osdf_endpoint_data = get_osdf_endpoint_data(cache_file=args.cache_dir / "osdf_endpoint_data.pickle")
    namespaces = get_namespace_list(cache_file=args.cache_dir / "osdf_namespaces.json")
    error_codes = load_pelican_error_codes(args.pelican_error_codes)

    if not es_args.get("es_timeout"):
        es_args["es_timeout"] = 60 + int(10 * (days**0.75))
    es = connect(**es_args)
    es.info()

    direction_results = []

    input_result = process_direction(
        es, index, args.start, args.end, osdf_endpoint_data,
        transfer_type="download",
        display_names=INPUT_DISPLAY_NAMES,
        components=INPUT_COMPONENTS,
        namespaces=namespaces,
    )
    input_result["label"] = "Input Transfers"
    direction_results.append(input_result)

    output_result = process_direction(
        es, index, args.start, args.end, osdf_endpoint_data,
        transfer_type="upload",
        display_names=OUTPUT_DISPLAY_NAMES,
        components=OUTPUT_COMPONENTS,
        namespaces=namespaces,
    )
    output_result["label"] = "Output Transfers"
    direction_results.append(output_result)

    if args.to:
        html = render_html(direction_results, args.start, args.end, error_codes)
        send_email(
            subject=f"{days}-day OSDF Component Error Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
            from_addr=args.from_addr,
            to_addrs=args.to,
            html=html,
            cc_addrs=args.cc,
            bcc_addrs=args.bcc,
            reply_to_addr=args.reply_to,
            smtp_server=args.smtp_server,
            smtp_username=args.smtp_username,
            smtp_password_file=args.smtp_password_file,
        )
    else:
        render_text(direction_results, args.start, args.end)


if __name__ == "__main__":
    main()

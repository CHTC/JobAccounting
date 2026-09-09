import sys
import time
import json
import argparse

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email, get_osdf_endpoint_data
from metric_functions import valid_date, connect, print_es_error, EMAIL_ARGS, ELASTICSEARCH_ARGS

import elasticsearch
from elasticsearch_dsl import Search, A, Q


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


def get_endpoint_types(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        osdf_endpoint_data: dict = None,
    ) -> dict:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}) \
                .filter("exists", field="Endpoint") \
                .query(~Q("term", Endpoint="")) \
                .query(Q("wildcard", TransferUrl="*osdf://*") | Q("wildcard", TransferUrl="*pelican://osg-htc.org*"))
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


def get_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        endpoint_types: dict,
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}) \
                .filter("terms", TransferProtocol=["osdf", "pelican"]) \
                .filter("term", TransferSuccess=False) \
                .query(Q("wildcard", TransferUrl="*osdf://*") | Q("wildcard", TransferUrl="*pelican://osg-htc.org*"))

    # filter out jobs that did not run in the OSPool
    has_resource_name = Q("exists", field="machineattrglidein_resourcename0") & ~Q("terms", machineattrglidein_resourcename0=["Undefined", "2"])
    query = query.query(has_resource_name)

    # pull CacheHit out of _source since it's not currently indexed
    query.update_from_dict({
        "runtime_mappings": {
            "CacheHit": {
                "type": "boolean",
                "script": {
                    "source": "if (params._source.containsKey('cachehit')) { emit(Boolean.parseBoolean(params._source['cachehit'])); }"
                }
            }
        }
    })

    director404_agg = A("filter", filter=~Q("exists", field="Endpoint"))
    cache_agg = A("terms", field="Endpoint", size=len(endpoint_types["cache"]) or 1, include=list(endpoint_types["cache"]))
    origin_agg = A("terms", field="Endpoint", size=len(endpoint_types["origin"]) or 1, include=list(endpoint_types["origin"]))
    site_agg = A("terms", field="machineattrglidein_site0", size=256)
    owner_agg = A("terms", field="Owner", size=128, missing="Undefined")
    ap_agg = A("terms", field="ScheddName", size=128)
    error_type_agg = A("terms", field="ErrorType", size=128, missing="Undefined")

    # Add error_type to director to split out director and 404 errors
    director404_agg.bucket("debug_error_type", A("terms", field="DebugErrorType", size=128))

    # Add endpoint to site
    site_agg.bucket("endpoint", A("terms", field="Endpoint", size=128))

    # Add cache hit/miss to cache
    cache_agg.bucket("hit_or_miss", A("terms", field="CacheHit", size=3))

    query.aggs.bucket("cache", cache_agg)
    query.aggs.bucket("origin", origin_agg)
    query.aggs.bucket("site", site_agg)
    query.aggs.bucket("owner", owner_agg)
    query.aggs.bucket("ap", ap_agg)
    query.aggs.bucket("director404", director404_agg)
    query.aggs.bucket("error_type", error_type_agg)

    return query


ENDPOINT_SECTIONS = {"cache", "origin", "cache-hits", "cache-misses"}

COMPONENT_DISPLAY_NAMES = {
    "cache": "Cache",
    "origin": "Origin",
    "institution": "Endpoint Institution",
    "director404": "Director/404",
    "site": "OSG Site",
    "owner": "Owner",
    "ap": "AP",
    "error_type": "Error Type",
    "site_endpoint": "(Site, Endpoint)",
    "cache-hits": "Cache (Hits)",
    "cache-misses": "Cache (Misses)",
}


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


def build_top_components_summary(component_totals: dict, totals: dict, total_failures: int, osdf_endpoint_data: dict) -> list:
    """Build a summary of top contributors per component, sorted by signal clarity.

    Returns a list of dicts sorted by the top-1 contributor's share of section
    failures (descending), so sections with the clearest signal come first.
    """
    summary = []
    max_components = 3

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
                display_key = f"({parts[0]}, {ep_name})" if len(parts) == 2 else key
            else:
                display_key = key
            top_entries.append((display_key, count, count / section_total if section_total > 0 else 0))
            cumulative += count
            if cumulative >= half_section:
                break

        top1_pct_of_section = comp_list[0][1] / section_total if comp_list and section_total > 0 else 0
        top1_pct_of_total = comp_list[0][1] / total_failures if comp_list and total_failures > 0 else 0

        summary.append({
            "key": comp,
            "component": COMPONENT_DISPLAY_NAMES.get(comp, comp),
            "total_failures": section_total,
            "avg_pct_of_section": avg_pct_of_section,
            "top_entries": top_entries,
            "top1_pct_of_section": top1_pct_of_section,
            "top1_pct_of_total": top1_pct_of_total,
        })

    summary.sort(key=lambda s: s["top1_pct_of_total"], reverse=True)
    return summary


def format_section_data(comp: str, comp_list: list, section_total: int, total_failures: int, osdf_endpoint_data: dict) -> list:
    """Format a component's breakdown into a list of row dicts for rendering."""
    rows = []
    for key, count in comp_list:
        if key is None:
            name = "(unknown)"
        elif comp in ENDPOINT_SECTIONS:
            name = enrich_endpoint_name(key, osdf_endpoint_data)
        elif comp == "site_endpoint":
            parts = key.split("..", 1)
            name = f"({parts[0]}, {enrich_endpoint_name(parts[1], osdf_endpoint_data)})" if len(parts) == 2 else key
        else:
            name = key
        rows.append({
            "name": name,
            "failures": count,
            "pct_of_section": count / section_total if section_total > 0 else 0,
            "pct_of_total": count / total_failures if total_failures > 0 else 0,
        })
    return rows


def render_text(sections: dict, summary: list, start: datetime, end: datetime, total_failures: int):
    """Print formatted text tables to stdout."""
    try:
        from tabulate import tabulate
    except ImportError:
        print("WARNING: tabulate not installed, falling back to TSV", file=sys.stderr)
        tabulate = None

    days = (end - start).days
    print(f"\n{days}-day OSDF Component Error Report {start.strftime(r'%Y-%m-%d')} to {end.strftime(r'%Y-%m-%d')}")
    print(f"Total failed transfers: {total_failures:,}\n")

    # Summary table
    summary_rows = []
    for s in summary:
        top_strs = [f"{name} ({pct:.1%})" for name, count, pct in s["top_entries"]]
        summary_rows.append([
            s["component"],
            f"{s['total_failures']:,}",
            f"{s['top1_pct_of_total']:.1%}",
            ", ".join(top_strs) if top_strs else "-",
            f"{s['avg_pct_of_section']:.1%}",
        ])
    headers = ["Component", "Failures", "Top-1 Share of Total Failures", "Top Contributors (% of Component Failures)", "Avg % per Item"]
    if tabulate:
        print(tabulate(summary_rows, headers=headers, tablefmt="simple"))
    else:
        print("\t".join(headers))
        for row in summary_rows:
            print("\t".join(row))
    print()

    # Per-section tables
    for s in summary:
        rows = sections[s["key"]]
        if not rows:
            continue
        print(f"--- {s['component']} ---")
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


def render_html(sections: dict, summary: list, start: datetime, end: datetime, total_failures: int) -> str:
    """Build an HTML report string."""
    styles = {
        "h1": "text-align: center",
        "h2": "margin-top: 1.5em",
        "table": "border-collapse: collapse",
        "td": "border: 1px solid black; padding: 4px 8px",
        "td.text": "text-align: left",
        "td.numeric": "text-align: right",
        "tr.warn": "background-color: #ffc",
        "tr.err": "background-color: #fcc",
    }

    days = (end - start).days
    html = ["<html>", "<head></head>", "<body>"]
    html.append(f'<h1 style="{styles["h1"]}">{days}-day OSDF Component Error Report {start.strftime(r"%Y-%m-%d")} to {end.strftime(r"%Y-%m-%d")}</h1>')
    html.append(f"<p>Total failed transfers: {total_failures:,}</p>")

    # Summary table
    html.append(f'<h2 style="{styles["h2"]}">Summary by Component (sorted by signal clarity)</h2>')
    summary_columns = [
        ("Component", "component", "s"),
        ("Failures", "total_failures", ",d"),
        ("Top-1 Share of Total Failures", "top1_pct_of_total", ".1%"),
        ("Top Contributors (% of Component Failures)", "top_contributors", "s"),
        ("Avg % per Item", "avg_pct_of_section", ".1%"),
    ]
    html.append(f'<table style="{styles["table"]}">')
    html.append("\t<tr>")
    for name, _, _ in summary_columns:
        html.append(f'\t\t<th style="{styles["td"]}">{name}</th>')
    html.append("\t</tr>")
    for s in summary:
        top_strs = [f"{name} ({pct:.1%})" for name, count, pct in s["top_entries"]]
        row_data = {
            "component": s["component"],
            "total_failures": s["total_failures"],
            "avg_pct_of_section": s["avg_pct_of_section"],
            "top1_pct_of_total": s["top1_pct_of_total"],
            "top_contributors": ", ".join(top_strs) if top_strs else "-",
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

    # Per-section tables
    section_columns = [
        ("Name", "name", "s"),
        ("Failures", "failures", ",d"),
        ("% of Section", "pct_of_section", ".1%"),
        ("% of Total", "pct_of_total", ".1%"),
    ]
    max_rows = 10
    for s in summary:
        all_rows = sections[s["key"]]
        if not all_rows:
            continue
        top_rows = all_rows[:max_rows]
        remaining = all_rows[max_rows:]

        html.append(f'<h2 style="{styles["h2"]}">{s["component"]}</h2>')
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
                try:
                    html.append(f'\t\t<td style="{styles["td"]}; {styles[style]}">{row[key]:{fmt}}</td>')
                except (ValueError, TypeError):
                    html.append(f'\t\t<td style="{styles["td"]}">{row[key]}</td>')
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

    if not es_args.get("es_timeout"):
        es_args["es_timeout"] = 60 + int(10 * (days**0.75))
    es = connect(**es_args)
    es.info()

    endpoint_types = get_endpoint_types(
        client=es,
        index=index,
        start=args.start,
        end=args.end,
        osdf_endpoint_data=osdf_endpoint_data,
    )

    query = get_query(
        client=es,
        index=index,
        start=args.start,
        end=args.end,
        endpoint_types=endpoint_types,
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
        "cache": sum(bucket.doc_count for bucket in result.aggregations.cache.buckets),
        "origin": sum(bucket.doc_count for bucket in result.aggregations.origin.buckets),
        "director404": result.aggregations.director404.doc_count,
        "site": sum(bucket.doc_count for bucket in result.aggregations.site.buckets),
        "owner": sum(bucket.doc_count for bucket in result.aggregations.owner.buckets),
        "ap": sum(bucket.doc_count for bucket in result.aggregations.ap.buckets),
        "error_type": sum(bucket.doc_count for bucket in result.aggregations.error_type.buckets),
        "cache-hits": sum(
            hit_bucket.doc_count
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "true"
        ),
        "cache-misses": sum(
            hit_bucket.doc_count
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "false"
        ),
    }
    totals["site_endpoint"] = totals["site"]

    component_totals = {
        "cache":       [(b.key, b.doc_count) for b in result.aggregations.cache.buckets],
        "origin":      [(b.key, b.doc_count) for b in result.aggregations.origin.buckets],
        "director404": [(b.key, b.doc_count) for b in result.aggregations.director404.debug_error_type.buckets],
        "site":        [(b.key, b.doc_count) for b in result.aggregations.site.buckets],
        "owner":       [(b.key, b.doc_count) for b in result.aggregations.owner.buckets],
        "ap":          [(b.key, b.doc_count) for b in result.aggregations.ap.buckets],
        "error_type":  [(b.key, b.doc_count) for b in result.aggregations.error_type.buckets],
        "site_endpoint": [
            (f"{site_bucket.key}..{endpoint_bucket.key}", endpoint_bucket.doc_count)
            for site_bucket in result.aggregations.site.buckets
            for endpoint_bucket in site_bucket.endpoint.buckets
        ],
        "cache-hits": [
            (cache_bucket.key, hit_bucket.doc_count)
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "true"
        ],
        "cache-misses": [
            (cache_bucket.key, hit_bucket.doc_count)
            for cache_bucket in result.aggregations.cache.buckets
            for hit_bucket in cache_bucket.hit_or_miss.buckets
            if hit_bucket.key_as_string == "false"
        ],
    }

    # Add institution-level rollup from cache + origin
    institution_totals = {}
    for endpoint, count in component_totals["cache"] + component_totals["origin"]:
        inst = osdf_endpoint_data.get(endpoint, {}).get("institution", "Unknown")
        institution_totals[inst] = institution_totals.get(inst, 0) + count
    component_totals["institution"] = list(institution_totals.items())
    totals["institution"] = totals["cache"] + totals["origin"]

    for key in component_totals:
        component_totals[key].sort(key=itemgetter(-1), reverse=True)

    # Build summary and per-section data
    summary = build_top_components_summary(component_totals, totals, total_failures, osdf_endpoint_data)
    sections = {}
    for comp in component_totals:
        sections[comp] = format_section_data(comp, component_totals[comp], totals[comp], total_failures, osdf_endpoint_data)

    if args.to:
        html = render_html(sections, summary, args.start, args.end, total_failures)
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
        render_text(sections, summary, args.start, args.end, total_failures)


if __name__ == "__main__":
    main()

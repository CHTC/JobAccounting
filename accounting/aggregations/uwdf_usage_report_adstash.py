import sys
import json
import argparse
import urllib.request

from operator import itemgetter
from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email
from metric_functions import EMAIL_ARGS, ELASTICSEARCH_ARGS, connect, valid_date, print_es_error

import elasticsearch
from elasticsearch_dsl import Search, A, Q


NAMESPACE_REGISTRY_URL = "https://uwdf-registry.chtc.wisc.edu/api/v1.0/registry_ui/namespaces"
NAMESPACE_CACHE_FILE = Path(__file__).parent / "uwdf_namespaces.json"


def fmt_val(value, fmt):
    formatted = f"{value:{fmt}}"
    if value and formatted == f"{0:{fmt}}":
        zero_str = formatted.lstrip("0").lstrip(".")
        return f"&gt;0{zero_str}"
    return formatted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)
    es_args.add_argument("--es-transfer-index", default="adstash-chtc-ap-transfer-history-*")
    parser.set_defaults(es_index="adstash-chtc-ap-job-epoch-history-*")

    parser.add_argument("--start", type=valid_date)
    parser.add_argument("--end", type=valid_date)
    parser.add_argument("--project-table", action="store_true")
    parser.add_argument("--namespace-table", action="store_true")

    return parser.parse_args()


def get_project_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime
    ) -> Search:

    time_range = {"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}

    pelican_in_range = Q("bool", minimum_should_match=1, should=[
        Q("bool", filter=[
            Q("wildcard", TransferInput="*pelican://chtc.wisc.edu/*"),
            Q("range", JobCurrentFinishTransferInputDate=time_range),
            Q("bool", minimum_should_match=1, should=[
                Q("range", **{"TransferInputStats.PelicanFilesCountLastRun": {"gt": 0}}),
                Q("range", **{"TransferInputStats.PELICANFilesCountLastRun": {"gt": 0}}),
            ]),
        ]),
        Q("bool", filter=[
            Q("bool", minimum_should_match=1, should=[
                Q("wildcard", TransferOutputRemaps="*pelican://chtc.wisc.edu/*"),
                Q("wildcard", OutputDestination="*pelican://chtc.wisc.edu/*"),
            ]),
            Q("range", JobCurrentFinishTransferOutputDate=time_range),
            Q("bool", minimum_should_match=1, should=[
                Q("range", **{"TransferOutputStats.PelicanFilesCountLastRun": {"gt": 0}}),
                Q("range", **{"TransferOutputStats.PELICANFilesCountLastRun": {"gt": 0}}),
            ]),
        ]),
    ])

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter(pelican_in_range)

    project_agg = A("terms", field="ProjectName", size=512)
    for direction, date_field in [("Input", "JobCurrentFinishTransferInputDate"), ("Output", "JobCurrentFinishTransferOutputDate")]:
        dir_filter = A("filter", filter=Q("range", **{date_field: time_range}))
        for casing in ("Pelican", "PELICAN", ""):
            prefix = casing if casing else "bare"
            dir_filter.metric(
                f"files_{prefix}",
                A("sum", field=f"Transfer{direction}Stats.{casing}FilesCountLastRun"),
            )
            dir_filter.metric(
                f"bytes_{prefix}",
                A("sum", field=f"Transfer{direction}Stats.{casing}SizeBytesLastRun"),
            )
        project_agg.bucket(direction.lower(), dir_filter)

    query.aggs.bucket("project", project_agg)

    return query


def get_endpoint_query(
        client: elasticsearch.Elasticsearch,
        index: str,
        start: datetime,
        end: datetime,
        namespaces: list,
    ) -> Search:

    query = Search(using=client, index=index) \
                .extra(size=0) \
                .extra(track_scores=False) \
                .extra(track_total_hits=True) \
                .filter("wildcard", TransferUrl="*pelican://chtc.wisc.edu/*") \
                .filter("term", TransferSuccess=True) \
                .filter("range", TransferEndTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"})

    all_runtime_mappings = {}
    for ns in namespaces:
        ns_key = ns.lstrip("/").replace("/", "_")
        ns_filter = A("filter", filter=Q("wildcard", TransferUrl=f"*pelican://chtc.wisc.edu{ns}/*"))

        pi_script_src = f"""
String url = doc['TransferUrl'].value;
int nsIdx = url.indexOf('{ns}/');
if (nsIdx < 0) {{ emit('UNKNOWN'); return; }}
int piStart = nsIdx + {len(ns) + 1};
int piEnd = url.indexOf('/', piStart);
if (piEnd < 0) {{ emit('UNKNOWN'); return; }}
emit(url.substring(piStart, piEnd));
"""
        all_runtime_mappings[f"pi_{ns_key}"] = {
            "type": "keyword",
            "script": {"source": pi_script_src},
        }

        pi_agg = A("terms", field=f"pi_{ns_key}", size=512)
        pi_agg.metric("bytes", A("sum", field="TransferFileBytes"))
        cache_agg = A("filter", filter=Q("term", CacheHit=True))
        cache_agg.metric("bytes", A("sum", field="TransferFileBytes"))
        pi_agg.metric("cache_hit", cache_agg)
        ns_filter.bucket("pi", pi_agg)

        ns_filter.metric("bytes", A("sum", field="TransferFileBytes"))
        ns_cache_agg = A("filter", filter=Q("term", CacheHit=True))
        ns_cache_agg.metric("bytes", A("sum", field="TransferFileBytes"))
        ns_filter.metric("cache_hit", ns_cache_agg)

        query.aggs.bucket(f"ns_{ns_key}", ns_filter)
    query.update_from_dict({"runtime_mappings": all_runtime_mappings})

    return query


def _report_type(args) -> str:
    if args.project_table and args.namespace_table:
        return "Usage"
    if args.namespace_table:
        return "Namespace Usage"
    return "Project Usage"


def get_namespace_list() -> list:
    try:
        with urllib.request.urlopen(NAMESPACE_REGISTRY_URL, timeout=10) as response:
            data = json.loads(response.read())
        namespaces = [
            entry["prefix"] for entry in data
            if not entry["prefix"].startswith(("/caches/", "/origins/"))
        ]
        NAMESPACE_CACHE_FILE.write_text(json.dumps(namespaces))
        return namespaces
    except Exception:
        return json.loads(NAMESPACE_CACHE_FILE.read_text())


def main():
    args = parse_args()
    if not args.project_table and not args.namespace_table:
        print("No tables selected! Use --project-table and/or --namespace-table.")
        sys.exit(1)

    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args.pop("es_password_file").open().read().rstrip()
    index = es_args.pop("es_index", args.es_index)
    transfer_index = es_args.pop("es_transfer_index", args.es_transfer_index)

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)
    days = (args.end - args.start).days

    namespaces = get_namespace_list()

    es_args["timeout"] = es_args.pop("es_timeout", None)
    if not es_args["timeout"]:
        es_args["timeout"] = 60 + int(10 * (days**0.75))
    es = connect(**es_args)
    es.info()

    if args.project_table:
        print(f"{datetime.now()} - Running job history query")
        project_query = get_project_query(client=es, index=index, start=args.start, end=args.end)
        try:
            project_result = project_query.execute()
        except Exception as err:
            try:
                print_es_error(err.info)
            except Exception:
                pass
            raise err
        print(f"{datetime.now()} - Done.")
        project_buckets = project_result.to_dict()["aggregations"]["project"]["buckets"]

    if args.namespace_table:
        print(f"{datetime.now()} - Running transfer history query")
        endpoint_query = get_endpoint_query(client=es, index=transfer_index, start=args.start, end=args.end, namespaces=namespaces)
        try:
            endpoint_result = endpoint_query.execute()
        except Exception as err:
            try:
                print_es_error(err.info)
            except Exception:
                pass
            raise err
        print(f"{datetime.now()} - Done.")


    if args.project_table:
        data = []
        total = {
            "project": "TOTAL",
            "jobs": 0,
            "files_transferred": 0,
            "gb_transferred": 0,
        }
        for bucket in project_buckets:
            files = 0
            bytes_total = 0
            for direction in ("input", "output"):
                for prefix in ("Pelican", "PELICAN", "bare"):
                    files += bucket.get(direction, {}).get(f"files_{prefix}", {}).get("value", 0)
                    bytes_total += bucket.get(direction, {}).get(f"bytes_{prefix}", {}).get("value", 0)
            row = {
                "project": bucket["key"],
                "jobs": bucket["doc_count"],
                "files_transferred": int(files),
                "gb_transferred": bytes_total / 1e9,
            }
            for k, v in row.items():
                if k == "project":
                    continue
                total[k] += v
            data.append(row)
        total["project"] = '<span style="font-weight: bold">TOTAL</span>'
        data.sort(key=itemgetter("files_transferred"), reverse=True)
        data.insert(0, total)

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

    if args.project_table:
        html.append(f"<h1>UWDF usage from CHTC jobs completed {args.start} to {args.end}</h1>")

        cols = ["project", "files_transferred", "gb_transferred", "jobs"]
        hdrs = ["Project", "Files"            , "GBs"           , "Jobs"]
        fmts = ["s"      , ",d"               , ",.0f"          , ",d"]
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
                    html.append(f'\t\t<td class="{stys[col]}">{fmt_val(row[col], fmts[col])}</td>')
                except ValueError:
                    html.append(f"\t\t<td>{row[col]}</td>")
            html.append("\t</tr>")
        html.append("</table>")

    if args.namespace_table:
        aggs = endpoint_result.to_dict()["aggregations"]
        grand_total = {
            "namespace": '<span style="font-weight: bold">TOTAL</span>',
            "pi": "",
            "files": 0,
            "total_gb": 0.0,
            "cached_gb": 0.0,
            "_total_bytes": 0.0,
            "_cache_bytes": 0.0,
        }
        ns_groups = []
        for ns in namespaces:
            ns_key = ns.lstrip("/").replace("/", "_")
            ns_bucket = aggs.get(f"ns_{ns_key}", {})
            if ns_bucket.get("doc_count", 0) == 0:
                continue
            ns_total_bytes = ns_bucket.get("bytes", {}).get("value", 0.0)
            ns_cache_bytes = ns_bucket.get("cache_hit", {}).get("bytes", {}).get("value", 0.0)
            ns_total_row = {
                "namespace": ns,
                "pi": "TOTAL",
                "files": ns_bucket["doc_count"],
                "total_gb": ns_total_bytes / 1e9,
                "cached_gb": ns_cache_bytes / 1e9,
                "cache_pct": ns_cache_bytes / ns_total_bytes if ns_total_bytes > 0 else 0.0,
            }
            grand_total["files"] += ns_total_row["files"]
            grand_total["total_gb"] += ns_total_row["total_gb"]
            grand_total["cached_gb"] += ns_total_row["cached_gb"]
            grand_total["_total_bytes"] += ns_total_bytes
            grand_total["_cache_bytes"] += ns_cache_bytes

            pi_rows = []
            if ns == "/researchdrive":
                for pi_bucket in ns_bucket.get("pi", {}).get("buckets", []):
                    pi = pi_bucket["key"]
                    files = pi_bucket["doc_count"]
                    total_bytes = pi_bucket.get("bytes", {}).get("value", 0.0)
                    cache_bytes = pi_bucket.get("cache_hit", {}).get("bytes", {}).get("value", 0.0)
                    if files > 0:
                        pi_rows.append({
                            "namespace": ns,
                            "pi": pi,
                            "files": files,
                            "total_gb": total_bytes / 1e9,
                            "cached_gb": cache_bytes / 1e9,
                            "cache_pct": cache_bytes / total_bytes if total_bytes > 0 else 0.0,
                        })
                pi_rows.sort(key=lambda r: (-r["files"], r["pi"]))

            ns_groups.append((ns, ns_total_row, pi_rows))

        _total_bytes = grand_total.pop("_total_bytes")
        _cache_bytes = grand_total.pop("_cache_bytes")
        grand_total["cache_pct"] = _cache_bytes / _total_bytes if _total_bytes > 0 else 0.0

        # /researchdrive first, then alphabetically
        ns_groups.sort(key=lambda g: (0 if g[0] == "/researchdrive" else 1, g[0]))

        ns_data = [grand_total]
        for ns, ns_total_row, pi_rows in ns_groups:
            ns_data.append(ns_total_row)
            ns_data.extend(pi_rows)

        html.append(f"<h1>UWDF namespace usage from transfers completed {args.start} to {args.end}</h1>")

        ns_cols = ["namespace", "pi",        "files", "total_gb",  "cached_gb",  "cache_pct"  ]
        ns_hdrs = ["Namespace", "PI NetID*",  "Files", "Total GBs", "Cached GBs", "Cache Hit %"]
        ns_fmts = ["s",         "s",         ",d",    ",.0f",      ",.0f",       ".1%"        ]
        ns_stys = ["text" if fmt == "s" else "num" for fmt in ns_fmts]

        ns_hdrs = dict(zip(ns_cols, ns_hdrs))
        ns_fmts = dict(zip(ns_cols, ns_fmts))
        ns_stys = dict(zip(ns_cols, ns_stys))

        html.append('<table>')
        html.append("\t<tr>")
        for col in ns_cols:
            html.append(f"\t\t<th>{ns_hdrs[col]}</th>")
        html.append("\t</tr>")
        last_ns = None
        for row in ns_data:
            if row.get("pi") in ("TOTAL", ""):
                html.append('\t<tr style="font-weight: bold">')
            else:
                html.append('\t<tr>')
            for col in ns_cols:
                if col == "namespace" and row[col] == last_ns:
                    html.append(f'\t\t<td class="{ns_stys[col]}"></td>')
                    continue
                try:
                    html.append(f'\t\t<td class="{ns_stys[col]}">{fmt_val(row[col], ns_fmts[col])}</td>')
                except ValueError:
                    html.append(f"\t\t<td>{row[col]}</td>")
            last_ns = row["namespace"]
            html.append("\t</tr>")
        html.append("</table>")
        html.append("*For ResearchDrive only")

    html.append("</body>")

    html.append("</html>")

    send_email(
        subject=f"{days}-day CHTC UWDF {_report_type(args)} Report {args.start.strftime(r'%Y-%m-%d')} to {args.end.strftime(r'%Y-%m-%d')}",
        from_addr=args.from_addr,
        to_addrs=args.to,
        html="\n".join(html),
        cc_addrs=args.cc,
        bcc_addrs=args.bcc,
        reply_to_addr=args.reply_to,
        smtp_server=args.smtp_server,
        smtp_username=args.smtp_username,
        smtp_password_file=args.smtp_password_file,
    )


if __name__ == "__main__":
    main()

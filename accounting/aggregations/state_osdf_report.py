from __future__ import annotations

import sys
import csv
import json
import argparse

from datetime import datetime, timedelta
from pathlib import Path

from functions import get_osdf_endpoint_location_map
from metric_functions import connect, execute_async, load_state_geometry, point_in_state, print_es_error, valid_date

import elasticsearch
from elasticsearch_dsl import Search, A, Q


ELASTICSEARCH_ARGS = {
    "--es-host": {"default": "localhost:9200"},
    "--es-url-prefix": {},
    "--es-index": {},
    "--es-user": {},
    "--es-password-file": {"type": Path},
    "--es-use-https": {"action": "store_true"},
    "--es-ca-certs": {},
    "--es-timeout": {"type": int, "default": 120},
    "--es-config-file": {
        "type": Path,
        "help": "JSON file containing an object that sets above ES options",
    }
}

# Emits a lowercased resource name from the machineattrglidein_resourcename0 field,
# matching the lowercased keys produced by the OSPool report's ResourceName runtime field.
RESOURCE_NAME_LOWER_SCRIPT_SRC = """
if (doc.containsKey('machineattrglidein_resourcename0.indexed') && doc['machineattrglidein_resourcename0.indexed'].size() > 0) {
    emit(doc['machineattrglidein_resourcename0.indexed'].value.toLowerCase());
}
"""

# Emits the OSPool username for a transfer record.
# Prefers owner.indexed if present (a newer field that directly stores the submitting user).
# Otherwise parses the username from TransferUrl: finds the first /ospool occurrence in the
# then extracts the path component immediately following the first /data/ segment after that
# point, e.g. osdf:///ospool/ap40/data/<user>/...
TRANSFER_URL_OWNER_SCRIPT_SRC = """
if (doc.containsKey('owner.indexed') && doc['owner.indexed'].size() > 0) {
    emit(doc['owner.indexed'].value);
    return;
}
if (doc.containsKey('TransferUrl.indexed') && doc['TransferUrl.indexed'].size() > 0) {
    String url = doc['TransferUrl.indexed'].value;
    int ospoolIdx = url.indexOf('/ospool');
    if (ospoolIdx < 0) return;
    String rest = url.substring(ospoolIdx);
    int dataIdx = rest.indexOf('/data/');
    if (dataIdx < 0) return;
    String after = rest.substring(dataIdx + 6);
    int slashIdx = after.indexOf('/');
    emit(slashIdx >= 0 ? after.substring(0, slashIdx) : after);
}
"""

ENDPOINT_LOCATION_MAP = get_osdf_endpoint_location_map()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--state", required=True, metavar="XX",
                        help="Two-letter state abbreviation (e.g. TN, WI).")
    parser.add_argument("--shapefile", type=Path, required=True,
                        help="Path to TIGER/Line US state shapefile (.shp).")
    parser.add_argument("--ospool-cache", type=Path, required=True,
                        help="Path to a state_ospool_report cache file to source project owners from.")
    parser.add_argument("--start", type=valid_date, metavar="YYYY-MM-DD",
                        help="Start of the reporting period (inclusive). Defaults to yesterday.")
    parser.add_argument("--end", type=valid_date, metavar="YYYY-MM-DD",
                        help="End of the reporting period (exclusive). Defaults to one day after --start.")
    parser.add_argument("--no-cache", action="store_true",
                        help="Skip loading cached results and re-run the query.")
    parser.add_argument("--debug", action="store_true",
                        help="Print owners, endpoints, and full query to stderr before executing.")

    return parser.parse_args()


def get_state_endpoints(state_geometry) -> set[str]:
    """Return OSDF endpoints (host:port) whose coordinates fall within the given state."""
    return {
        endpoint
        for endpoint, loc in ENDPOINT_LOCATION_MAP.items()
        if point_in_state(loc["longitude"], loc["latitude"], state_geometry)
    }


def get_state_owners(ospool_cache: dict) -> set[str]:
    """Extract unique job owners from state-based projects in an OSPool report cache."""
    return {
        bucket["key"]
        for bucket in ospool_cache["aggregations"]["state_projects"]["owners"]["buckets"]
    }


def get_state_resources(ospool_cache: dict) -> set[str]:
    """Extract lowercased resource names from state-based resources in an OSPool report cache."""
    return {
        bucket["key"].lower()
        for bucket in ospool_cache["aggregations"]["state_resources"]["resource_name"]["buckets"]
    }


def get_base_query(
        index: str,
        start: datetime,
        end: datetime,
        state_endpoints: set[str],
        state_owners: set[str],
        state_resources: set[str],
    ) -> Search:

    runtime_mappings = {
        "TransferUrlOwner": {
            "type": "keyword",
            "script": {
                "source": TRANSFER_URL_OWNER_SCRIPT_SRC,
            }
        },
        "ResourceNameLower": {
            "type": "keyword",
            "script": {
                "source": RESOURCE_NAME_LOWER_SCRIPT_SRC,
            }
        },
    }

    query = Search(index=index) \
                .extra(size=0, track_scores=False, track_total_hits=True) \
                .extra(runtime_mappings=runtime_mappings) \
                .filter("terms", TransferProtocol=["osdf", "stash", "pelican"]) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("term", TransferSuccess=True)

    transfer_total_bytes_agg = A("sum", field="TransferTotalBytes")

    endpoint_agg = A("terms", field="Endpoint", size=512)
    endpoint_agg.metric("transfer_total_bytes", transfer_total_bytes_agg)

    owner_endpoint_agg = A("terms", field="Endpoint", size=512)
    owner_endpoint_agg.metric("transfer_total_bytes", transfer_total_bytes_agg)

    resource_endpoint_agg = A("terms", field="Endpoint", size=512)
    resource_endpoint_agg.metric("transfer_total_bytes", transfer_total_bytes_agg)

    # state_endpoints: transfers served by OSDF caches physically located in the state,
    # identified by point-in-polygon check of the director's lat/lon against the shapefile.
    state_endpoint_filter = A("filter", Q("terms", Endpoint=list(state_endpoints)))
    state_endpoint_filter.bucket("endpoint", endpoint_agg)

    # state_owners: transfers made by OSPool job owners whose projects are state-affiliated,
    # sourced from the owners sub-bucket of the OSPool report cache.
    state_owner_filter = A("filter", Q("terms", TransferUrlOwner=list(state_owners)))
    state_owner_filter.bucket("endpoint", owner_endpoint_agg)

    # state_resources: transfers originating from state-based OSPool execution resources,
    # matched by lowercased resource name from the OSPool report cache.
    state_resource_filter = A("filter", Q("terms", ResourceNameLower=list(state_resources)))
    state_resource_filter.bucket("endpoint", resource_endpoint_agg)

    query.aggs.bucket("state_endpoints", state_endpoint_filter)
    query.aggs.bucket("state_owners", state_owner_filter)
    query.aggs.bucket("state_resources", state_resource_filter)

    return query


def print_endpoint_table(buckets):
    writer = csv.writer(sys.stdout)
    writer.writerow(["Endpoint", "Objects Transferred", "GB Transferred"])
    for bucket in buckets:
        endpoint = bucket["key"]
        writer.writerow([
            endpoint,
            bucket["doc_count"],
            round(bucket["transfer_total_bytes"]["value"] / 1e9, 2),
        ])


def main():
    args = parse_args()
    state = args.state.upper()

    ospool_cache = json.loads(args.ospool_cache.read_text())

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

    cache_file = Path(f"{state.lower()}_osdf_{args.start.date()}_{args.end.date()}.json")
    if not args.no_cache and cache_file.exists():
        print(f"Loading cached results from {cache_file}", file=sys.stderr)
        result = json.loads(cache_file.read_text())
    else:
        es = connect(**es_args)
        es.info()

        state_geometry = load_state_geometry(args.shapefile, state)
        state_endpoints = get_state_endpoints(state_geometry)
        state_owners = get_state_owners(ospool_cache)
        state_resources = get_state_resources(ospool_cache)

        if args.debug:
            print(f"State endpoints ({len(state_endpoints)}): {sorted(state_endpoints)}", file=sys.stderr)
            print(f"State owners ({len(state_owners)}): {sorted(state_owners)}", file=sys.stderr)
            print(f"State resources ({len(state_resources)}): {sorted(state_resources)}", file=sys.stderr)

        q = get_base_query(index, args.start, args.end, state_endpoints, state_owners, state_resources)

        if args.debug:
            print(f"Query: {json.dumps(q.to_dict(), indent=2)}", file=sys.stderr)

        try:
            result = execute_async(es, q)
        except KeyboardInterrupt:
            sys.exit(1)
        except elasticsearch.exceptions.ElasticsearchException as e:
            print_es_error(e, file=sys.stderr)
            sys.exit(1)

        cache_file.write_text(json.dumps(result))
        print(f"Results cached to {cache_file}", file=sys.stderr)

    print("Transfers to state-based OSDF endpoints:")
    print_endpoint_table(result["aggregations"]["state_endpoints"]["endpoint"]["buckets"])
    print()
    print("Transfers by state-based project owners:")
    print_endpoint_table(result["aggregations"]["state_owners"]["endpoint"]["buckets"])
    print()
    print("Transfers from state-based resources:")
    print_endpoint_table(result["aggregations"]["state_resources"]["endpoint"]["buckets"])


if __name__ == "__main__":
    main()

from __future__ import annotations

import sys
import csv
import json
import argparse

from datetime import datetime, timedelta
from pathlib import Path

from functions import get_institution_database, get_topology_resource_data, get_topology_project_data
from metric_functions import connect, execute_async, get_ospool_aps, OSPOOL_COLLECTORS, print_es_error, valid_date

import elasticsearch
from elasticsearch_dsl import Search, A, Q


PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE = Path().home() / "JobAccounting" / "ospool-host-map.pkl"


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

# Emits a normalized resource name for each job record.
# Prefers MachineAttrOSG_INSTITUTION_ID0 (emitted raw, e.g. "osg-htc.org_iid_xxxx"),
# Falls back to MachineAttrGLIDEIN_ResourceName0 then MATCH_EXP_JOBGLIDEIN_ResourceName,
# both lowercased to match topology resource data structure keys.
RESOURCE_NAME_SCRIPT_SRC = """
String res;
if (doc.containsKey("MachineAttrOSG_INSTITUTION_ID0") && doc["MachineAttrOSG_INSTITUTION_ID0.keyword"].size() > 0) {
    emit(doc["MachineAttrOSG_INSTITUTION_ID0.keyword"].value);
} else if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
    res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
    emit(res.toLowerCase());
} else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
    res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
    emit(res.toLowerCase());
}
"""

# Emits a lowercased project name, checking four common case variants of the field name.
PROJECT_NAME_SCRIPT_SRC = """
String proj;
def fieldNames = ["ProjectName", "projectName", "Projectname", "projectname"];
for (String fieldName : fieldNames) {
    if (doc.containsKey(fieldName) && doc[fieldName + ".keyword"].size() > 0) {
        proj = doc[fieldName + ".keyword"].value.toLowerCase();
        emit(proj);
        break;
    }
}
"""

# Emits CPU-hours for a job: min(RequestCpus, CpusProvisioned) * RemoteWallClockTime / 3600.
# CpusProvisioned caps the count when fewer cores were actually allocated than requested,
# which can happen if a user qedits their job after it ran.
CPU_HOURS_SCRIPT_SRC = """
long cpus = 1;
long wallclocktime = 0;
if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
    cpus = doc["RequestCpus"].value;
}
if (doc.containsKey("CpusProvisioned") && doc["CpusProvisioned"].size() > 0 && doc["CpusProvisioned"].value < cpus) {
    cpus = doc["CpusProvisioned"].value;
}
if (doc.containsKey("RemoteWallClockTime") && doc["RemoteWallClockTime"].size() > 0) {
    wallclocktime = doc["RemoteWallClockTime"].value;
}
emit((double)cpus * ((double)wallclocktime / (double)3600));
"""


INSTITUTION_DATABASE = get_institution_database()
RESOURCE_TOPOLOGY_DATA = get_topology_resource_data()
PROJECT_TOPOLOGY_DATA = get_topology_project_data()

IS_OSPOOL_JOB_FILTER = (
    (
        (
            Q("terms", ScheddName__keyword=list(get_ospool_aps(include_jupyter_aps=False, pickled_ap_collector_hosts_cache=PICKLED_AP_COLLECTOR_HOSTS_CACHE_FILE))) &
            ~Q("exists", field="LastRemotePool")
        ) | (
            Q("terms", LastRemotePool__keyword=list(OSPOOL_COLLECTORS))
        )
    ) &
    ~(
        Q("exists", field="TargetAnnexName") |
        Q("terms", MachineAttrGLIDEIN_ResourceName0__keyword=["Local Job", "2"]) |
        Q("terms", MATCH_EXP_JOBGLIDEIN_ResourceName__keyword=["Local Job", "2"])
    )
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--state", required=True, metavar="XX",
                        help="Two-letter state abbreviation (e.g. TN, WI).")
    parser.add_argument("--start", type=valid_date, metavar="YYYY-MM-DD",
                        help="Start of the reporting period (inclusive). Defaults to yesterday.")
    parser.add_argument("--end", type=valid_date, metavar="YYYY-MM-DD",
                        help="End of the reporting period (exclusive). Defaults to one day after --start.")
    parser.add_argument("--no-cache", action="store_true",
                        help="Skip loading cached results and re-run the query.")
    parser.add_argument("--debug", action="store_true",
                        help="Print additional debug information to stderr.")

    return parser.parse_args()


def get_base_query(
        index: str,
        start: datetime,
        end: datetime,
        state_projects: set[str],
        state_resources: set[str],
    ) -> Search:

    runtime_mappings = {
        "ResourceName": {
            "type": "keyword",
            "script": {
                "source": RESOURCE_NAME_SCRIPT_SRC,
            }
        },
        "ProjectNameLower": {
            "type": "keyword",
            "script": {
                "source": PROJECT_NAME_SCRIPT_SRC,
            }
        },
        "CpuHours": {
            "type": "double",
            "script": {
                "source": CPU_HOURS_SCRIPT_SRC,
            }
        },
    }

    query = Search(index=index) \
                .extra(size=0, track_scores=False, track_total_hits=True) \
                .extra(runtime_mappings=runtime_mappings) \
                .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp())}) \
                .filter("range", RemoteWallClockTime={"gt": 0}) \
                .query(~Q("terms", JobUniverse=[7, 12])) \
                .query(IS_OSPOOL_JOB_FILTER) \
                .filter(Q("terms", ProjectNameLower=list(state_projects)) | Q("terms", ResourceName=list(state_resources)))

    cpu_hours_agg = A("sum", field="CpuHours")

    # LastRun fields are used for resource buckets:
    # LastRun reflects the transfer for the final run only - not cumulative across
    # retries, which can happen at different resources than the recorded resource.
    osdf_transfer_aggs_lastrun = {
        "input_osdf_files":   A("sum", field="TransferInputStats.OSDFFilesCountLastRun"),
        "input_stash_files":  A("sum", field="TransferInputStats.STASHFilesCountLastRun"),
        "input_pelican_files": A("sum", field="TransferInputStats.PELICANFilesCountLastRun"),
        "output_osdf_files":  A("sum", field="TransferOutputStats.OSDFFilesCountLastRun"),
        "output_stash_files": A("sum", field="TransferOutputStats.STASHFilesCountLastRun"),
        "output_pelican_files": A("sum", field="TransferOutputStats.PELICANFilesCountLastRun"),
        "input_osdf_bytes":   A("sum", field="TransferInputStats.OSDFSizeBytesLastRun"),
        "input_stash_bytes":  A("sum", field="TransferInputStats.STASHSizeBytesLastRun"),
        "input_pelican_bytes": A("sum", field="TransferInputStats.PELICANSizeBytesLastRun"),
        "output_osdf_bytes":  A("sum", field="TransferOutputStats.OSDFSizeBytesLastRun"),
        "output_stash_bytes": A("sum", field="TransferOutputStats.STASHSizeBytesLastRun"),
        "output_pelican_bytes": A("sum", field="TransferOutputStats.PELICANSizeBytesLastRun"),
    }
    # Total fields are used for project buckets:
    # A project's full transfer volume across all retries of all its jobs.
    osdf_transfer_aggs_total = {
        "input_osdf_files":   A("sum", field="TransferInputStats.OSDFFilesCountTotal"),
        "input_stash_files":  A("sum", field="TransferInputStats.STASHFilesCountTotal"),
        "input_pelican_files": A("sum", field="TransferInputStats.PELICANFilesCountTotal"),
        "output_osdf_files":  A("sum", field="TransferOutputStats.OSDFFilesCountTotal"),
        "output_stash_files": A("sum", field="TransferOutputStats.STASHFilesCountTotal"),
        "output_pelican_files": A("sum", field="TransferOutputStats.PELICANFilesCountTotal"),
        "input_osdf_bytes":   A("sum", field="TransferInputStats.OSDFSizeBytesTotal"),
        "input_stash_bytes":  A("sum", field="TransferInputStats.STASHSizeBytesTotal"),
        "input_pelican_bytes": A("sum", field="TransferInputStats.PELICANSizeBytesTotal"),
        "output_osdf_bytes":  A("sum", field="TransferOutputStats.OSDFSizeBytesTotal"),
        "output_stash_bytes": A("sum", field="TransferOutputStats.STASHSizeBytesTotal"),
        "output_pelican_bytes": A("sum", field="TransferOutputStats.PELICANSizeBytesTotal"),
    }

    # Use a bucket_script to combine OSDF fields
    files_buckets_path = {
        "input_osdf":     "input_osdf_files",
        "input_stash":    "input_stash_files",
        "input_pelican":  "input_pelican_files",
        "output_osdf":    "output_osdf_files",
        "output_stash":   "output_stash_files",
        "output_pelican": "output_pelican_files",
    }
    bytes_buckets_path = {
        "input_osdf":     "input_osdf_bytes",
        "input_stash":    "input_stash_bytes",
        "input_pelican":  "input_pelican_bytes",
        "output_osdf":    "output_osdf_bytes",
        "output_stash":   "output_stash_bytes",
        "output_pelican": "output_pelican_bytes",
    }
    sum_params_expr = "params.input_osdf + params.input_stash + params.input_pelican + params.output_osdf + params.output_stash + params.output_pelican"

    files_total_agg = A("bucket_script", buckets_path=files_buckets_path, script=sum_params_expr)
    bytes_total_agg = A("bucket_script", buckets_path=bytes_buckets_path, script=sum_params_expr)

    resource_name_agg = A("terms", field="ResourceName", size=512)
    resource_name_agg.metric("cpu_hours", cpu_hours_agg)
    for name, agg in osdf_transfer_aggs_lastrun.items():
        resource_name_agg.metric(name, agg)
    resource_name_agg.pipeline("osdf_files_transferred", files_total_agg)
    resource_name_agg.pipeline("osdf_bytes_transferred", bytes_total_agg)
    resource_name_agg.bucket("project_names", A("terms", field="ProjectNameLower", size=512))

    project_name_agg = A("terms", field="ProjectNameLower", size=512)
    project_name_agg.metric("cpu_hours", cpu_hours_agg)
    for name, agg in osdf_transfer_aggs_total.items():
        project_name_agg.metric(name, agg)
    project_name_agg.pipeline("osdf_files_transferred", files_total_agg)
    project_name_agg.pipeline("osdf_bytes_transferred", bytes_total_agg)
    project_name_agg.bucket("resource_names", A("terms", field="ResourceName", size=512))

    # state_resources: jobs that ran on a state-based resource, broken down by resource name.
    state_resource_filter = A("filter", Q("terms", ResourceName=list(state_resources)))
    state_resource_filter.bucket("resource_name", resource_name_agg)

    # state_projects: jobs from state-based projects, broken down by project name.
    # Sub-bucket "owners" collects unique job owners across all state projects, which
    # is used by the OSDF report to identify transfers from state-affiliated users.
    state_project_filter = A("filter", Q("terms", ProjectNameLower=list(state_projects)))
    state_project_filter.bucket("project_name", project_name_agg)
    state_project_filter.bucket("owners", A("terms", field="Owner.keyword", size=512))

    query.aggs.bucket("state_resources", state_resource_filter)
    query.aggs.bucket("state_projects", state_project_filter)

    return query


def get_state_projects(state: str) -> set[str]:
    """Return lowercased project names whose institution is in the given state."""
    return {
        name
        for name, data in PROJECT_TOPOLOGY_DATA.items()
        if data.get("state") == state
    }


def get_state_resources(state: str) -> set[str]:
    """Return lowercased resource names whose institution is in the given state."""
    return {
        name
        for name, data in RESOURCE_TOPOLOGY_DATA.items()
        if data.get("state") == state
    }


def get_state_institutions(state: str) -> set[str]:
    """Return OSG institution IDs whose institution is in the given state."""
    return {
        name.removeprefix("https://").replace("/", "_")
        for name, data in INSTITUTION_DATABASE.items()
        if "/iid/" in name
        and data.get("state") == state
    }


def print_table(buckets, topology_data, name_column: str, sub_bucket_key: str, cross_topology_data: dict, cross_column: str, debug: bool = False):
    writer = csv.writer(sys.stdout)
    writer.writerow([name_column, "Institution", "Total Jobs", "CPU Hours", "OSDF Objects Transferred", "OSDF GB Transferred", cross_column, f"{cross_column} Institutions"])
    for bucket in buckets:
        key = bucket["key"]
        if "_iid_" in key:
            institution_id = key.split("_iid_")[-1]
            institution_name = INSTITUTION_DATABASE.get(institution_id, {}).get("name", key)
            display_name = institution_name
        else:
            topo = topology_data.get(key, {})
            display_name = topo.get("name", key)
            institution_name = topo.get("institution", "")

        sub_buckets = bucket.get(sub_bucket_key, {}).get("buckets", [])
        cross_count = len(sub_buckets)
        cross_institutions = set()
        for b in sub_buckets:
            b_key = b["key"]
            if "_iid_" in b_key:
                inst = INSTITUTION_DATABASE.get(b_key.split("_iid_")[-1], {}).get("name", "")
            else:
                inst = cross_topology_data.get(b_key, {}).get("institution", "")
            if inst:
                cross_institutions.add(inst)

        if debug:
            print(f"{display_name} {cross_column} institutions: {cross_institutions}", file=sys.stderr)
        writer.writerow([
            display_name,
            institution_name,
            bucket["doc_count"],
            round(bucket["cpu_hours"]["value"], 2),
            int(bucket["osdf_files_transferred"]["value"]),
            round(bucket["osdf_bytes_transferred"]["value"] / 1e9, 2),
            cross_count,
            len(cross_institutions),
        ])


def main():
    args = parse_args()
    state = args.state.upper()

    es_args = {}
    if args.es_config_file:
        es_args = json.load(args.es_config_file.open())
    else:
        es_args = {arg: v for arg, v in vars(args).items() if arg.startswith("es_")}
    if es_args.get("es_password_file"):
        es_args["es_pass"] = es_args.pop("es_password_file").open().read().rstrip()
    index = es_args.pop("es_index", "osg-schedd-*")

    if args.start is None:
        args.start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if args.end is None:
        args.end = args.start + timedelta(days=1)

    cache_file = Path(f"{state.lower()}_ospool_{args.start.date()}_{args.end.date()}.json")
    if not args.no_cache and cache_file.exists():
        print(f"Loading cached results from {cache_file}", file=sys.stderr)
        result = json.loads(cache_file.read_text())
    else:
        es = connect(**es_args)
        es.info()

        state_projects = get_state_projects(state)
        state_resources = get_state_resources(state) | get_state_institutions(state)

        q = get_base_query(index, args.start, args.end, state_projects, state_resources)

        try:
            result = execute_async(es, q)
        except KeyboardInterrupt:
            sys.exit(1)
        except elasticsearch.exceptions.ElasticsearchException as e:
            print_es_error(e, file=sys.stderr)
            sys.exit(1)

        cache_file.write_text(json.dumps(result))
        print(f"Results cached to {cache_file}", file=sys.stderr)

    print_table(result["aggregations"]["state_resources"]["resource_name"]["buckets"], RESOURCE_TOPOLOGY_DATA, "Resource", "project_names", PROJECT_TOPOLOGY_DATA, "Projects", debug=args.debug)
    print()
    print_table(result["aggregations"]["state_projects"]["project_name"]["buckets"], PROJECT_TOPOLOGY_DATA, "Project", "resource_names", RESOURCE_TOPOLOGY_DATA, "Resources", debug=args.debug)


if __name__ == "__main__":
    main()

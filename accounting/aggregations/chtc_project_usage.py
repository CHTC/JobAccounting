import csv
import html
import json
import re
import sys
import argparse
import traceback

from datetime import datetime, timedelta
from pathlib import Path

from functions import send_email
from metric_functions import connect, valid_date, get_es_error, EMAIL_ARGS, ELASTICSEARCH_ARGS

from elasticsearch_dsl import Search, A, Q



NUMERIC_TD_STYLE = "border: 1px solid black; text-align: right"
TEXT_TD_STYLE = "border: 1px solid black; text-align: left"

DEFAULT_COLUMNS = {
     5: "User",
    10: "Num Uniq Job Ids",
    15: "Num GPU Job Ids",
    20: "All CPU Hours",
    25: "All GPU Hours",
    30: "% Good CPU Hours",
    33: "% Good GPU Hours",
    35: "Job Unit Hours",

    45: "% Ckpt Able",
    50: "% Rm'd Jobs",
    51: "Total Files Xferd",
    52: "OSDF Files Xferd",
    53: "% OSDF Files",
    54: "% OSDF Bytes",
    55: "Shadw Starts / Job Id",
    56: "Exec Atts / Shadw Start",
    57: "Holds / Job Id",

    60: "% Short Jobs",
    70: "% Jobs w/>1 Exec Att",
    80: "% Jobs w/1+ Holds",
    81: "% Jobs Over Rqst Disk",
    82: "% Jobs using S'ty",

    100: "Mean Actv Hrs",
    105: "Mean Setup Secs",

    110: "Min Hrs",
    120: "25% Hrs",
    130: "Med Hrs",
    140: "75% Hrs",
    145: "95% Hrs",
    150: "Max Hrs",
    160: "Mean Hrs",
    170: "Std Hrs",

    180: "Input Files / Exec Att",
    190: "Output Files / Job",

    300: "Good CPU Hours",
    305: "CPU Hours / Bad Exec Att",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    325: "Num Job Holds",
    330: "Num Rm'd Jobs",
    340: "Num DAG Node Jobs",
    350: "Num Jobs w/>1 Exec Att",
    355: "Num Jobs w/1+ Holds",
    357: "Num Jobs Over Rqst Disk",
    360: "Num Short Jobs",
    390: "Num Ckpt Able Jobs",
    400: "Num S'ty Jobs",

    500: "Max Rqst Mem MB",
    510: "Med Used Mem MB",
    520: "Max Used Mem MB",
    525: "Max Rqst Disk GB",
    527: "Max Used Disk GB",
    530: "Max Rqst Cpus",
    540: "Med Job Units",
    545: "Max Job Units",
}

RESOURCE_TYPES = ["Cpus", "Memory", "Disk", "Gpus"]
DEFAULT_RESOURCE_VALUE = {
    "Cpus": 1,
    "Memory": 1024,
    "Disk": 1024**2,
    "Gpus": 0,
}
FLOORED_RESOURCE_FIELD = "FlooredRequest{resource}"
FLOORED_RESOURCE_SCRIPT = """
long value = {default:d};
if (
    doc.containsKey("Request{resource}") &&
    doc["Request{resource}"].size() > 0
    ) {{
    value = doc["Request{resource}"].value;
}}
if (
    doc.containsKey("{resource}Provisioned") &&
    doc["{resource}Provisioned"].size() > 0 &&
    doc["{resource}Provisioned"].value < value
    ) {{
       value = doc["{resource}Provisioned"].value;
}}
emit(value);
"""

HAS_SCRIPT_SRC = """
    if (doc.containsKey("{key}") && doc["{key}.keyword"].size() > 0) {{
        emit(true);
    }}
"""

HAS_FIELDS = {
    "HasSingularityImage": "SingularityImage",
    "HasDAGNodeName": "DAGNodeName",
}

LAST_EXEC_TIME_SRC = r"""
    long last_exec_time = 0;
    if (doc.containsKey("lastremotewallclocktime") && doc["lastremotewallclocktime.keyword"].size() > 0) {
        long lrwct = (long)Double.parseDouble(doc["lastremotewallclocktime.keyword"].value);
        if (lrwct > 0 && lrwct < 3600*24*365) {
            last_exec_time = lrwct;
        }
    }
    if (doc.containsKey("CommittedTime") && doc["CommittedTime"].size() > 0) {
        long ct = (long)doc["CommittedTime"].value;
        if (ct > 0 && ct < 3600*24*365) {
            last_exec_time = ct;
        }
    }
    emit(last_exec_time);
"""

CKPTABLE_SCRIPT_SRC = r"""
    boolean is_ckptable = false;
    if (doc.containsKey("SuccessCheckpointExitCode") && doc["SuccessCheckpointExitCode"].size() > 0) {
        is_ckptable = true;
    } else if (doc.containsKey("SuccessCheckpointExitBySignal") &&
        doc["SuccessCheckpointExitBySignal"].size() > 0 &&
        doc["SuccessCheckpointExitBySignal"].value) {
        is_ckptable = true;
    } else if (doc.containsKey("WhenToTransferOutput") &&
        doc["WhenToTransferOutput.keyword"].size() > 0 &&
        doc.containsKey("Is_resumable") &&
        doc["Is_resumable"].size() > 0) {
        is_ckptable = doc["WhenToTransferOutput.keyword"].value == "ON_EXIT_OR_EVICT" && doc["Is_resumable"].value;
    }
    emit(is_ckptable);
"""

JOB_UNITS_SCRIPT_SRC = r"""
    long cpu_unit = 1;
    long memory_gb_unit = 4;
    long disk_gb_unit = 4;
    long cpu_units = 1;
    long memory_units = 1;
    long disk_units = 1;
    long job_units = 1;
    if (doc.containsKey("RequestCpus") && doc["RequestCpus"].size() > 0) {
        if (doc.containsKey("CpusProvisioned") && doc["CpusProvisioned"].size() > 0 && doc["CpusProvisioned"].value < doc["RequestCpus"].value) {
            cpu_units = (long)(doc["CpusProvisioned"].value / cpu_unit);
        } else {
            cpu_units = (long)(doc["RequestCpus"].value / cpu_unit);
        }
    }
    if (doc.containsKey("RequestMemory") && doc["RequestMemory"].size() > 0) {
        if (doc.containsKey("MemoryProvisioned") && doc["MemoryProvisioned"].size() > 0 && doc["MemoryProvisioned"].value < doc["RequestMemory"].value) {
            memory_units = (long)(((double)doc["MemoryProvisioned"].value / (double)1024) / memory_gb_unit);
        } else {
            memory_units = (long)(((double)doc["RequestMemory"].value / (double)1024) / memory_gb_unit);
        }
    }
    if (doc.containsKey("RequestDisk") && doc["RequestDisk"].size() > 0) {
        if (doc.containsKey("DiskProvisioned") && doc["DiskProvisioned"].size() > 0 && doc["DiskProvisioned"].value < doc["RequestDisk"].value) {
            disk_units = (long)(((double)doc["DiskProvisioned"].value / Math.pow((double)1024, 2)) / disk_gb_unit);
        } else {
            disk_units = (long)(((double)doc["RequestDisk"].value / Math.pow((double)1024, 2)) / disk_gb_unit);
        }
    }
    job_units = (long)Math.max((long)Math.max(cpu_units, memory_units), disk_units);
    emit(job_units);
"""

FILES_TRANSFERRED_SRC = r"""
long files = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /FilesCountTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      files += Long.parseLong(matcher.group(1));
    }
  }
}
emit(files);
"""

GB_TRANSFERRED_SRC = r"""
double gb = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /SizeBytesTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      gb += (double)Long.parseLong(matcher.group(1)) / 1e9;
    }
  }
}
emit(gb);
"""

OSDF_FILES_TRANSFERRED_SRC = r"""
long files = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /(STASH|OSDF)FilesCountTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      files += Long.parseLong(matcher.group(2));
    }
  }
}
emit(files);
"""

OSDF_GB_TRANSFERRED_SRC = r"""
double gb = 0;
for (key in ["transferinputstats", "transferoutputstats"]) {
  if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /(STASH|OSDF)SizeBytesTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      gb += (double)Long.parseLong(matcher.group(2)) / 1e9;
    }
  }
}
emit(gb);
"""

OVER_REQUEST_DISK_SRC = r"""
boolean over = false;
long request_disk = 1024*1024;
if (doc.containsKey("RequestDisk") && doc["RequestDisk"].size() > 0) {
    request_disk = doc["RequestDisk"].value;
}
if (doc.containsKey("DiskProvisioned") && doc["DiskProvisioned"].size() > 0 && doc["DiskProvisioned"].value < request_disk) {
    request_disk = doc["DiskProvisioned"].value;
}
if (doc.containsKey("DiskUsage") && doc["DiskUsage"].size() > 0) {
    over = doc["DiskUsage"].value > request_disk;
}
emit(over)
"""

ACTIVATION_HOURS_SRC = r"""
double activation_duration = 0;
long limit = 1600000000;
if (doc.containsKey("activationduration") && doc["activationduration.keyword"].size() > 0) {
    activation_duration = Double.parseDouble(doc["activationduration.keyword"].value);
}
if (activation_duration < limit) {
    emit(activation_duration / 3600.0);
}
"""

ACTIVATION_SETUP_SECONDS_SRC = r"""
long activation_setup_duration = 0;
long limit = 1600000000;
if (doc.containsKey("activationsetupduration") && doc["activationsetupduration.keyword"].size() > 0) {
    activation_setup_duration = (long)Double.parseDouble(doc["activationsetupduration.keyword"].value);
}
if (activation_setup_duration < limit) {
    emit(activation_setup_duration);
}
"""

NON_SHORT_JOB_HOURS_SRC = r"""
    long last_exec_time = 0;
    //if (doc.containsKey("lastremotewallclocktime") && doc["lastremotewallclocktime.keyword"].size() > 0) {
    //    long lrwct = (long)Double.parseDouble(doc["lastremotewallclocktime.keyword"].value);
    //    if (lrwct > 0 && lrwct < 3600*24*365) {
    //        last_exec_time = lrwct;
    //    }
    //}
    if (doc.containsKey("CommittedTime") && doc["CommittedTime"].size() > 0) {
        long ct = (long)doc["CommittedTime"].value;
        if (ct > 0 && ct < 3600*24*365) {
            last_exec_time = ct;
        }
    }
    if (last_exec_time > 60) {
        emit((double)last_exec_time / 3600.0);
    }
"""

INPUT_FILES_TRANSFERRED_SRC = r"""
long files = 0;
String key = "transferinputstats";
if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /FilesCountTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      files += Long.parseLong(matcher.group(1));
    }
}
emit(files);
"""

OUTPUT_FILES_TRANSFERRED_SRC = r"""
long files = 0;
String key = "transferoutputstats";
if (params._source.containsKey(key) && params._source[key].length() > 0) {
    def matcher = /FilesCountTotal\D+(\d+)[,}]/i.matcher(params._source[key]);
    while (matcher.find()) {
      files += Long.parseLong(matcher.group(1));
    }
}
emit(files);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)

    es_args = parser.add_argument_group("Elasticsearch-related options")
    for name, properties in ELASTICSEARCH_ARGS.items():
        es_args.add_argument(name, **properties)

    parser.add_argument("--project", required=True)
    parser.add_argument("--gpu", action="store_true",
                        help="Include All GPU Hours and %% Good GPU Hours columns")
    parser.add_argument("--use-chtc-projects", action="store_true",
                        help="Filter on CHTCProjects instead of ProjectName")
    parser.add_argument("--users", action="store_true",
                        help="Split usage by username")
    parser.add_argument("--anonymize", action="store_true",
                        help="Anonymize usernames")
    parser.add_argument("--start", type=valid_date)
    parser.add_argument("--end", type=valid_date)
    parser.add_argument(
        "--csv-file",
        nargs="?",
        const="",
        type=Path,
        help="Write output to a CSV file. If no path is given, writes to the current directory with an auto-generated name.",
    )
    parser.add_argument(
        "--html-file",
        nargs="?",
        const="",
        type=Path,
        help="Write HTML report to a file. If no path is given, writes to the current directory with an auto-generated name.",
    )

    return parser.parse_args()


def get_query(
        client,
        index,
        project,
        start,
        end,
        agg_users=False,
        use_chtc_projects=False,
        gpu=False,
        ):
    if use_chtc_projects:
        project_filter = Q("regexp", chtcprojects__keyword={"value": f"(.*,)?{re.escape(project)}(,.*)?"})
    else:
        project_filter = Q("term", ProjectName__keyword=project)
    query = Search(using=client, index=index) \
            .extra(size=0) \
            .extra(track_scores=False) \
            .extra(track_total_hits=True) \
            .filter(project_filter) \
            .filter("range", RecordTime={"gte": int(start.timestamp()), "lt": int(end.timestamp()), "format": "epoch_second"}) \
            .query(~Q("terms", JobUniverse=[7, 12]))

    runtime_mappings = {
        "runtime_mappings": {
            "LastExecWallClockTime": {
                "type": "long",
                "script": {
                    "source": LAST_EXEC_TIME_SRC,
                }
            },
            "IsCheckpointable": {
                "type": "boolean",
                "script": {
                    "source": CKPTABLE_SCRIPT_SRC,
                }
            },
            "JobUnits": {
                "type": "long",
                "script": {
                    "source": JOB_UNITS_SCRIPT_SRC,
                }
            },
            "FilesTransferred": {
                "type": "long",
                "script": {
                    "source": FILES_TRANSFERRED_SRC,
                }
            },
            "GbTransferred": {
                "type": "double",
                "script": {
                    "source": GB_TRANSFERRED_SRC,
                }
            },
            "OsdfFilesTransferred": {
                "type": "long",
                "script": {
                    "source": OSDF_FILES_TRANSFERRED_SRC,
                }
            },
            "OsdfGbTransferred": {
                "type": "double",
                "script": {
                    "source": OSDF_GB_TRANSFERRED_SRC,
                }
            },
            "OverRequestDisk": {
                "type": "boolean",
                "script": {
                    "source": OVER_REQUEST_DISK_SRC,
                }
            },
            "ActivationHours": {
                "type": "double",
                "script": {
                    "source": ACTIVATION_HOURS_SRC,
                }
            },
            "ActivationSetupSeconds": {
                "type": "long",
                "script": {
                    "source": ACTIVATION_SETUP_SECONDS_SRC,
                }
            },
            "NonShortJobHours": {
                "type": "double",
                "script": {
                    "source": NON_SHORT_JOB_HOURS_SRC,
                }
            },
            "InputFilesTransferred": {
                "type": "long",
                "script": {
                    "source": INPUT_FILES_TRANSFERRED_SRC,
                }
            },
            "OutputFilesTransferred": {
                "type": "long",
                "script": {
                    "source": OUTPUT_FILES_TRANSFERRED_SRC,
                }
            },
        }
    }
    for resource in RESOURCE_TYPES:
        runtime_mappings["runtime_mappings"][FLOORED_RESOURCE_FIELD.format(resource=resource)] = {
            "type": "long",
            "script": {
                "source": FLOORED_RESOURCE_SCRIPT.format(resource=resource, default=DEFAULT_RESOURCE_VALUE[resource])
            }
        }
    for new_key, old_key in HAS_FIELDS.items():
        runtime_mappings["runtime_mappings"][new_key] = {
            "type": "boolean",
            "script": {
                "source": HAS_SCRIPT_SRC.format(key=old_key)
            }
        }
    query.update_from_dict(runtime_mappings)

    users_agg = A(
        "terms",
        field="User.keyword"
    )

    metric_aggs = {}

    cpu_hours_agg = A(
        "sum",
        script={
            "lang": "painless",
            "inline": "doc['FlooredRequestCpus'].value * (doc['RemoteWallClockTime'].value / 3600.0)"
        }
    )
    metric_aggs["cpu_hours"] = cpu_hours_agg

    good_cpu_hours_agg = A(
        "sum",
        script={
            "lang": "painless",
            "inline": "doc['FlooredRequestCpus'].value * (doc['CommittedTime'].value / 3600.0)"
        }
    )
    metric_aggs["good_cpu_hours"] = good_cpu_hours_agg

    if gpu:
        gpu_jobs_agg = A(
            "filter",
            Q("range", RequestGpus={"gt": 0}),
        )
        metric_aggs["gpu_jobs"] = gpu_jobs_agg

        gpu_hours_agg = A(
            "sum",
            script={
                "lang": "painless",
                "inline": "doc['FlooredRequestGpus'].value * (doc['RemoteWallClockTime'].value / 3600.0)"
            }
        )
        metric_aggs["gpu_hours"] = gpu_hours_agg

        good_gpu_hours_agg = A(
            "sum",
            script={
                "lang": "painless",
                "inline": "doc['FlooredRequestGpus'].value * (doc['CommittedTime'].value / 3600.0)"
            }
        )
        metric_aggs["good_gpu_hours"] = good_gpu_hours_agg

    job_unit_hours_agg = A(
        "sum",
        script={
            "lang": "painless",
            "inline": "doc['JobUnits'].value * (doc['RemoteWallClockTime'].value / 3600.0)"
        }
    )
    metric_aggs["job_unit_hours"] = job_unit_hours_agg

    ckptable_jobs_agg = A(
        "filter",
        Q("term", IsCheckpointable=True)
    )
    metric_aggs["ckptable_jobs"] = ckptable_jobs_agg

    rm_jobs_agg = A(
        "filter",
        Q("term", JobStatus=3)
    )
    metric_aggs["rm_jobs"] = rm_jobs_agg

    files_transferred_agg = A(
        "sum",
        field="FilesTransferred"
    )
    metric_aggs["files_transferred"] = files_transferred_agg

    gb_transferred_agg = A(
        "sum",
        field="GbTransferred"
    )
    metric_aggs["gb_transferred"] = gb_transferred_agg

    osdf_files_transferred_agg = A(
        "sum",
        field="OsdfFilesTransferred"
    )
    metric_aggs["osdf_files_transferred"] = osdf_files_transferred_agg

    osdf_gb_transferred_agg = A(
        "sum",
        field="OsdfGbTransferred"
    )
    metric_aggs["osdf_gb_transferred"] = osdf_gb_transferred_agg

    shadow_starts_agg = A(
        "sum",
        field="NumShadowStarts",
    )
    metric_aggs["shadow_starts"] = shadow_starts_agg

    job_starts_agg = A(
        "sum",
        field="NumJobStarts",
    )
    metric_aggs["job_starts"] = job_starts_agg

    holds_agg = A(
        "sum",
        field="NumHolds",
    )
    metric_aggs["holds"] = holds_agg

    short_jobs_agg = A(
        "filter",
        Q("range", LastExecWallClockTime={"lt": 60}),
    )
    metric_aggs["short_jobs"] = short_jobs_agg

    multi_start_jobs_agg = A(
        "filter",
        Q("range", NumJobStarts={"gt": 1}),
    )
    metric_aggs["multi_start_jobs"] = multi_start_jobs_agg

    held_jobs_agg = A(
        "filter",
        Q("range", NumHolds={"gte": 1}),
    )
    metric_aggs["held_jobs"] = held_jobs_agg

    jobs_over_request_disk_agg = A(
        "filter",
        Q("term", OverRequestDisk=True),
    )
    metric_aggs["jobs_over_request_disk"] = jobs_over_request_disk_agg

    jobs_using_singularity_agg = A(
        "filter",
        Q("term", HasSingularityImage=True),
    )
    metric_aggs["jobs_using_singularity"] = jobs_using_singularity_agg

    mean_activation_hours_agg = A(
        "avg",
        field="ActivationHours",
    )
    metric_aggs["activation_hours"] = mean_activation_hours_agg

    mean_activation_setup_seconds_agg = A(
        "avg",
        field="ActivationSetupSeconds",
    )
    metric_aggs["activation_setup_seconds"] = mean_activation_setup_seconds_agg

    non_short_jobs_stats_agg = A(
        "extended_stats",
        field="NonShortJobHours",
    )
    metric_aggs["non_short_jobs_stats"] = non_short_jobs_stats_agg

    non_short_jobs_pcts_agg = A(
        "percentiles",
        field="NonShortJobHours",
        percents=[25, 50, 75, 95],
    )
    metric_aggs["non_short_jobs_pcts"] = non_short_jobs_pcts_agg

    input_files_transferred_agg = A(
        "sum",
        field="InputFilesTransferred",
    )
    metric_aggs["input_files_transferred"] = input_files_transferred_agg

    output_files_transferred_agg = A(
        "sum",
        field="OutputFilesTransferred",
    )
    metric_aggs["output_files_transferred"] = output_files_transferred_agg

    dag_node_jobs_agg = A(
        "filter",
        Q("term", HasDAGNodeName=True),
    )
    metric_aggs["dag_node_jobs"] = dag_node_jobs_agg

    max_request_memory_agg = A(
        "max",
        field="FlooredRequestMemory",
    )
    metric_aggs["max_request_memory"] = max_request_memory_agg

    med_used_memory_agg = A(
        "percentiles",
        field="MemoryUsage",
        percents=[50]
    )
    metric_aggs["med_used_memory"] = med_used_memory_agg

    max_used_memory_agg = A(
        "max",
        field="MemoryUsage",
    )
    metric_aggs["max_used_memory"] = max_used_memory_agg

    max_request_disk_agg = A(
        "max",
        field="FlooredRequestDisk",
    )
    metric_aggs["max_request_disk"] = max_request_disk_agg

    max_used_disk_agg = A(
        "max",
        field="DiskUsage",
    )
    metric_aggs["max_used_disk"] = max_used_disk_agg

    max_request_cpus_agg = A(
        "max",
        field="FlooredRequestCpus",
    )
    metric_aggs["max_request_cpus"] = max_request_cpus_agg

    med_job_units_agg = A(
        "percentiles",
        field="JobUnits",
        percents=[50],
    )
    metric_aggs["med_job_units"] = med_job_units_agg

    max_job_units_agg = A(
        "max",
        field="JobUnits"
    )
    metric_aggs["max_job_units"] = max_job_units_agg

    for agg_name, agg in metric_aggs.items():
        users_agg.metric(agg_name, agg)
        query.aggs.metric(agg_name, agg)

    if agg_users:
        query.aggs.bucket("users", users_agg)

    return query


def summarize_results(res, user: str, gpu=False) -> dict:
    o = {}
    if user == "TOTAL":
        jobs = res.hits.total.value
    else:
        jobs = res.doc_count
    if jobs == 0:
        return o
    if user == "TOTAL":
        a = res.aggregations
    else:
        a = res
    o["User"] = user
    o["Num Uniq Job Ids"] = jobs
    o["All CPU Hours"] = a.cpu_hours.value
    o["% Good CPU Hours"] = 100 * (a.good_cpu_hours.value / a.cpu_hours.value) if a.cpu_hours.value > 0 else 0
    if gpu:
        o["Num GPU Job Ids"] = a.gpu_jobs.doc_count
        o["All GPU Hours"] = a.gpu_hours.value
        o["% Good GPU Hours"] = 100 * (a.good_gpu_hours.value / a.gpu_hours.value) if a.gpu_hours.value > 0 else 0
    o["Job Unit Hours"] = a.job_unit_hours.value
    o["% Ckpt Able"] = 100 * (a.ckptable_jobs.doc_count / jobs)
    o["% Rm'd Jobs"] = 100 * (a.rm_jobs.doc_count / jobs)
    o["Total Files Xferd"] = a.files_transferred.value
    o["OSDF Files Xferd"] = a.osdf_files_transferred.value
    o["% OSDF Files"] = a.osdf_files_transferred.value / a.files_transferred.value if a.files_transferred.value > 0 else 0
    o["% OSDF Bytes"] = a.osdf_gb_transferred.value / a.gb_transferred.value if a.gb_transferred.value > 0 else 0
    o["Shadw Starts / Job Id"] = a.shadow_starts.value / jobs
    o["Exec Atts / Shadw Start"] = a.job_starts.value / a.shadow_starts.value if a.shadow_starts.value > 0 else 0
    o["Holds / Job Id"] = a.holds.value / jobs
    o["% Short Jobs"] = 100 * (a.short_jobs.doc_count / jobs)
    o["% Jobs w/>1 Exec Att"] = 100 * (a.multi_start_jobs.doc_count / jobs)
    o["% Jobs w/1+ Holds"] = 100 * (a.held_jobs.doc_count / jobs)
    o["% Jobs Over Rqst Disk"] = 100 * (a.jobs_over_request_disk.doc_count / jobs)
    o["% Jobs using S'ty"] = 100 * (a.jobs_using_singularity.doc_count / jobs)
    o["Mean Actv Hrs"] = a.activation_hours.value
    o["Mean Setup Secs"] = a.activation_setup_seconds.value
    o["Min Hrs"] = a.non_short_jobs_stats.min
    o["25% Hrs"] = a.non_short_jobs_pcts.values["25.0"]
    o["Med Hrs"] = a.non_short_jobs_pcts.values["50.0"]
    o["75% Hrs"] = a.non_short_jobs_pcts.values["75.0"]
    o["95% Hrs"] = a.non_short_jobs_pcts.values["95.0"]
    o["Max Hrs"] = a.non_short_jobs_stats.max
    o["Mean Hrs"] = a.non_short_jobs_stats.avg
    o["Std Hrs"] = a.non_short_jobs_stats.std_deviation
    o["Input Files / Exec Att"] = a.input_files_transferred.value / a.job_starts.value if a.job_starts.value > 0 else 0
    o["Output Files / Job"] = a.output_files_transferred.value / jobs
    # o["Good CPU Hours"] = a.good_cpu_hours.value
    o["CPU Hours / Bad Exec Att"] = max(a.cpu_hours.value - a.good_cpu_hours.value, 0) / max(a.job_starts.value - jobs, 1)
    # o["Num Exec Atts"] = a.job_starts.value
    # o["Num Shadw Starts"] = a.shadow_starts.value
    # o["Num Job Holds"] = a.holds.value
    # o["Num Rm'd Jobs"] = a.rm_jobs.doc_count
    o["Num DAG Node Jobs"] = a.dag_node_jobs.doc_count
    # o["Num Jobs w/>1 Exec Att"] = a.multi_start_jobs.doc_count
    # o["Num Jobs w>1+ Holds"] = a.held_jobs.doc_count
    o["Num Jobs Over Rqst Disk"] = a.jobs_over_request_disk.doc_count
    # o["Num Short Jobs"] = a.short_jobs.doc_count
    o["Num Ckpt Able Jobs"] = a.ckptable_jobs.doc_count
    o["Num S'ty Jobs"] = a.jobs_using_singularity.doc_count
    o["Max Rqst Mem MB"] = a.max_request_memory.value
    o["Med Used Mem MB"] = a.med_used_memory.values["50.0"]
    o["Max Used Mem MB"] = a.max_used_memory.value
    o["Max Rqst Disk GB"] = a.max_request_disk.value / (1000**2)
    o["Max Used Disk GB"] = a.max_used_disk.value / (1000**2)
    o["Max Rqst Cpus"] = a.max_request_cpus.value
    o["Med Job Units"] = a.med_job_units.values["50.0"]
    o["Max Job Units"] = a.max_job_units.value
    return o


def get_html(summary: dict, sort_col="Num Uniq Job Ids") -> str:

    def break_user(user: str):
        zwsp = "&#8203;"
        user = user.replace("@", f"@{zwsp}")
        return f"""<td style="{TEXT_TD_STYLE};">{user}</td>"""

    def num(x, dtype=int, fmt=",d"):
        try:
            return f"""<td style="{NUMERIC_TD_STYLE};">{dtype(x):{fmt}}</td>"""
        except TypeError:
            if x is None:
                return f"""<td style="{TEXT_TD_STYLE};"></td>"""
        return f"""<td style="{NUMERIC_TD_STYLE}; text-align: right">{x}</td>"""

    def hhmm(hours: float):
        if hours is None:
            return f"""<td style="{TEXT_TD_STYLE};"></td>"""
        # Convert float hours to HH:MM
        h = int(hours)
        m = int(60 * (float(hours) - int(hours)))
        return f"""<td style="{NUMERIC_TD_STYLE};">{h:02d}:{m:02d}</td>"""

    def get_fmt(col: str):
        custom_fmts = {
            "User":       break_user,
            "Min Hrs":    hhmm,
            "25% Hrs":    hhmm,
            "Med Hrs":    hhmm,
            "75% Hrs":    hhmm,
            "95% Hrs":    hhmm,
            "Max Hrs":    hhmm,
            "Mean Hrs":   hhmm,
            "Std Hrs":    hhmm,
            "Mean Actv Hrs": hhmm,
            "CPU Hours / Bad Exec Att": lambda x: num(x, dtype=float, fmt=".1f"),
            "% Good GPU Hours":         lambda x: num(x, dtype=float, fmt=".1f"),
            "Shadw Starts / Job Id":    lambda x: num(x, dtype=float, fmt=".2f"),
            "Exec Atts / Shadw Start":  lambda x: num(x, dtype=float, fmt=".3f"),
            "Holds / Job Id":           lambda x: num(x, dtype=float, fmt=".2f"),
            "OSDF Files Xferd":     lambda x: num(x, dtype=int, fmt=",d"),
            "% OSDF Files":         lambda x: num(x, dtype=float, fmt=".1f"),
            "% OSDF Bytes":         lambda x: num(x, dtype=float, fmt=".1f"),
            "% Rm'd Jobs":          lambda x: num(x, dtype=float, fmt=".0f"),
            "% Short Jobs":         lambda x: num(x, dtype=float, fmt=".0f"),
            "% Jobs w/>1 Exec Att": lambda x: num(x, dtype=float, fmt=".0f"),
            "% Jobs w/1+ Holds":    lambda x: num(x, dtype=float, fmt=".0f"),
            "% Jobs Over Rqst Disk": lambda x: num(x, dtype=float, fmt=".0f"),
            "% Ckpt Able":          lambda x: num(x, dtype=float, fmt=".0f"),
            "% Jobs using S'ty":    lambda x: num(x, dtype=float, fmt=".0f"),
            "Input Files / Exec Att": lambda x: num(x, dtype=float, fmt=".1f"),
            "Input MB / Exec Att":    lambda x: num(x, dtype=float, fmt=".0f"),
            "Input MB / File":        lambda x: num(x, dtype=float, fmt=".1f"),
            "Output Files / Job":     lambda x: num(x, dtype=float, fmt=".1f"),
            "Output MB / Job":        lambda x: num(x, dtype=float, fmt=".0f"),
            "Output MB / File":       lambda x: num(x, dtype=float, fmt=".1f"),
            "Max Rqst Disk GB":       lambda x: num(x, dtype=float, fmt=".1f"),
            "Max Used Disk GB":       lambda x: num(x, dtype=float, fmt=".1f"),
        }
        return custom_fmts.get(col, num)

    def get_legend():
        custom_items = {}
        custom_items["Num Uniq Job Ids"] = "Number of unique job ids across all execution attempts"
        custom_items["All CPU Hours"]    = "Total CPU hours for all execution attempts, including preemption and removal"
        custom_items["All GPU Hours"]    = "Total GPU hours for all execution attempts, including preemption and removal"
        custom_items["% Good CPU Hours"] = "Good CPU Hours per All CPU Hours, as a percentage"
        custom_items["% Good GPU Hours"] = "Good GPU Hours per All GPU Hours, as a percentage"
        custom_items["Good CPU Hours"]   = "Total CPU hours for execution attempts that ran to completion"
        custom_items["Job Units"]        = "The minimum number of base job units required to satisfy a job's resource requirements. The base job unit is 1 CPU, 4 GB memory, and 4 GB disk"
        custom_items["Job Unit Hours"]   = """Job units multiplied by total wallclock hours, like "All CPU Hours" but using "Job Units" instead of CPUs"""
        custom_items["Med Job Units"]    = "Median number of job units requested by the submitted jobs"
        custom_items["Max Job Units"]    = "Maximum number of job units requested by a submitted job"
        custom_items["Max Rqst Mem MB"]  = "Maximum memory requested across all submitted jobs in MB"
        custom_items["Max Used Mem MB"]  = "Maximum measured memory usage across all submitted jobs' last execution attempts in MB"
        custom_items["Max Rqst Cpus"]    = "Maximum number of CPUs requested across all submitted jobs"

        custom_items["Num Shadw Starts"] = "Total times a condor_shadow was spawned across all submitted jobs (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Exec Atts"]    = "Total number of execution attempts (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Rm'd Jobs"]    = "Number of jobs that were removed from the queue instead of allowing to complete"
        custom_items["Num Short Jobs"]   = "Number of execution attempts that completed in less than 60 seconds"
        custom_items["Num Jobs w/>1 Exec Att"] = "Number of unique jobs that were executed more than once"
        custom_items["Num Jobs Over Rqst Disk"] = "Number of unique jobs that went over their requested disk space"
        custom_items["Num Ckpt Able Jobs"] = "Number of unique jobs that may be using user-level checkpointing"

        custom_items["% Rm'd Jobs"] = "Percent of Num Uniq Job Ids that were removed"
        custom_items["Total Files Xferd"] = "Total files transferred (input and output) across all transfer protocols and execution attempts"
        custom_items["OSDF Files Xferd"] = "Total files transferred using the OSDF (f.k.a. Stash) transfer plugin. A dash (-) in this column means that no APs associated with this data had the capability to transfer files with the OSDF, whereas a zero (0) means that no OSDF file transfers occurred (though OSDF file transfers were possible)."
        custom_items["% OSDF Files"] = "Percent of all transferred files that were transferred using the OSDF plugin"
        custom_items["% OSDF Bytes"] = "Percent of all transferred bytes that were transferred using the OSDF plugin"
        custom_items["Shadw Starts / Job Id"]   = "Num Shadw Starts per Num Uniq Job Ids"
        custom_items["Exec Atts / Shadw Start"] = "Num Exec Atts per Num Shadw Starts"
        custom_items["Holds / Job Id"] = "Num Job Holds per Num Uniq Job Ids"

        custom_items["% Short Jobs"] = "Percent of Num Uniq Job Ids that were short jobs"
        custom_items["% Jobs w/>1 Exec Att"] = "Percent of Num Uniq Job Ids that had more than one execution attempt"
        custom_items["% Jobs w/1+ Holds"] = "Percent of Num Uniq Job Ids that had one or more jobs go on hold"
        custom_items["% Jobs Over Rqst Disk"] = "Percent of Num Uniq Job Ids that went over their requested disk space"
        custom_items["% Jobs using S'ty"] = "Percent of Num Uniq Job Ids that requested to run inside a Singularity image"
        custom_items["% Ckpt Able"] = "Percent of Num Uniq Job Ids that may be using user-level checkpointing"

        custom_items["Mean Actv Hrs"] = "Mean slot activation time (in hours)"
        custom_items["Mean Setup Secs"] = "Mean slot activation setup time (in seconds). The slot activation setup time is the duration from when a shadow sends a claim activation to when the shadow is told that a job's executable is running."

        custom_items["Min/25%/Median/75%/Max/Mean/Std Hrs"] = "Final execution wallclock hours that a non-short job (Min-Max) or jobs (Mean/Std) ran for (excluding Short jobs, excluding Local and Scheduler Universe jobs)"

        custom_items["Input Files / Exec Att"] = "Number of files (all protocols) sent as part of the input sandbox per job start"
        custom_items["Input MB / Exec Att"] = "Average size (in MB) of input sandboxes"
        custom_items["Input MB / File"] = "Average size of file in input sandboxes"

        custom_items["Output Files / Job"] = "Number of files transferred back to the access point (and other output locations) per (completed) job"
        custom_items["Output MB / Job"] = "Average size (in MB) of output sandboxes"
        custom_items["Output MB / File"] = "Average size of file in output sandboxes"

        custom_items["CPU Hours / Bad Exec Att"] = "Average CPU Hours used in a non-final execution attempt"

        custom_items["Med Used Mem MB"]  = "Median measured memory usage across all submitted jobs' last execution attempts in MB"
        custom_items["Max Rqst/Used Disk GB"] = "Maximum requested/used disk space across all submittted jobs' last execution attempts in GB"
        return custom_items

    html = []
    html.append(
        """<html>
<head>
</head>
<body>
<table style="border-collapse: collapse; border: 1px solid black;">
<tr>
"""
    )
    for _, col in sorted(DEFAULT_COLUMNS.items()):
        if col in summary["TOTAL"]:
            html.append(f"""\t<th style="border: 1px solid black;">{col}</th>\n""")
    html.append("</tr>\n<tr>\n")
    for _, col in sorted(DEFAULT_COLUMNS.items()):
        if col in summary["TOTAL"]:
            fmt = get_fmt(col)
            html.append(f"""\t{fmt(summary["TOTAL"][col])}\n""")
    html.append("</tr>\n")
    for _, user in sorted([(s[sort_col], s["User"]) for s in summary.values() if s["User"] != "TOTAL"], reverse=True):
        html.append("<tr>\n")
        for _, col in sorted(DEFAULT_COLUMNS.items()):
            if col in summary[user]:
                fmt = get_fmt(col)
                html.append(f"""\t{fmt(summary[user][col])}\n""")
        html.append("</tr>\n")
    html.append("</table>\n")
    html.append("<p>Legend:\n<ul>\n")
    for col, desc in get_legend().items():
        html.append(f"\t<li><strong>{col}</strong>: {desc}</li>\n")
    html.append("""</ul>
</body>
</html>
"""
    )
    return "".join(html)


def get_rows(summary: dict) -> list:
    rows = [summary["TOTAL"]]
    rows += [
        summary[user]
        for _, user in sorted(
            [(s.get("Num Uniq Job Ids", 0), s["User"]) for s in summary.values() if s["User"] != "TOTAL"],
            reverse=True,
        )
    ]
    return rows


def write_csv(summary: dict, csv_path: Path):
    if summary["TOTAL"]:
        cols = [col for _, col in sorted(DEFAULT_COLUMNS.items()) if col in summary["TOTAL"]]
        rows = get_rows(summary)
    else:
        cols = [col for _, col in sorted(DEFAULT_COLUMNS.items())]
        rows = []
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(rows)


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
    email_subject = f"{days}-day CHTC Usage Report for {args.project} starting {args.start.strftime(r'%Y-%m-%d')}"

    if not es_args.get("es_timeout"):
        es_args["es_timeout"] = 60 + int(10 * (days**0.75))

    es = connect(**es_args)
    es.info()

    query = get_query(
        client=es,
        index=index,
        project=args.project,
        start=args.start,
        end=args.end,
        agg_users=args.users,
        use_chtc_projects=args.use_chtc_projects,
        gpu=args.gpu,
    )

    try:
        result = query.execute()
    except Exception as err:
        error_str = None
        try:
            error_str = get_es_error(err.info)
        except Exception:
            pass
        if not error_str:
            error_str = traceback.format_exc()
        print(error_str, file=sys.stderr)
        if args.admin_to:
            send_email(
                subject=f"Error in {email_subject}",
                from_addr=args.from_addr,
                to_addrs=args.admin_to,
                html=f"<html><body><pre>{html.escape(error_str)}</pre></body></html>",
                smtp_server=args.smtp_server,
                smtp_username=args.smtp_username,
                smtp_password_file=args.smtp_password_file,
            )
        raise

    summary = {}
    summary["TOTAL"] = summarize_results(result, "TOTAL", gpu=args.gpu)
    if summary["TOTAL"]:
        if args.users:
            for i_user, bucket in enumerate(result.aggregations.users.buckets):
                user = bucket["key"]
                if args.anonymize:
                    user = f"user{i_user}"
                summary[user] = summarize_results(bucket, user, gpu=args.gpu)
        rows = get_rows(summary)
        print("[\n" + ",\n".join("  " + json.dumps(row) for row in rows) + "\n]")
        html_body = get_html(summary) if (args.to or args.html_file is not None) else None
        if args.html_file is not None:
            if args.html_file == "":
                html_path = Path(f"{args.project}_{args.start.strftime('%Y-%m-%d')}_{args.end.strftime('%Y-%m-%d')}.html")
            else:
                html_path = args.html_file
            html_path.write_text(html_body)
    else:
        print("Warning: No usage found during the reporting period.")
        html_body = "<html><body>No usage found during the reporting period</body></html>"
        if args.admin_to:
            send_email(
                subject=f"No usage found in {email_subject}",
                from_addr=args.from_addr,
                to_addrs=args.admin_to,
                html=f"<html><body><p>No usage found during the reporting period.</p>"
                     f"<p>Script called with: <pre>{html.escape(' '.join(sys.argv))}</pre></p></body></html>",
                smtp_server=args.smtp_server,
                smtp_username=args.smtp_username,
                smtp_password_file=args.smtp_password_file,
            )

    if args.csv_file is not None:
        if args.csv_file == "":
            csv_path = Path(f"{args.project}_{args.start.strftime('%Y-%m-%d')}_{args.end.strftime('%Y-%m-%d')}.csv")
        else:
            csv_path = args.csv_file
        write_csv(summary, csv_path)

    if args.to:  # only send email if at least one To: address is specified
        send_email(
            subject=email_subject,
            from_addr=args.from_addr,
            to_addrs=args.to,
            html=html_body,
            cc_addrs=args.cc,
            bcc_addrs=args.bcc,
            reply_to_addr=args.reply_to,
            smtp_server=args.smtp_server,
            smtp_username=args.smtp_username,
            smtp_password_file=args.smtp_password_file,
        )


if __name__ == "__main__":
    main()

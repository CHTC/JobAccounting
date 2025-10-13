import statistics as stats
from ast import literal_eval
from .BaseFilter import BaseFilter
from accounting.functions import get_job_units


DEFAULT_COLUMNS = {
    10: "Num Uniq Job Ids",
    20: "All CPU Hours",
    30: "% Good CPU Hours",
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
#    181: "Input MB / Exec Att",
#    182: "Input MB / File",
    190: "Output Files / Job",
#    191: "Output MB / Job",
#    192: "Output MB / File",

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


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "CommittedTime",
    "RequestCpus",
    "RequestMemory",
    "RequestDisk",
    "RecordTime",
    "JobStartDate",
    "JobCurrentStartDate",
    "MemoryUsage",
    "DiskUsage",
    "NumJobStarts",
    "NumShadowStarts",
    "NumHolds",
    "JobStatus",
    "EnteredCurrentStatus",
    "BytesSent",
    "BytesRecvd",
    "TransferInputStats",
    "TransferOutputStats",
    "SingularityImage",
    "ActivationDuration",
    "ActivationSetupDuration",
    "CondorVersion",
    "lastremotewallclocktime",
    "transferinputstats",
    "transferoutputstats",
    "activationduration",
    "activationsetupduration",
]


class ChtcScheddCpuFilter(BaseFilter):
    name = "CHTC schedd job history"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sort_col = "Num Uniq Job Ids"

    def get_query(self, index, start_ts, end_ts, **kwargs):
        # Returns dict matching Elasticsearch.search() kwargs
        # (Dict has same structure as the REST API query language)
        query = super().get_query(index, start_ts, end_ts, **kwargs)

        query.update({
            "body": {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {
                                "RecordTime": {
                                   "gte": start_ts,
                                    "lt": end_ts,
                                }
                            }},
                            {"regexp": {
                                "ScheddName.keyword": ".*[.]chtc[.]wisc[.]edu"
                            }}
                        ],
                        "must_not": [
                            {"terms": {
                                "JobUniverse": [7, 12]
                            }},
                        ],
                    }
                }
            }
        })
        return query

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        o = data["Schedds"][schedd]

        # Get list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Count number of checkpointable jobs
        if  (
                i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                i.get("Is_resumable", False)
            ) or (
                i.get("SuccessCheckpointExitBySignal", False) or
                i.get("SuccessCheckpointExitCode") is not None
            ):
            o["_NumCkptJobs"].append(1)
        else:
            o["_NumCkptJobs"].append(0)

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime", 0)
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0)))))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Compute job units
        if i.get("RemoteWallClockTime", 0) > 0:
            o["NumJobUnits"].append(get_job_units(
                cpus=i.get("RequestCpus", 1),
                memory_gb=i.get("RequestMemory", 1024)/1024,
                disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
            ))
        else:
            o["NumJobUnits"].append(None)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr in {"lastremotewallclocktime", "activationduration", "activationsetupduration"}:
                try:
                    o[attr].append(int(float(i.get(attr))))
                except TypeError:
                    o[attr].append(None)
            else:
                o[attr].append(i.get(attr, None))

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o = data["Users"][user]

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["ScheddName", "ProjectName"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Count number of checkpointable jobs
        if  (
                i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                i.get("Is_resumable", False)
            ) or (
                i.get("SuccessCheckpointExitBySignal", False) or
                i.get("SuccessCheckpointExitCode") is not None
            ):
            o["_NumCkptJobs"].append(1)
        else:
            o["_NumCkptJobs"].append(0)

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime", 0)
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0)))))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Compute job units
        if i.get("RemoteWallClockTime", 0) > 0:
            o["NumJobUnits"].append(get_job_units(
                cpus=i.get("RequestCpus", 1),
                memory_gb=i.get("RequestMemory", 1024)/1024,
                disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
            ))
        else:
            o["NumJobUnits"].append(None)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            # Use UNKNOWN for missing or blank ScheddName
            if attr in {"ScheddName", "ProjectName"}:
                o[attr].append(i.get(attr, i.get(attr.lower(), "UNKNOWN")) or "UNKNOWN")
            elif attr in {"lastremotewallclocktime", "activationduration", "activationsetupduration"}:
                try:
                    o[attr].append(int(float(i.get(attr))))
                except TypeError:
                    o[attr].append(None)
            else:
                o[attr].append(i.get(attr, None))


    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", i.get("projectname", "UNKNOWN")) or "UNKNOWN"
        o = data["Projects"][project]

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Count number of checkpointable jobs
        if  (
                i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                i.get("Is_resumable", False)
            ) or (
                i.get("SuccessCheckpointExitBySignal", False) or
                i.get("SuccessCheckpointExitCode") is not None
            ):
            o["_NumCkptJobs"].append(1)
        else:
            o["_NumCkptJobs"].append(0)

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime", 0)
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0)))))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Compute job units
        if i.get("RemoteWallClockTime", 0) > 0:
            o["NumJobUnits"].append(get_job_units(
                cpus=i.get("RequestCpus", 1),
                memory_gb=i.get("RequestMemory", 1024)/1024,
                disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
            ))
        else:
            o["NumJobUnits"].append(None)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr in {"lastremotewallclocktime", "activationduration", "activationsetupduration"}:
                try:
                    o[attr].append(int(float(i.get(attr))))
                except TypeError:
                    o[attr].append(None)
            else:
                o[attr].append(i.get(attr, None))


    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            self.user_filter,
            self.project_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Project and Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        if agg == "Users":
            columns[5] = "Most Used Project"
            columns[175] = "Most Used Schedd"
        if agg == "Projects":
            columns[5] = "Num Users"
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        return rows

    def compute_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        badput_cpu_time = []
        total_cpu_time = []
        for (last_wallclock_time, committed_time, badput_time, total_time, cpus) in zip(
                data["lastremotewallclocktime"],
                data["CommittedTime"],
                data["_BadWallClockTime"],
                data["RemoteWallClockTime"],
                data["RequestCpus"]):
            #goodput_time = last_wallclock_time or committed_time
            goodput_time = committed_time
            if cpus is not None:
                cpus = max(cpus, 1)  # assume at least 1 CPU even if 0 CPUs were stored in Elasticsearch
            if None in [goodput_time, cpus]:
                goodput_cpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)
            if None in [badput_time, cpus]:
                badput_cpu_time.append(None)
            else:
                badput_cpu_time.append(badput_time * cpus)
            if None in [total_time, cpus]:
                total_cpu_time.append(None)
            else:
                total_cpu_time.append(total_time * cpus)

        num_exec_attempts = []
        num_shadow_starts = []
        for (job_starts, shadow_starts) in zip(
                data["NumJobStarts"],
                data["NumShadowStarts"]):
            num_exec_attempts.append(job_starts)
            num_shadow_starts.append(shadow_starts)

        # Short jobs are jobs that ran for < 1 minute
        is_short_job = []
        for (last_wallclock_time, committed_time, record_date, start_date) in zip(
                data["lastremotewallclocktime"],
                data["CommittedTime"],
                data["RecordTime"],
                data["JobCurrentStartDate"]):
            #goodput_time = last_wallclock_time or committed_time
            goodput_time = committed_time
            if (goodput_time is not None) and (goodput_time > 0):
                is_short_job.append(goodput_time < 60)
            elif None in (record_date, start_date):
                is_short_job.append(None)
            else:
                is_short_job.append((record_date - start_date) < 60)

        # "Long" (i.e. "normal") jobs ran >= 1 minute
        # We only want to use these when computing percentiles,
        # so filter out short jobs and removed jobs,
        # and sort them so we can easily grab the percentiles later
        long_times_sorted = []
        for (is_short, last_wallclock_time, committed_time, job_status) in zip(
                is_short_job,
                data["lastremotewallclocktime"],
                data["CommittedTime"],
                data["JobStatus"]):
            #goodput_time = last_wallclock_time or committed_time
            goodput_time = committed_time
            if (is_short is False) and (job_status != 3):
                long_times_sorted.append(goodput_time)
        long_times_sorted = self.clean(long_times_sorted)
        long_times_sorted.sort()

        # File transfer stats
        input_files_total_count = []
        input_files_total_bytes = []
        input_files_total_job_starts = []
        output_files_total_count = []
        output_files_total_bytes = []
        output_files_total_job_stops = []
        osdf_files_count = 0
        osdf_bytes_total = 0
        for (
                job_status,
                job_starts,
                input_stats,
                input_cedar_bytes,
                output_stats,
                output_cedar_bytes,
            ) in zip(
                data["JobStatus"],
                data["NumJobStarts"],
                data["transferinputstats"],
                data["BytesRecvd"],
                data["transferoutputstats"],
                data["BytesSent"],
            ):

            input_files_count = 0
            input_files_bytes = 0
            if input_stats is None:
                input_files_total_count.append(None)
                input_files_total_job_starts.append(None)
            else:
                try:
                    if input_stats.endswith("..."):
                        input_stats = f"{input_stats[:input_stats.rindex(',')]}}}"
                    input_stats = literal_eval(input_stats)
                except SyntaxError:
                    input_files_total_count.append(None)
                    input_files_total_job_starts.append(None)
                    continue
                got_cedar_bytes = False
                for attr in input_stats:
                    if attr.casefold() in {"stashfilescounttotal", "osdffilescounttotal"}:
                        osdf_files_count += input_stats[attr]
                    if attr.casefold() in {"stashsizebytestotal", "osdfsizebytestotal"}:
                        osdf_bytes_total += input_stats[attr]
                    if attr.casefold().endswith("FilesCountTotal".casefold()):
                        input_files_count += input_stats[attr]
                    elif attr.casefold().endswith("SizeBytesTotal".casefold()):
                        input_files_bytes += input_stats[attr]
                        if attr.casefold() == "CedarSizeBytesTotal".casefold():
                            got_cedar_bytes = True
                if not got_cedar_bytes:
                    input_files_bytes += input_cedar_bytes
                input_files_total_count.append(input_files_count)
                input_files_total_bytes.append(input_files_bytes)
                input_files_total_job_starts.append(job_starts)

            output_files_count = 0
            output_files_bytes = 0
            if output_stats is None:
                output_files_total_count.append(None)
                output_files_total_job_stops.append(None)
            else:
                try:
                    if output_stats.endswith("..."):
                        output_stats = f"{output_stats[:output_stats.rindex(',')]}}}"
                    output_stats = literal_eval(output_stats)
                except SyntaxError:
                    output_files_total_count.append(None)
                    output_files_total_job_stops.append(None)
                    continue
                got_cedar_bytes = False
                for attr in output_stats:
                    if attr.casefold() in {"stashfilescounttotal", "osdffilescounttotal"}:
                        osdf_files_count += output_stats[attr]
                    if attr.casefold() in {"stashsizebytestotal", "osdfsizebytestotal"}:
                        osdf_bytes_total += output_stats[attr]
                    if attr.casefold().endswith("FilesCountTotal".casefold()):
                        output_files_count += output_stats[attr]
                    elif attr.casefold().endswith("SizeBytesTotal".casefold()):
                        output_files_bytes += output_stats[attr]
                        if attr.casefold() == "CedarSizeBytesTotal".casefold():
                            got_cedar_bytes = True
                if not got_cedar_bytes:
                    output_files_bytes += output_cedar_bytes
                output_files_total_count.append(output_files_count)
                output_files_total_bytes.append(output_files_bytes)
                output_files_total_job_stops.append(1)

        activation_durations = []
        setup_durations = []
        act_cutoff_date = 1_642_053_600  # 2022-01-13 HTCondor 9.5.0
        for (start_date, current_start_date, activation_duration, setup_duration) in zip(
                data["JobStartDate"],
                data["JobCurrentStartDate"],
                data["activationduration"],
                data["activationsetupduration"]):
            start_date = current_start_date or start_date
            if None in [start_date, activation_duration, setup_duration]:
                continue
            if ((start_date > act_cutoff_date) and
                (activation_duration < (act_cutoff_date - 24*3600) and
                (setup_duration < (act_cutoff_date - 24*3600)))):
                activation_durations.append(activation_duration)
                setup_durations.append(setup_duration)

        # Compute columns
        row["All CPU Hours"]    = sum(self.clean(total_cpu_time)) / 3600
        row["Good CPU Hours"]   = sum(self.clean(goodput_cpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Num Jobs Over Rqst Disk"] = sum([(usage or 0) > (request or 1)
            for (usage, request) in zip(data["DiskUsage"], data["RequestDisk"])])
        row["Num DAG Node Jobs"] = sum(data['_NumDAGNodes'])
        row["Num Rm'd Jobs"]    = sum([status == 3 for status in data["JobStatus"]])
        row["Num Job Holds"]    = sum(self.clean(data["NumHolds"]))
        row["Num Jobs w/1+ Holds"] = sum([holds > 0 for holds in self.clean(data["NumHolds"])])
        row["Num Jobs w/>1 Exec Att"] = sum([starts > 1 for starts in self.clean(data["NumJobStarts"])])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Disk GB"] = max(self.clean(data["RequestDisk"], allow_empty_list=False)) / (1000*1000)
        row["Max Used Disk GB"] = max(self.clean(data["DiskUsage"], allow_empty_list=False)) / (1000*1000)
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Num Exec Atts"]    = sum(self.clean(num_exec_attempts))
        row["Num Shadw Starts"] = sum(self.clean(num_shadow_starts))
        row["Num Ckpt Able Jobs"] = sum(data["_NumCkptJobs"])
        row["Num S'ty Jobs"]    = len(self.clean(data["SingularityImage"]))

        # Compute derivative columns
        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0
        if row["Num Uniq Job Ids"] > 0:
            row["% Ckpt Able"] = 100 * row["Num Ckpt Able Jobs"] / row["Num Uniq Job Ids"]
            row["Shadw Starts / Job Id"] = row["Num Shadw Starts"] / row["Num Uniq Job Ids"]
            row["Holds / Job Id"] = row["Num Job Holds"] / row["Num Uniq Job Ids"]
            row["% Rm'd Jobs"] = 100 * row["Num Rm'd Jobs"] / row["Num Uniq Job Ids"]
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs Over Rqst Disk"] = 100 * row["Num Jobs Over Rqst Disk"] / row["Num Uniq Job Ids"]
            row["% Jobs w/>1 Exec Att"] = 100 * row["Num Jobs w/>1 Exec Att"] / row["Num Uniq Job Ids"]
            row["% Jobs w/1+ Holds"] = 100 * row["Num Jobs w/1+ Holds"] / row["Num Uniq Job Ids"]
            row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = 0
            row["Holds / Job Id"] = 0
            row["% Rm'd Jobs"] = 0
            row["% Short Jobs"] = 0
            row["% Jobs Over Rqst Disk"] = 0
            row["% Jobs w/>1 Exec Att"] = 0
            row["% Jobs w/1+ Holds"] = 0
            row["% Jobs using S'ty"] = 0
        if row["Num Shadw Starts"] > 0:
            row["Exec Atts / Shadw Start"] = row["Num Exec Atts"] / row["Num Shadw Starts"]
        else:
            row["Exec Atts / Shadw Start"] = 0
        if sum(data["_NumBadJobStarts"]) > 0:
            row["CPU Hours / Bad Exec Att"] = (sum(self.clean(badput_cpu_time)) / 3600) / sum(data["_NumBadJobStarts"])
        else:
            row["CPU Hours / Bad Exec Att"] = 0

        # File transfer stats
        total_files = 0
        total_bytes = 0
        if any(input_files_total_job_starts):
            exec_atts = sum(self.clean(input_files_total_job_starts))
            input_files = sum(self.clean(input_files_total_count))
            input_mb = sum(self.clean(input_files_total_bytes)) / 1e6

            total_files += input_files
            total_bytes += input_mb * 1e6

            row["Total Input Files"] = input_files
            if exec_atts > 0:
                row["Input Files / Exec Att"] = input_files / exec_atts
                row["Input MB / Exec Att"] = input_mb / exec_atts
            if input_files > 0:
                row["Input MB / File"] = input_mb / input_files
                row["Total Files Xferd"] = row.get("Total Files Xferd", 0) + input_files

        if any(output_files_total_job_stops):
            exec_ends = sum(self.clean(output_files_total_job_stops))
            output_files = sum(self.clean(output_files_total_count))
            output_mb = sum(self.clean(output_files_total_bytes)) / 1e6

            total_files += output_files
            total_bytes += output_mb * 1e6

            row["Total Ouptut Files"] = output_files
            if exec_ends > 0:
                row["Output Files / Job"] = output_files / exec_ends
                row["Output MB / Job"] = output_mb / exec_ends
            if output_files > 0:
                row["Output MB / File"] = output_mb / output_files
                row["Total Files Xferd"] = row.get("Total Files Xferd", 0) + output_files

        if osdf_files_count == 0 or osdf_bytes_total == 0:
            condor_versions_set = set(data["CondorVersion"])
            condor_versions_set.discard(None)
            condor_versions_tuples_list = []
            for version in condor_versions_set:
                condor_versions_tuples_list.append(tuple([int(x) for x in version.split()[1].split(".")]))
            if all(condor_version_tuple < (9, 7, 0) for condor_version_tuple in condor_versions_tuples_list):
                row["OSDF Files Xferd"] = row["% OSDF Files"] = row["% OSDF Bytes"] = "-"
            else:
                row["OSDF Files Xferd"] = row["% OSDF Files"] = row["% OSDF Bytes"] = 0                    
        else:
            row["OSDF Files Xferd"] = osdf_files_count or ""
            if osdf_files_count > 0 and osdf_bytes_total > 0 and total_files > 0 and total_bytes > 0:
                row["% OSDF Files"] = 100 * osdf_files_count / total_files
                row["% OSDF Bytes"] = 100 * osdf_bytes_total / total_bytes
            else:
                row["% OSDF Files"] = row["% OSDF Bytes"] = ""

        # Insert missing value if any missing
        for key in ["Total Files Xferd", "Total Input Files", "Total Output Files",
                    "Input Files / Exec Att", "Output Files / Job",
                    "Input MB / Exec Att", "Output MB / Job",
                    "Input MB / File", "Output MB / File"]:
            row[key] = row.get(key, -999)

        # Compute activation time stats
        row["Mean Actv Hrs"] = ""
        row["Mean Setup Secs"] = ""
        if len(activation_durations) > 0:
            row["Mean Actv Hrs"] = (sum(activation_durations) / len(activation_durations)) / 3600
        if len(setup_durations) > 0:
            row["Mean Setup Secs"] = sum(setup_durations) / len(setup_durations)

        # Compute time percentiles and stats
        if len(long_times_sorted) > 0:
            row["Min Hrs"]  = long_times_sorted[ 0] / 3600
            row["25% Hrs"]  = long_times_sorted[  len(long_times_sorted)//4] / 3600
            row["Med Hrs"]  = stats.median(long_times_sorted) / 3600
            row["75% Hrs"]  = long_times_sorted[3*len(long_times_sorted)//4] / 3600
            row["95% Hrs"]  = long_times_sorted[int(0.95*len(long_times_sorted))] / 3600
            row["Max Hrs"]  = long_times_sorted[-1] / 3600
            row["Mean Hrs"] = stats.mean(long_times_sorted) / 3600
        else:
            for col in [f"{x} Hrs" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(long_times_sorted) > 1:
            row["Std Hrs"] = stats.stdev(long_times_sorted) / 3600
        else:
            # There is no variance if there is only one value
            row["Std Hrs"] = 0

        # Compute job unit metrics
        row["Med Job Units"] = stats.median(self.clean(data["NumJobUnits"], allow_empty_list=False))
        row["Max Job Units"] = max(self.clean(data["NumJobUnits"], allow_empty_list=False))
        row["Job Unit Hours"] = sum(
            job_units*wallclocktime/3600 for job_units, wallclocktime in zip(
                data.get("NumJobUnits", []),
                data.get("RemoteWallClockTime", []),
            )
            if job_units is not None
        )

        # Compute mode for Schedd columns in the Users table
        if agg == "Users":
            projects = self.clean(data["ProjectName"])
            if len(projects) > 0:
                row["Most Used Project"] = max(set(projects), key=projects.count)
            else:
                row["Most Used Project"] = "UNKNOWN"

            schedds = self.clean(data["ScheddName"])
            if len(schedds) > 0:
                row["Most Used Schedd"] = max(set(schedds), key=schedds.count)
            else:
                row["Most Used Schedd"] = "UNKNOWN"
        if agg == "Projects":
            row["Num Users"] = len(set(data["User"]))

        return row

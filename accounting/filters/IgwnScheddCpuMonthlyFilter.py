from collections import defaultdict
from operator import itemgetter
from .BaseFilter import BaseFilter
from accounting.functions import get_job_units

MAX_INT = 2**62

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

    110: "Min Hrs",
    150: "Max Hrs",
    160: "Mean Hrs",

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
    520: "Max Used Mem MB",
    525: "Max Rqst Disk GB",
    527: "Max Used Disk GB",
    530: "Max Rqst Cpus",
    545: "Max Job Units",
}


class IgwnScheddCpuMonthlyFilter(BaseFilter):
    name = "IGWN schedd job history"

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
                        ],
                        "must_not": [
                            {"term": {
                                "MATCH_EXP_JOB_GLIDEIN_Site.keyword": "Unknown"
                            }},
                            {"terms": {
                                "JobUniverse": [7, 12]
                            }},
                        ],
                    }
                }
            }
        })
        return query

    def reduce_data(self, i, o, t, is_site=False):

        is_removed = i.get("JobStatus") == 3
        is_dagnode = i.get("DAGNodeName") is not None
        is_exec = i.get("NumJobStarts", 0) >= 1
        is_multiexec = i.get("NumJobStarts", 0) > 1
        has_holds = i.get("NumHolds", 0) > 0
        is_short = False
        is_long = False
        is_singularity = i.get("SingularityImage") is not None
        is_checkpointable = ((
                i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                i.get("Is_resumable", False)
            ) or (
                i.get("SuccessCheckpointExitBySignal", False) or
                i.get("SuccessCheckpointExitCode") is not None
        ))
        is_over_disk_request = i.get("DiskUsage", 0) > i.get("RequestDisk", 1)
        goodput_time = 0
        if not is_removed:
            goodput_time = int(float(i.get("lastremotewallclockTime", i.get("CommittedTime", 0))))
            if goodput_time > 0 and goodput_time < 60:
                is_short = True
            elif None in [i.get("RecordTime"), i.get("JobCurrentStartDate")]:
                if goodput_time == 0:
                    is_short = True
            elif i.get("RecordTime") - i.get("JobCurrentStartDate") < 60:
                is_short = True
            else:
                is_long = True
        elif not is_removed:
            goodput_time = int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0))))
        input_files = output_files = 0
        input_bytes = output_bytes = 0
        osdf_files = osdf_bytes = 0
        job_units = 0
        if not is_site:
            input_file_stats = i.get("TransferInputStats", {})
            output_file_stats = i.get("TransferOutputStats", {})
            got_cedar_input_bytes = False
            got_cedar_output_bytes = False
            for key, value in input_file_stats.items():
                if key.casefold() in {"stashfilescounttotal", "osdffilescounttotal"}:
                        osdf_files += value
                elif key.casefold() in {"stashsizebytestotal", "osdfsizebytestotal"}:
                        osdf_bytes += value
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    input_files += value
                elif key.casefold().endswith("SizeBytesTotal".casefold()):
                    input_bytes += value
                    if key.casefold() == "CedarSizeBytesTotal".casefold():
                        got_cedar_input_bytes = True
            for key, value in output_file_stats.items():
                if key.casefold() in {"stashfilescounttotal", "osdffilescounttotal"}:
                        osdf_files += value
                elif key.casefold() in {"stashsizebytestotal", "osdfsizebytestotal"}:
                        osdf_bytes += value
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    output_files += value
                elif key.casefold().endswith("SizeBytesTotal".casefold()):
                    output_bytes += value
                    if key.casefold() == "CedarSizeBytesTotal".casefold():
                        got_cedar_output_bytes = True
            if not (got_cedar_input_bytes or got_cedar_output_bytes):
                input_bytes += i.get("BytesRecvd", 0)
                output_bytes += i.get("BytesSent", 0)
        job_units = get_job_units(
            cpus=i.get("RequestCpus", 1),
            memory_gb=i.get("RequestMemory", 1024)/1024,
            disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
        )
        long_job_wallclock_time = int(is_long) * int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 60))))

        sum_cols = {}
        sum_cols["Jobs"] = 1
        sum_cols["RunJobs"] = int(is_exec)
        sum_cols["RmJobs"] = int(is_removed)
        sum_cols["DAGNodeJobs"] = int(is_dagnode)
        sum_cols["MultiExecJobs"] = int(is_multiexec)
        sum_cols["ShortJobs"] = int(is_short)
        sum_cols["LongJobs"] = int(is_long)
        sum_cols["CheckpointableJobs"] = int(is_checkpointable)
        sum_cols["SingularityJobs"] = int(is_singularity)
        sum_cols["OverDiskJobs"] = int(is_over_disk_request)

        sum_cols["TotalLongJobWallClockTime"] = long_job_wallclock_time
        sum_cols["GoodCpuTime"] = (goodput_time * max(i.get("RequestCpus", 1), 1))
        sum_cols["CpuTime"] = (i.get("RemoteWallClockTime", 0) * max(i.get("RequestCpus", 1), 1))
        sum_cols["BadCpuTime"] = ((i.get("RemoteWallClockTime", 0) - goodput_time) * max(i.get("RequestCpus", 1), 1))
        sum_cols["JobUnitTime"] = job_units * i.get("RemoteWallClockTime", 0)
        sum_cols["NumShadowStarts"] = i.get("NumShadowStarts", 0)
        sum_cols["NumJobStarts"] = i.get("NumJobStarts", 0)
        sum_cols["NumBadJobStarts"] = max(i.get("NumJobStarts", 0) - 1, 0)
        sum_cols["HeldJobs"] = int(has_holds)
        sum_cols["NumJobHolds"] = i.get("NumHolds", 0)
        if input_files > 0:
            sum_cols["InputFiles"] = input_files
            sum_cols["InputBytes"] = input_bytes
        if output_files > 0:
            sum_cols["OutputFiles"] = output_files
            sum_cols["OutputBytes"] = output_bytes
        if input_files > 0 or output_files > 0:
            sum_cols["TotalFiles"] = input_files + output_files
            sum_cols["TotalBytes"] = input_bytes + output_bytes
            sum_cols["OSDFFiles"] = osdf_files
            sum_cols["OSDFBytes"] = osdf_bytes

        max_cols = {}
        max_cols["MaxLongJobWallClockTime"] = long_job_wallclock_time
        max_cols["MaxRequestMemory"] = i.get("RequestMemory", 0)
        max_cols["MaxMemoryUsage"] = i.get("MemoryUsage", 0)
        max_cols["MaxRequestDisk"] = i.get("RequestDisk", 0)
        max_cols["MaxDiskUsage"] = i.get("DiskUsage", 0)
        max_cols["MaxRequestCpus"] = i.get("RequestCpus", 1)
        max_cols["MaxJobUnits"] = job_units

        min_cols = {}
        min_cols["MinLongJobWallClockTime"] = long_job_wallclock_time

        for col in sum_cols:
            o[col] = (o.get(col) or 0) + sum_cols[col]
            t[col] = (t.get(col) or 0) + sum_cols[col]
        for col in max_cols:
            o[col] = max([(o.get(col) or 0), max_cols[col]])
            t[col] = max([(t.get(col) or 0), max_cols[col]])
        for col in min_cols:
            o[col] = min([(o.get(col) or MAX_INT), min_cols[col]])
            t[col] = min([(t.get(col) or MAX_INT), min_cols[col]])

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        output = data["Schedds"][schedd]
        total = data["Schedds"]["TOTAL"]

        self.reduce_data(i, output, total)

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        output = data["Users"][user]
        total = data["Users"]["TOTAL"]

        self.reduce_data(i, output, total)

        counter_cols = {}
        counter_cols["ScheddNames"] = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"

        for col in counter_cols:
            if not col in output:
                output[col] = defaultdict(int)
            output[col][counter_cols[col]] += 1
            if not col in total:
                total[col] = defaultdict(int)
            total[col][counter_cols[col]] += 1

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            self.user_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        if agg == "Users":
            columns[175] = "Most Used Schedd"
        return columns

    def compute_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute columns
        row["All CPU Hours"]     = data["CpuTime"] / 3600
        row["Good CPU Hours"]    = data["GoodCpuTime"] / 3600
        row["Job Unit Hours"]    = data["JobUnitTime"] / 3600
        row["Num Uniq Job Ids"]  = data["Jobs"]
        row["Num DAG Node Jobs"] = data["DAGNodeJobs"]
        row["Num Rm'd Jobs"]     = data["RmJobs"]
        row["Num Job Holds"]    = data["NumJobHolds"]
        row["Num Jobs w/1+ Holds"] = data["HeldJobs"]
        row["Num Jobs Over Rqst Disk"] = data["OverDiskJobs"]
        row["Num Jobs w/>1 Exec Att"] = data["MultiExecJobs"]
        row["Num Short Jobs"]   = data["ShortJobs"]
        row["Max Rqst Mem MB"]  = data["MaxRequestMemory"]
        row["Max Used Mem MB"]  = data["MaxMemoryUsage"]
        row["Max Rqst Disk GB"] = data["MaxRequestDisk"] / (1024*1024)
        row["Max Used Disk GB"] = data["MaxDiskUsage"] / (1024*1024)
        row["Max Rqst Cpus"]    = data["MaxRequestCpus"]
        row["Max Job Units"]    = data["MaxJobUnits"]
        row["Num Exec Atts"]    = data["NumJobStarts"]
        row["Num Shadw Starts"] = data["NumShadowStarts"]
        row["Num Ckpt Able Jobs"]  = data["CheckpointableJobs"]
        row["Num S'ty Jobs"]       = data["SingularityJobs"]

        row["Total Files Xferd"] = data.get("TotalFiles", "")
        if data["NumJobStarts"] > 0 and data.get("InputFiles") is not None:
            row["Input Files / Exec Att"] = data["InputFiles"] / data["NumJobStarts"]
            row["Input MB / Exec Att"] = (data["InputBytes"] / data["NumJobStarts"]) / (1024*1024)
        else:
            row["Input Files / Exec Att"] = row["Input MB / Exec Att"] = ""
        if data["Jobs"] > 0 and data.get("OutputFiles") is not None:
            row["Output Files / Job"] = data["OutputFiles"] / data["Jobs"]
            row["Output MB / Job"] = (data["OutputBytes"] / data["Jobs"]) / (1024*1024)
        else:
            row["Output Files / Job"] = row["Output MB / Job"] = ""

        row["OSDF Files Xferd"] = data.get("OSDFFiles", "") or ""
        if data.get("OSDFFiles", 0) > 0 and data.get("TotalFiles", 0) > 0:
            row["% OSDF Files"] = 100 * (data["OSDFFiles"] / data["TotalFiles"])
            row["% OSDF Bytes"] = 100 * (data["OSDFBytes"] / data["TotalBytes"])
        else:
            row["% OSDF Files"] = row["% OSDF Bytes"] = ""

        # Compute derivative columns
        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0
        if row["Num Uniq Job Ids"] > 0:
            row["Shadw Starts / Job Id"] = row["Num Shadw Starts"] / row["Num Uniq Job Ids"]
            row["Holds / Job Id"] = row["Num Job Holds"] / row["Num Uniq Job Ids"]
            row["% Rm'd Jobs"] = 100 * row["Num Rm'd Jobs"] / row["Num Uniq Job Ids"]
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs w/>1 Exec Att"] = 100 * row["Num Jobs w/>1 Exec Att"] / row["Num Uniq Job Ids"]
            row["% Jobs w/1+ Holds"] = 100 * row["Num Jobs w/1+ Holds"] / row["Num Uniq Job Ids"]
            row["% Jobs Over Rqst Disk"] = 100 * row["Num Jobs Over Rqst Disk"] / row["Num Uniq Job Ids"]
            row["% Ckpt Able"] = 100 * row["Num Ckpt Able Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = 0
            row["Holds / Job Id"] = 0
            row["% Rm'd Jobs"] = 0
            row["% Short Jobs"] = 0
            row["% Jobs w/>1 Exec Att"] = 0
            row["% Jobs w/1+ Holds"] = 0
            row["% Jobs Over Rqst Disk"] = 0
            row["% Jobs using S'ty"] = 0
        if row["Num Shadw Starts"] > 0:
            row["Exec Atts / Shadw Start"] = row["Num Exec Atts"] / row["Num Shadw Starts"]
        else:
            row["Exec Atts / Shadw Start"] = 0
        if data["NumBadJobStarts"] > 0:
            row["CPU Hours / Bad Exec Att"] = (data["BadCpuTime"] / data["NumBadJobStarts"]) / 3600
        else:
            row["CPU Hours / Bad Exec Att"] = 0

        if data["LongJobs"] > 0:
            row["Min Hrs"]  = data["MinLongJobWallClockTime"] / 3600
            row["Max Hrs"]  = data["MaxLongJobWallClockTime"] / 3600
            row["Mean Hrs"] = (data["TotalLongJobWallClockTime"] / data["LongJobs"]) / 3600
        else:
            row["Min Hrs"] = row["Max Hrs"] = row["Mean Hrs"] = 0

        # Compute mode for Project and Schedd columns in the Users table
        if agg == "Users":
            schedds = data["ScheddNames"]
            if len(schedds) > 0:
                row["Most Used Schedd"] = max(schedds.items(), key=itemgetter(1))[0]
            else:
                row["Most Used Schedd"] = "UNKNOWN"

        return row

    def scan_and_filter(self, es_index, start_ts, end_ts, **kwargs):
        return super().scan_and_filter(es_index, start_ts, end_ts, build_totals=False, **kwargs)

    def merge_filtered_data(self, data, agg):
        # Takes filtered data and an aggregation level (e.g. Users, Schedds,
        # Projects) and returns a list of tuples, with the first item
        # containing a tuple of column names in the order that matches
        # the following tuples of values

        # Get the dict of columns
        columns = self.add_custom_columns(agg)

        # Make the 0th column the aggregation level
        # e.g. agg on "Users"   -> "User" column
        #      agg on "Schedds" -> "Schedd" column
        columns[0] = agg.rstrip("s")

        # Get the names of the columns in order
        columns_sorted = [col for (n, col) in sorted(columns.items())]

        # Loop over aggregated data and store computed data in rows
        rows = []
        for agg_name, d in data[agg].items():
            row = {}

            # It's possible to have no job data stored
            # if the dict was initialized but then job data was skipped
            if d.get("Jobs", 0) == 0:
                continue

            # Put the name for this aggregation
            # (e.g. user name, schedd name, project name)
            # as the value for the 0th column
            row[columns[0]] = agg_name

            # Compute any custom columns
            row.update(self.compute_custom_columns(d, agg, agg_name))

            # Store a tuple of all column data in order
            rows.append(tuple(row[col] for col in columns_sorted))

        # Sort rows by All CPU Hours
        rows.sort(reverse=True, key=itemgetter(columns_sorted.index(self.sort_col)))

        # Prepend the header row
        rows.insert(0, tuple(columns_sorted))

        return rows


import statistics as stats
from collections import defaultdict
from operator import itemgetter
from ast import literal_eval
import elasticsearch.helpers
from .BaseFilter import BaseFilter

MAX_INT = 2**62

DEFAULT_COLUMNS = {
    10: "All CPU Hours",
    20: "Num Uniq Job Ids",
    30: "% Good CPU Hours",

    45: "% Ckpt Able",
    50: "% Rm'd Jobs",
    60: "% Short Jobs",
    70: "% Jobs w/>1 Exec Att",
    80: "% Jobs w/1+ Holds",
    83: "Total Files Xferd",

    85: "Shadw Starts / Job Id",
    90: "Exec Atts / Shadw Start",
    95: "Holds / Job Id",

    110: "Min Hrs",
    150: "Max Hrs",
    160: "Mean Hrs",

    180: "Input Files / Exec Att",
#    181: "Input MB / Exec Att",
#    182: "Input MB / File",
    190: "Output Files / Job",
#    191: "Output MB / Job",
#    192: "Output MB / File",

    200: "Max Rqst Mem MB",
    220: "Max Used Mem MB",
    230: "Max Rqst Cpus",

    300: "Good CPU Hours",
    305: "CPU Hours / Bad Exec Att",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    325: "Num Job Holds",
    330: "Num Rm'd Jobs",
    340: "Num DAG Node Jobs",
    350: "Num Jobs w/>1 Exec Att",
    355: "Num Jobs w/1+ Holds",
    360: "Num Short Jobs",
    370: "Num Local Univ Jobs",
    380: "Num Sched Univ Jobs",
    390: "Num Ckpt Able Jobs",
}


class ChtcScheddCpuMonthlyFilter(BaseFilter):
    name = "CHTC schedd job history"

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
                        ]
                    }
                }
            }
        })
        return query

    def reduce_data(self, i, o, t, is_site=False):

        is_removed = i.get("JobStatus") == 3
        is_scheduler = i.get("JobUniverse") == 7
        is_local = i.get("JobUniverse") == 12
        has_shadow = not (is_scheduler or is_local)
        is_dagnode = i.get("DAGNodeName") is not None and i.get("JobUniverse", 5) != 12
        is_exec = i.get("NumJobStarts", 0) >= 1
        is_multiexec = i.get("NumJobStarts", 0) > 1
        has_holds = i.get("NumHolds", 0) > 0
        is_short = False
        is_long = False
        is_ckptable = False
        goodput_time = 0
        if has_shadow and not is_removed:
            goodput_time = int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0))))
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
        input_files = 0
        output_files = 0
        if has_shadow and not is_site:
            is_ckptable = ((
                    i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                    i.get("Is_resumable", False)
                ) or (
                    i.get("SuccessCheckpointExitBySignal", False) or
                    i.get("SuccessCheckpointExitCode") is not None
                ))

            try:
                transferinputstats = i.get("transferinputstats", "{}")
                if transferinputstats.endswith("..."):
                    transferinputstats = f"{transferinputstats[:transferinputstats.rindex(',')]}}}"
                input_file_stats = literal_eval(i.get("transferinputstats", "{}"))
            except SyntaxError:
                input_file_stats = {}
            try:
                transfeoutputstats = i.get("transfeoutputstats", "{}")
                if transfeoutputstats.endswith("..."):
                    transfeoutputstats = f"{transfeoutputstats[:transfeoutputstats.rindex(',')]}}}"
                output_file_stats = literal_eval(i.get("transfeoutputstats", "{}"))
            except SyntaxError:
                output_file_stats = {}

            for key, value in input_file_stats.items():
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    input_files += value
            for key, value in output_file_stats.items():
                if key.casefold().endswith("FilesCountTotal".casefold()):
                    output_files += value
        long_job_wallclock_time = int(is_long) * i.get("LastRemoteWallClockTime", i.get("CommittedTime", 60))

        sum_cols = {}
        sum_cols["Jobs"] = 1
        sum_cols["RunJobs"] = int(is_exec)
        sum_cols["RmJobs"] = int(is_removed)
        sum_cols["SchedulerJobs"] = int(is_scheduler)
        sum_cols["LocalJobs"] = int(is_local)
        sum_cols["DAGNodeJobs"] = int(is_dagnode)
        sum_cols["MultiExecJobs"] = int(is_multiexec)
        sum_cols["ShortJobs"] = int(is_short)
        sum_cols["LongJobs"] = int(is_long)
        sum_cols["ShadowJobs"] = int(has_shadow)
        sum_cols["Checkpointable"] = int(is_ckptable)

        sum_cols["TotalLongJobWallClockTime"] = long_job_wallclock_time
        sum_cols["GoodCpuTime"] = (goodput_time * max(i.get("RequestCpus", 1), 1))
        sum_cols["CpuTime"] = (i.get("RemoteWallClockTime", 0) * max(i.get("RequestCpus", 1), 1))
        sum_cols["BadCpuTime"] = ((i.get("RemoteWallClockTime", 0) - goodput_time) * max(i.get("RequestCpus", 1), 1))
        sum_cols["NumShadowStarts"] = int(has_shadow) * i.get("NumShadowStarts", 0)
        sum_cols["NumJobStarts"] = int(has_shadow) * i.get("NumJobStarts", 0)
        sum_cols["NumBadJobStarts"] = int(has_shadow) * max(i.get("NumJobStarts", 0) - 1, 0)
        sum_cols["HeldJobs"] = int(has_holds)
        sum_cols["NumJobHolds"] = i.get("NumHolds", 0)
        if input_files > 0:
            sum_cols["InputFiles"] = input_files
        if output_files > 0:
            sum_cols["OutputFiles"] = output_files
        if input_files > 0 or output_files > 0:
            sum_cols["TotalFiles"] = input_files + output_files

        max_cols = {}
        max_cols["MaxLongJobWallClockTime"] = long_job_wallclock_time
        max_cols["MaxRequestMemory"] = i.get("RequestMemory", 0)
        max_cols["MaxMemoryUsage"] = i.get("MemoryUsage", 0)
        max_cols["MaxRequestCpus"] = i.get("RequestCpus", 1)

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
        counter_cols["ProjectNames"] = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"

        for col in counter_cols:
            if not col in output:
                output[col] = defaultdict(int)
            output[col][counter_cols[col]] += 1
            if not col in total:
                total[col] = defaultdict(int)
            total[col][counter_cols[col]] += 1


    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"
        output = data["Projects"][project]
        total = data["Projects"]["TOTAL"]

        self.reduce_data(i, output, total)

        dict_cols = {}
        dict_cols["Users"] = i.get("User", "UNKNOWN") or "UNKNOWN"

        for col in dict_cols:
            output[col] = output.get(col) or {}
            output[col][dict_cols[col]] = 1
            total[col] = total.get(col) or {}
            total[col][dict_cols[col]] = 1


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

    def compute_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute columns
        row["All CPU Hours"]     = data["CpuTime"] / 3600
        row["Num Uniq Job Ids"]  = data["Jobs"]
        row["Good CPU Hours"]    = data["GoodCpuTime"] / 3600

        row["Num DAG Node Jobs"] = data["DAGNodeJobs"]
        row["Num Rm'd Jobs"]     = data["RmJobs"]
        row["Num Jobs w/>1 Exec Att"] = data["MultiExecJobs"]
        row["Num Jobs w/1+ Holds"] = data["HeldJobs"]
        row["Total Files Xferd"] = data.get("TotalFiles", "")

        row["Num Short Jobs"]      = data["ShortJobs"]
        row["Max Rqst Mem MB"]     = data["MaxRequestMemory"]
        row["Max Used Mem MB"]     = data["MaxMemoryUsage"]
        row["Max Rqst Cpus"]       = data["MaxRequestCpus"]
        row["Num Exec Atts"]       = data["NumJobStarts"]
        row["Num Shadw Starts"]    = data["NumShadowStarts"]
        row["Num Job Holds"]       = data["NumJobHolds"]
        row["Num Local Univ Jobs"] = data["LocalJobs"]
        row["Num Sched Univ Jobs"] = data["SchedulerJobs"]
        row["Num Ckpt Able Jobs"]  = data["Checkpointable"]

        if data["NumJobStarts"] > 0 and data.get("InputFiles") is not None:
            row["Input Files / Exec Att"] = data["InputFiles"] / data["NumJobStarts"]
        else:
            row["Input Files / Exec Att"] = ""
        if data["ShadowJobs"] > 0 and data.get("OutputFiles") is not None:
            row["Output Files / Job"] = data["OutputFiles"] / data["ShadowJobs"]
        else:
            row["Output Files / Job"] = ""

        # Compute derivative columns
        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0
        if row["Num Uniq Job Ids"] > 0:
            row["Shadw Starts / Job Id"] = row["Num Shadw Starts"] / row["Num Uniq Job Ids"]
            row["% Rm'd Jobs"] = 100 * row["Num Rm'd Jobs"] / row["Num Uniq Job Ids"]
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs w/>1 Exec Att"] = 100 * row["Num Jobs w/>1 Exec Att"] / row["Num Uniq Job Ids"]
            row["% Jobs w/1+ Holds"] = 100 * row["Num Jobs w/1+ Holds"] / row["Num Uniq Job Ids"]
            row["Holds / Job Id"] = row["Num Job Holds"] / row["Num Uniq Job Ids"]
            row["% Ckpt Able"] = 100 * row["Num Ckpt Able Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = 0
            row["% Rm'd Jobs"] = 0
            row["% Short Jobs"] = 0
            row["% Jobs w/>1 Exec Att"] = 0
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
            projects = data["ProjectNames"]
            if len(projects) > 0:
                row["Most Used Project"] = max(projects.items(), key=itemgetter(1))[0]
            else:
                row["Most Used Project"] = "UNKNOWN"

            schedds = data["ScheddNames"]
            if len(schedds) > 0:
                row["Most Used Schedd"] = max(schedds.items(), key=itemgetter(1))[0]
            else:
                row["Most Used Schedd"] = "UNKNOWN"
        if agg == "Projects":
            row["Num Users"] = len(data["Users"])

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
        rows.sort(reverse=True, key=itemgetter(columns_sorted.index("All CPU Hours")))

        # Prepend the header row
        rows.insert(0, tuple(columns_sorted))

        return rows

import logging
import htcondor
import statistics as stats
from pathlib import Path
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    10: "All CPU Hours",
    20: "Num Uniq Job Ids",
    30: "% Good CPU Hours",

    50: "% Rm'd Jobs",
    60: "% Short Jobs",
    70: "% Jobs w/>1 Exec Att",
    80: "% Jobs w/1+ Holds",
    82: "% Jobs using S'ty",

    85: "Shadw Starts / Job Id",
    90: "Exec Atts / Shadw Start",
    95: "Holds / Job Id",

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

    180: "Input Files Xferd / Exec Att",
    181: "Input MB Xferd / Exec Att",
    182: "Input MB / File",
    190: "Output Files Xferd / Exec Att",
    191: "Output MB Xferd / Exec Att",
    192: "Output MB / File",

    200: "Max Rqst Mem MB",
    210: "Med Used Mem MB",
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
    390: "Num S'ty Jobs",
}


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "CommittedTime",
    "RequestCpus",
    "RequestMemory",
    "RecordTime",
    "JobCurrentStartDate",
    "MemoryUsage",
    "NumJobStarts",
    "NumShadowStarts",
    "NumHolds",
    "JobUniverse",
    "JobStatus",
    "EnteredCurrentStatus",
    "BytesSent",
    "BytesRecvd",
    "TransferInputFilesCount",
    "TransferOutputFilesCount",
    "TransferInputFilesTotalCount",
    "TransferOutputFilesTotalCount",
    "SingularityImage",
    "ActivationDuration",
    "ActivationSetupDuration",
]


class OsgScheddCpuFilter(BaseFilter):
    name = "OSG schedd job history"

    def __init__(self, **kwargs):
        self.collector_host = "flock.opensciencegrid.org"
        self.schedd_collector_host_map = {}
        super().__init__(**kwargs)

    def schedd_collector_host(self, schedd):
        # Query Schedd ad in Collector for its CollectorHost,
        # unless result previously cached
        if schedd not in self.schedd_collector_host_map:

            collector = htcondor.Collector(self.collector_host)
            ads = collector.query(
                htcondor.AdTypes.Schedd,
                constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                projection=["CollectorHost"],
            )
            ads = list(ads)
            if len(ads) == 0:
                logging.warning(f'Could not find Schedd ClassAd for Machine == "{schedd}"')
                logging.warning(f"Assuming jobs from {schedd} are not in OS pool")
                self.schedd_collector_host_map[schedd] = "UNKNOWN"
                return "UNKNOWN"
            if len(ads) > 1:
                logging.warning(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

            # Cache the CollectorHost in the map
            if "CollectorHost" in ads[0]:
                self.schedd_collector_host_map[schedd] = ads[0]["CollectorHost"].split(':')[0]
            else:
                logging.warning(f"CollectorHost not found in Schedd ClassAd for {schedd}")
                self.schedd_collector_host_map[schedd] = "UNKNOWN"

        return self.schedd_collector_host_map[schedd]

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        o = data["Schedds"][schedd]

        # Filter out jobs that did not run in the OS pool
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Get list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Do filtering for scheduler and local universe jobs
        univ = i.get("JobUniverse", 5)
        o["_NumSchedulerUnivJobs"].append(univ == 7)
        o["_NumLocalUnivJobs"].append(univ == 12)
        o["_NoShadow"].append(univ in [7, 12])

        # Compute badput fields
        if (
                univ not in [7, 12] and
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o = data["Users"][user]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["ScheddName", "ProjectName"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Do filtering for scheduler and local universe jobs
        univ = i.get("JobUniverse", 5)
        o["_NumSchedulerUnivJobs"].append(univ == 7)
        o["_NumLocalUnivJobs"].append(univ == 12)
        o["_NoShadow"].append(univ in [7, 12])

        # Compute badput fields
        if (
                univ not in [7, 12] and
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            # Use UNKNOWN for missing or blank ProjectName and ScheddName
            if attr in ["ScheddName", "ProjectName"]:
                o[attr].append(i.get(attr, "UNKNOWN") or "UNKNOWN")
            else:
                o[attr].append(i.get(attr, None))

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"
        o = data["Projects"][project]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Do filtering for scheduler and local universe jobs
        univ = i.get("JobUniverse", 5)
        o["_NumSchedulerUnivJobs"].append(univ == 7)
        o["_NumLocalUnivJobs"].append(univ == 12)
        o["_NoShadow"].append(univ in [7, 12])

        # Compute badput fields
        if (
                univ not in [7, 12] and
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))


    def site_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that were removed
        if i.get("JobStatus", 4) == 3:
            return

        # Filter out scheduler and local universe jobs
        if i.get("JobUniverse") in [7, 12]:
            return

        # Get output dict for this site
        site = i.get("MachineAttrGLIDEIN_ResourceName0", "UNKNOWN") or "UNKNOWN"
        o = data["Site"][site]

        # Filter out jobs that did not run in the OS pool
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        if i.get("LastRemotePool", self.schedd_collector_host(schedd)) != self.collector_host:
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None and i.get("JobUniverse")!=12:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            self.user_filter,
            self.project_filter,
            self.site_filter,
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
        if agg == "Site":
            columns[5] = "Num Users"
            rm_columns = [30,50,70,80,85,90,95,180,181,182,190,191,192,300,305,310,320,325,330,340,350,355,370,380]
            [columns.pop(key) for key in rm_columns]
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        if agg == "Site":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All CPU Hours")] = "Final Exec Att CPU Hours"
            rows[0] = tuple(columns_sorted)
        return rows


    def compute_site_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        for (goodput_time, cpus) in zip(
                data["CommittedTime"],
                data["RequestCpus"]):
            if None in [goodput_time, cpus]:
                goodput_cpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)

        # Short jobs are jobs that ran for < 1 minute
        is_short_job = []
        for (goodput_time, record_date, start_date) in zip(
                data["CommittedTime"],
                data["RecordTime"],
                data["JobCurrentStartDate"]):
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
        for (is_short, goodput_time) in zip(
                is_short_job,
                data["CommittedTime"]):
            if (is_short == False):
                long_times_sorted.append(goodput_time)
        long_times_sorted = self.clean(long_times_sorted)
        long_times_sorted.sort()

        # Activation metrics added in 9.4.1
        # Added to the OSG Connect access points at 1640100600
        activation_durations = []
        setup_durations = []
        act_cutoff_date = 1_640_100_600  # 2021-12-21 09:30:00
        for (start_date, current_start_date, activation_duration, setup_duration) in zip(
                data["JobStartDate"],
                data["JobCurrentStartDate"],
                data["ActivationDuration"],
                data["ActivationSetupDuration"]):
            start_date = current_start_date or start_date
            if None in [start_date, activation_duration, setup_duration]:
                continue
            if ((start_date > act_cutoff_date) and
                (activation_duration < (act_cutoff_date - 24*3600) and
                (setup_duration < (act_cutoff_date - 24*3600)))):
                activation_durations.append(activation_duration)
                setup_durations.append(setup_duration)

        # Compute columns
        row["All CPU Hours"]    = sum(self.clean(goodput_cpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Num Users"]        = len(set(data["User"]))
        row["Num S'ty Jobs"]    = len(self.clean(data["SingularityImage"]))

        if row["Num Uniq Job Ids"] > 0:
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["% Short Jobs"] = 0
            row["% Jobs using S'ty"] = 0

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

        # Compute mode for Project and Schedd columns in the Users table
        row["Num Users"] = len(set(data["User"]))

        return row

    def compute_custom_columns(self, data, agg, agg_name):

        if agg == "Site":
            row = self.compute_site_custom_columns(data, agg, agg_name)
            return row

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        badput_cpu_time = []
        total_cpu_time = []
        for (goodput_time, badput_time, total_time, cpus) in zip(
                data["CommittedTime"],
                data["_BadWallClockTime"],
                data["RemoteWallClockTime"],
                data["RequestCpus"]):
            if None in [goodput_time, cpus]:
                goodput_cpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)
            badput_cpu_time.append(badput_time * cpus)
            if None in [total_time, cpus]:
                total_cpu_time.append(None)
            else:
                total_cpu_time.append(total_time * cpus)

        # Don't count starts and shadows for jobs that don't/shouldn't have shadows
        num_exec_attempts = []
        num_shadow_starts = []
        for (job_starts, shadow_starts, no_shadow) in zip(
                data["NumJobStarts"],
                data["NumShadowStarts"],
                data["_NoShadow"]):
            if no_shadow:
                num_exec_attempts.append(None)
                num_shadow_starts.append(None)
            else:
                num_exec_attempts.append(job_starts)
                num_shadow_starts.append(shadow_starts)

        # Short jobs are jobs that ran for < 1 minute
        is_short_job = []
        for (goodput_time, record_date, start_date) in zip(
                data["CommittedTime"],
                data["RecordTime"],
                data["JobCurrentStartDate"]):
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
        for (is_short, goodput_time, no_shadow, job_status) in zip(
                is_short_job,
                data["CommittedTime"],
                data["_NoShadow"],
                data["JobStatus"]):
            if (is_short == False) and (no_shadow == False) and (job_status != 3):
                long_times_sorted.append(goodput_time)
        long_times_sorted = self.clean(long_times_sorted)
        long_times_sorted.sort()

        # Only do file counts computations
        # on jobs that have either file count attrs
        # Transfer[Input|Output]FilesCount or
        # Transfer[Input|Output]FilesTotalCount
        input_files_total_count = []
        output_files_total_count = []
        num_exec_attempts_with_file_counts = []
        for (job_starts, i_filesA, i_filesB, o_filesA, o_filesB) in zip(
                data["NumJobStarts"],
                data["TransferInputFilesCount"],
                data["TransferInputFilesTotalCount"],
                data["TransferOutputFilesCount"],
                data["TransferOutputFilesTotalCount"]):
            i_files = i_filesA or i_filesB
            o_files = o_filesA or o_filesB
            input_files_total_count.append(i_files)
            output_files_total_count.append(o_files)
            if (i_files is not None) and (o_files is not None):
                num_exec_attempts_with_file_counts.append(job_starts)
            else:
                num_exec_attempts_with_file_counts.append(None)

        # Activation metrics added in 9.4.1
        # Added to the OSG Connect access points at 1640100600
        activation_durations = []
        setup_durations = []
        act_cutoff_date = 1_640_100_600  # 2021-12-21 09:30:00
        for (start_date, current_start_date, activation_duration, setup_duration) in zip(
                data["JobStartDate"],
                data["JobCurrentStartDate"],
                data["ActivationDuration"],
                data["ActivationSetupDuration"]):
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
        row["Num DAG Node Jobs"] = sum(data['_NumDAGNodes'])
        row["Num Rm'd Jobs"]    = sum([status == 3 for status in data["JobStatus"]])
        row["Num Job Holds"]    = sum(self.clean(data["NumHolds"]))
        row["Num Jobs w/1+ Holds"] = sum([holds > 0 for holds in self.clean(data["NumHolds"])])
        row["Num Jobs w/>1 Exec Att"] = sum([starts > 1 for starts in self.clean(data["NumJobStarts"])])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Num Exec Atts"]    = sum(self.clean(num_exec_attempts))
        row["Num Shadw Starts"] = sum(self.clean(num_shadow_starts))
        row["Num Local Univ Jobs"] = sum(data["_NumLocalUnivJobs"])
        row["Num Sched Univ Jobs"] = sum(data["_NumSchedulerUnivJobs"])
        row["Num S'ty Jobs"]       = len(self.clean(data["SingularityImage"]))

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
            row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = 0
            row["Holds / Job Id"] = 0
            row["% Rm'd Jobs"] = 0
            row["% Short Jobs"] = 0
            row["% Jobs w/>1 Exec Att"] = 0
            row["% Jobs w/1+ Holds"] = 0
            row["% Jobs using S'ty"] = 0
        if row["Num Shadw Starts"] > 0:
            row["Exec Atts / Shadw Start"] = row["Num Exec Atts"] / row["Num Shadw Starts"]
        else:
            row["Exec Atts / Shadw Start"] = 0
        if sum(data["_NumBadJobStarts"]) > 0:
            row["CPU Hours / Bad Exec Att"] = (sum(badput_cpu_time) / 3600) / sum(data["_NumBadJobStarts"])
        else:
            row["CPU Hours / Bad Exec Att"] = 0

        input_mb = 0
        output_mb = 0
        if sum(self.clean(num_exec_attempts)) > 0:
            exec_att     = sum(self.clean(num_exec_attempts))
            input_mb     = sum(self.clean(data["BytesRecvd"])) / 1e6
            output_mb    = sum(self.clean(data["BytesSent"]))  / 1e6
            row["Input MB Xferd / Exec Att"] = input_mb / exec_att
            row["Output MB Xferd / Exec Att"] = output_mb / exec_att
        else:
            row["Input MB Xferd / Exec Att"] = 0
            row["Output MB Xferd / Exec Att"] = 0
        if sum(self.clean(num_exec_attempts_with_file_counts)) > 0:
            exec_att     = sum(self.clean(num_exec_attempts_with_file_counts))
            input_files  = sum(self.clean(input_files_total_count))
            output_files = sum(self.clean(output_files_total_count))
            row["Input Files Xferd / Exec Att"] = input_files / exec_att
            row["Output Files Xferd / Exec Att"] = output_files / exec_att
            if input_files > 0:
                row["Input MB / File"] = input_mb / input_files
            else:
                row["Input MB / File"] = 0
            if output_files > 0:
                row["Output MB / File"] = output_mb / output_files
            else:
                row["Output MB / File"] = 0
        else:
            row["Input Files Xferd / Exec Att"] = -999
            row["Input MB / File"] = -999
            row["Output Files Xferd / Exec Att"] = -999
            row["Output MB / File"] = -999

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

        # Compute mode for Project and Schedd columns in the Users table
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

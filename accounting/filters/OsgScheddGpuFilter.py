
import re
import htcondor
import pickle
import statistics as stats
from datetime import date
from pathlib import Path
from .BaseFilter import BaseFilter
from accounting.functions import get_topology_resource_data, get_institution_database


DEFAULT_COLUMNS = {
    10: "Num Uniq Job Ids",
    20: "All CPU Hours",
    25: "All GPU Hours",
    30: "% Good CPU Hours",
    35: "% Good GPU Hours",

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
    303: "Good GPU Hours",
    305: "CPU Hours / Bad Exec Att",
    307: "GPU Hours / Bad Exec Att",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    330: "Num Rm'd Jobs",
    340: "Num DAG Node Jobs",
    350: "Num Jobs w/>1 Exec Att",
    360: "Num Short Jobs",
    390: "Num Ckpt Able Jobs",

    500: "Max Rqst Mem MB",
    510: "Med Used Mem MB",
    520: "Max Used Mem MB",
    525: "Max Rqst Disk GB",
    527: "Max Used Disk GB",
    530: "Max Rqst Cpus",
    540: "Max Rqst Gpus",
}


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "CommittedTime",
    "RequestCpus",
    "RequestGpus",
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
]


INSTITUTION_DB = get_institution_database()
RESOURCE_DATA = get_topology_resource_data()


class OsgScheddGpuFilter(BaseFilter):
    name = "OSPool GPU schedd job history"

    def __init__(self, **kwargs):
        self.collector_hosts = {"cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"}
        self.schedd_collector_host_map_pickle = Path("ospool-host-map.pkl")
        self.schedd_collector_host_map = {}
        if self.schedd_collector_host_map_pickle.exists():
            try:
                self.schedd_collector_host_map = pickle.load(open(self.schedd_collector_host_map_pickle, "rb"))
            except IOError:
                pass
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
                            {"range": {
                                "RequestGpus": {
                                    "gt": 0
                                }
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

    def schedd_collector_host(self, schedd):
        # Query Schedd ad in Collector for its CollectorHost,
        # unless result previously cached
        if schedd not in self.schedd_collector_host_map:
            self.schedd_collector_host_map[schedd] = set()

            collectors_queried = set()
            for collector_host in self.collector_hosts:
                if collector_host in {"flock.opensciencegrid.org"}:
                    continue
                collector = htcondor.Collector(collector_host)
                try:
                    ads = collector.query(
                        htcondor.AdTypes.Schedd,
                        constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                        projection=["CollectorHost"],
                    )
                except htcondor.HTCondorIOError:
                    continue
                collectors_queried.add(collector_host)
                if len(ads) == 0:
                    continue
                if len(ads) > 1:
                    self.logger.warning(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

                # Cache the CollectorHost in the map
                if "CollectorHost" in ads[0]:
                    schedd_collector_hosts = set()
                    for schedd_collector_host in re.split(r'[, ]+', ads[0]["CollectorHost"]):
                        schedd_collector_host = schedd_collector_host.strip().split(":")[0]
                        if schedd_collector_host:
                            schedd_collector_hosts.add(schedd_collector_host)
                    if schedd_collector_hosts:
                        self.schedd_collector_host_map[schedd] = schedd_collector_hosts
                        break
            else:
                self.logger.warning(f"Did not find Machine == {schedd} in collectors {', '.join(collectors_queried)}")

        return self.schedd_collector_host_map[schedd]

    def is_ospool_job(self, ad):
        remote_pool = set()
        if "LastRemotePool" in ad and ad["LastRemotePool"]:
            remote_pool.add(ad["LastRemotePool"])
        else:
            schedd = ad.get("ScheddName", "UNKNOWN") or "UNKNOWN"
            if schedd != "UNKNOWN":
                remote_pool = self.schedd_collector_host(schedd)
        return bool(remote_pool & self.collector_hosts)

    def schedd_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        o = data["Schedds"][schedd]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

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
        if not self.is_ospool_job(i):
            return

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
            if attr in {"ScheddName", "ProjectName"}:
                o[attr].append(i.get(attr, i.get(attr.lower(), "UNKNOWN")) or "UNKNOWN")
            else:
                o[attr].append(i.get(attr, None))

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", i.get("projectname", "UNKNOWN")) or "UNKNOWN"
        o = data["Projects"][project]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Get list of sites and institutions this user has run at
        resource = i.get("MachineAttrGLIDEIN_ResourceName0", i.get("MATCH_EXP_JOBGLIDEIN_ResourceName"))
        if (resource is None) or (not resource):
            institution = "UNKNOWN"
        elif "MachineAttrOSG_INSTITUTION_ID0" in i:
            osg_id_short = (i.get("MachineAttrOSG_INSTITUTION_ID0") or "").split("_")[-1]
            institution = INSTITUTION_DB.get(osg_id_short, {}).get("name", "UNKNOWN")
        else:
            institution = RESOURCE_DATA.get(resource.lower(), {}).get("institution", "UNKNOWN")
        o["_Institutions"].append(institution)
        o["_Sites"].append(resource)

        # Count number of DAGNode Jobs
        if i.get("DAGNodeName") is not None:
            o["_NumDAGNodes"].append(1)
        else:
            o["_NumDAGNodes"].append(0)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Count number of checkpointable jobs
        if  (
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


    def institution_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

        # Filter out jobs that were removed
        if i.get("JobStatus", 4) == 3:
            return

        # Get output dict for this institution
        resource = i.get("MachineAttrGLIDEIN_ResourceName0", i.get("MATCH_EXP_JOBGLIDEIN_ResourceName"))
        if (resource is None) or (not resource):
            institution = "Unknown (resource name missing)"
        elif "MachineAttrOSG_INSTITUTION_ID0" in i:
            osg_id_short = (i.get("MachineAttrOSG_INSTITUTION_ID0") or "").split("_")[-1]
            institution = INSTITUTION_DB.get(osg_id_short, {}).get("name", f"Unmapped PRP resource: {osg_id_short}")
        else:
            institution = RESOURCE_DATA.get(resource.lower(), {}).get("institution", f"Unmapped resource: {resource}")
        o = data["Institution"][institution]
        o["_Sites"].append(resource)

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

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.schedd_filter,
            self.user_filter,
            self.project_filter,
            self.institution_filter,
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
            columns[6] = "Num Site Instns"
            columns[7] = "Num Sites"
        if agg == "Institution":
            columns[4] = "Num Sites"
            columns[5] = "Num Users"
            rm_columns = [30,35,45,50,51,52,53,54,55,56,57,70,80,180,181,190,191,300,303,305,307,310,320,330,340,350,390]
            [columns.pop(key) for key in rm_columns if key in columns]
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        if agg == "Institution":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All CPU Hours")] = "Final Exec Att CPU Hours"
            rows[0] = tuple(columns_sorted)
        return rows


    def compute_institution_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        goodput_gpu_time = []
        for (goodput_time, cpus, gpus) in zip(
                data["CommittedTime"],
                data["RequestCpus"],
                data["RequestGpus"]):
            if cpus is not None:
                cpus = max(cpus, 1)
            if None in [goodput_time, cpus, gpus]:
                goodput_cpu_time.append(None)
                goodput_gpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)
                goodput_gpu_time.append(goodput_time * gpus)

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
        row["All GPU Hours"]    = sum(self.clean(goodput_gpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Num Jobs Over Rqst Disk"] = sum([(usage or 0) > (request or 1)
            for (usage, request) in zip(data["DiskUsage"], data["RequestDisk"])])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Disk GB"] = max(self.clean(data["RequestDisk"], allow_empty_list=False)) / (1000*1000)
        row["Max Used Disk GB"] = max(self.clean(data["DiskUsage"], allow_empty_list=False)) / (1000*1000)
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Max Rqst Gpus"]    = max(self.clean(data["RequestGpus"], allow_empty_list=False))
        row["Num Users"]        = len(set(data["User"]))
        row["Num S'ty Jobs"]    = len(self.clean(data["SingularityImage"]))

        if row["Num Uniq Job Ids"] > 0:
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
            row["% Jobs Over Rqst Disk"] = 100 * row["Num Jobs Over Rqst Disk"] / row["Num Uniq Job Ids"]
            row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["% Short Jobs"] = 0
            row["% Jobs Over Rqst Disk"] = 0
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

        # Compute number of unique Sites
        row["Num Sites"] = len(set(data["_Sites"]))

        return row

    def compute_custom_columns(self, data, agg, agg_name):

        if agg == "Institution":
            row = self.compute_institution_custom_columns(data, agg, agg_name)
            return row

        # Output dictionary
        row = {}

        # Compute goodput and total CPU and GPU hours columns
        goodput_cpu_time = []
        badput_cpu_time = []
        total_cpu_time = []
        goodput_gpu_time = []
        badput_gpu_time = []
        total_gpu_time = []
        for (goodput_time, badput_time, total_time, cpus, gpus) in zip(
                data["CommittedTime"],
                data["_BadWallClockTime"],
                data["RemoteWallClockTime"],
                data["RequestCpus"],
                data["RequestGpus"]):
            if cpus is not None:
                cpus = max(cpus, 1)  # assume at least 1 CPU even if 0 CPUs were stored in Elasticsearch
            if None in [goodput_time, cpus, gpus]:
                goodput_cpu_time.append(None)
                goodput_gpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)
                goodput_gpu_time.append(goodput_time * gpus)
            if None in [badput_time, cpus, gpus]:
                badput_cpu_time.append(None)
                badput_gpu_time.append(None)
            else:
                badput_cpu_time.append(badput_time * cpus)
                badput_gpu_time.append(badput_time * gpus)
            if None in [total_time, cpus, gpus]:
                total_cpu_time.append(None)
                total_gpu_time.append(None)
            else:
                total_cpu_time.append(total_time * cpus)
                total_gpu_time.append(total_time * gpus)

        # Don't count starts and shadows for jobs that don't/shouldn't have shadows
        num_exec_attempts = []
        num_shadow_starts = []
        for (job_starts, shadow_starts) in zip(
                data["NumJobStarts"],
                data["NumShadowStarts"]):
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
        for (is_short, goodput_time, job_status) in zip(
                is_short_job,
                data["CommittedTime"],
                data["JobStatus"]):
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
                data["TransferInputStats"],
                data["BytesRecvd"],
                data["TransferOutputStats"],
                data["BytesSent"],
            ):

            input_files_count = 0
            input_files_bytes = 0
            if input_stats is None:
                input_files_total_count.append(None)
                input_files_total_job_starts.append(None)
            else:
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
        row["All GPU Hours"]    = sum(self.clean(total_gpu_time)) / 3600
        row["Good CPU Hours"]   = sum(self.clean(goodput_cpu_time)) / 3600
        row["Good GPU Hours"]   = sum(self.clean(goodput_gpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Num DAG Node Jobs"] = sum(data['_NumDAGNodes'])
        row["Num Rm'd Jobs"]    = sum([status == 3 for status in data["JobStatus"]])
        row["Num Job Holds"]    = sum(self.clean(data["NumHolds"]))
        row["Num Jobs w/1+ Holds"] = sum([holds > 0 for holds in self.clean(data["NumHolds"])])
        row["Num Jobs Over Rqst Disk"] = sum([(usage or 0) > (request or 1)
            for (usage, request) in zip(data["DiskUsage"], data["RequestDisk"])])
        row["Num Jobs w/>1 Exec Att"] = sum([starts > 1 for starts in self.clean(data["NumJobStarts"])])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Disk GB"] = max(self.clean(data["RequestDisk"], allow_empty_list=False)) / (1000*1000)
        row["Max Used Disk GB"] = max(self.clean(data["DiskUsage"], allow_empty_list=False)) / (1000*1000)
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Max Rqst Gpus"]    = max(self.clean(data["RequestGpus"], allow_empty_list=False))
        row["Num Exec Atts"]    = sum(self.clean(num_exec_attempts))
        row["Num Shadw Starts"] = sum(self.clean(num_shadow_starts))
        row["Num Ckpt Able Jobs"]  = sum(data["_NumCkptJobs"])
        row["Num S'ty Jobs"]       = len(self.clean(data["SingularityImage"]))

        # Compute derivative columns
        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0
        if row["All GPU Hours"] > 0:
            row["% Good GPU Hours"] = 100 * row["Good GPU Hours"] / row["All GPU Hours"]
        else:
            row["% Good GPU Hours"] = 0
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
        if sum(data["_NumBadJobStarts"]) > 0:
            row["CPU Hours / Bad Exec Att"] = (sum(self.clean(badput_cpu_time)) / 3600) / sum(data["_NumBadJobStarts"])
            row["GPU Hours / Bad Exec Att"] = (sum(self.clean(badput_gpu_time)) / 3600) / sum(data["_NumBadJobStarts"])
        else:
            row["CPU Hours / Bad Exec Att"] = 0
            row["GPU Hours / Bad Exec Att"] = 0

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
            row["Num Site Instns"] = len(set(data["_Institutions"]))
            row["Num Sites"] = len(set(data["_Sites"]))

        return row

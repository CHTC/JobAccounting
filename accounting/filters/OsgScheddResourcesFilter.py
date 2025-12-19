import re
import sys
import pickle
import statistics as stats
from datetime import date
from pathlib import Path
from .BaseFilter import BaseFilter
# from accounting.functions import get_topology_project_data, get_topology_resource_data, get_institution_database

try:
    import htcondor2 as htcondor
except ImportError:
    print("Could not import from htcondor2, falling back to htcondor", file=sys.stderr)
    try:
        import htcondor
    except ImportError:
        print("Could not import htcondor", file=sys.stderr)
        raise

DEFAULT_COLUMNS = {
    25: "Total Mem GBh Util%",
    30: "Total Unuse Mem GBh",
    35: "Total Allo Mem GBh",

    100: "&nbsp;",

    110: "Min Util% Mem",
    120: "25% Util% Mem",
    130: "Med Util% Mem",
    140: "75% Util% Mem",
    145: "95% Util% Mem",
    150: "Max Util% Mem",
    160: "Mean Util% Mem",
    170: "Stdv Util% Mem",

    200: "&nbsp;",

    210: "Min Allo Mem GBh",
    220: "25% Allo Mem GBh",
    230: "Med Allo Mem GBh",
    240: "75% Allo Mem GBh",
    245: "95% Allo Mem GBh",
    250: "Max Allo Mem GBh",
    260: "Mean Allo Mem GBh",
    270: "Stdv Allo Mem GBh",

    300: "&nbsp;",

    310: "Min Unuse Mem GBh",
    320: "25% Unuse Mem GBh",
    330: "Med Unuse Mem GBh",
    340: "75% Unuse Mem GBh",
    345: "95% Unuse Mem GBh",
    350: "Max Unuse Mem GBh",
    360: "Mean Unuse Mem GBh",
    370: "Stdv Unuse Mem GBh",

    400: "&nbsp;",

    410: "Min Allo Mem",
    420: "25% Allo Mem",
    430: "Med Allo Mem",
    440: "75% Allo Mem",
    445: "95% Allo Mem",
    450: "Max Allo Mem",
    460: "Mean Allo Mem",
    470: "Stdv Allo Mem",

    500: "&nbsp;",

    510: "Min Use Mem",
    520: "25% Use Mem",
    530: "Med Use Mem",
    540: "75% Use Mem",
    545: "95% Use Mem",
    550: "Max Use Mem",
    560: "Mean Use Mem",
    570: "Stdv Use Mem",

    1000: "&nbsp;",

    1105: "% Short Jobs",
    1110: "Min Hrs",
    1120: "25% Hrs",
    1130: "Med Hrs",
    1140: "75% Hrs",
    1145: "95% Hrs",
    1150: "Max Hrs",
    1160: "Mean Hrs",
    1170: "Stdv Hrs",
}


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "CommittedTime",
    "RequestMemory",
    "RecordTime",
    "JobStartDate",
    "JobCurrentStartDate",
    "MemoryUsage",
    "JobStatus",
    "EnteredCurrentStatus",
]


# INSTITUTION_DB = get_institution_database()
# RESOURCE_DATA = get_topology_resource_data()


class OsgScheddResourcesFilter(BaseFilter):
    name = "OSPool resource usage"

    def __init__(self, **kwargs):
        self.collector_hosts = {"cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"}
        self.schedd_collector_host_map_pickle = Path("ospool-host-map.pkl")
        self.schedd_collector_host_map = {}
        if self.schedd_collector_host_map_pickle.exists():
            try:
                self.schedd_collector_host_map = pickle.load(open(self.schedd_collector_host_map_pickle, "rb"))
            except IOError:
                pass
        self.schedd_collector_host_map_checked = set(self.schedd_collector_host_map)
        # Recheck and update the collector map every Monday during the daily report
        if kwargs.get("report_period") == "daily" and date.today().weekday() == 0:
            self.schedd_collector_host_map_checked = set()
        super().__init__(**kwargs)
        self.sort_col = "Total Unuse Mem GBh"
        # self.topology_project_map = get_topology_project_data()


    def schedd_collector_host(self, schedd):
        # Query Schedd ad in Collector for its CollectorHost,
        # unless result previously cached or it's Monday
        if schedd not in self.schedd_collector_host_map_checked:
            self.logger.debug(f"Schedd {schedd} not found/needs updating in cached collector host map, querying collector")
            self.schedd_collector_host_map[schedd] = set()
            self.schedd_collector_host_map_checked.add(schedd)
            new_hosts = False

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
                ads = list(ads)
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
                            new_hosts = True
                    if schedd_collector_hosts:
                        self.schedd_collector_host_map[schedd] = schedd_collector_hosts
                        break
            else:
                self.logger.warning(f"Did not find Machine == {schedd} in collectors {', '.join(collectors_queried)}")

            # Update the pickle
            if new_hosts and len(schedd_collector_hosts) > 0:
                self.logger.debug(f"Updating collector host pickle for {schedd} with {schedd_collector_hosts}")
                old_schedd_collector_host_map = {}
                if self.schedd_collector_host_map_pickle.exists():
                    try:
                        old_schedd_collector_host_map = pickle.load(open(self.schedd_collector_host_map_pickle, "rb"))
                        old_schedd_collector_host_map[schedd] = schedd_collector_hosts
                    except IOError:
                        pass
                old_schedd_collector_host_map[schedd] = schedd_collector_hosts
                with open(self.schedd_collector_host_map_pickle, "wb") as f:
                    pickle.dump(old_schedd_collector_host_map, f)

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

    def get_query(self, index, start_ts, end_ts, scroll=None, size=500):
        # Returns dict matching Elasticsearch.search() kwargs
        # (Dict has same structure as the REST API query language)

        # Set the scroll time based on how long the reporting period is.
        # Using 30s + 5s * sqrt(days-1)
        if scroll is None:
            scroll_seconds = 60 + int(5 * (((end_ts - start_ts) / (3600 * 24)) - 1)**0.5)
            scroll = f"{int(scroll_seconds)}s"
            self.logger.debug(f"No explicit scroll time set, using {scroll}.")

        query = {
            "index": index,
            "scroll": scroll,
            "size": size,
            "sort": ["_doc"],
            "track_scores": False,
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
                                "MemoryUsage": {
                                    "gt": 0,
                                }
                            }},
                            {"range": {
                                "RemoteWallClockTime": {
                                    "gt": 0,
                                }
                            }}
                        ],
                        "must_not": [
                            {"terms": {
                                "JobUniverse": [7, 12]
                            }},
                        ],
                        "should": [
                            {"exists": {
                                "field": "MachineAttrGLIDEIN_ResourceName0",
                            }},
                            {"exists": {
                                "field": "MATCH_EXP_JOBGLIDEIN_ResourceName",
                            }}
                        ]
                    }
                }
            }
        }
        return query


    # def schedd_filter(self, data, doc):

    #     # Get input dict
    #     i = doc["_source"]

    #     # Get output dict for this schedd
    #     schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
    #     o = data["Schedds"][schedd]

    #     # Filter out jobs that did not run in the OS pool
    #     if not self.is_ospool_job(i):
    #         return

    #     # Get list of attrs
    #     filter_attrs = DEFAULT_FILTER_ATTRS.copy()

    #     # Count number of DAGNode Jobs
    #     if i.get("DAGNodeName") is not None:
    #         o["_NumDAGNodes"].append(1)
    #     else:
    #         o["_NumDAGNodes"].append(0)

    #     # Count number of history ads (i.e. number of unique job ids)
    #     o["_NumJobs"].append(1)

    #     # Count number of checkpointable jobs
    #     if  (
    #             i.get("SuccessCheckpointExitBySignal", False) or
    #             i.get("SuccessCheckpointExitCode") is not None
    #         ):
    #         o["_NumCkptJobs"].append(1)
    #     else:
    #         o["_NumCkptJobs"].append(0)

    #     # Compute badput fields
    #     if (
    #             i.get("NumJobStarts", 0) > 1 and
    #             i.get("RemoteWallClockTime", 0) > 0 and
    #             i.get("RemoteWallClockTime") != i.get("CommittedTime")
    #         ):
    #         o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
    #         o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
    #     else:
    #         o["_BadWallClockTime"].append(0)
    #         o["_NumBadJobStarts"].append(0)

    #     # Compute job units
    #     if i.get("RemoteWallClockTime", 0) > 0:
    #         o["NumJobUnits"].append(get_job_units(
    #             cpus=i.get("RequestCpus", 1),
    #             memory_gb=i.get("RequestMemory", 1024)/1024,
    #             disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
    #         ))
    #     else:
    #         o["NumJobUnits"].append(None)

    #     # Add attr values to the output dict, use None if missing
    #     for attr in filter_attrs:
    #         o[attr].append(i.get(attr, None))

    # def user_filter(self, data, doc):

    #     # Get input dict
    #     i = doc["_source"]

    #     # Get output dict for this user
    #     user = i.get("User", "UNKNOWN") or "UNKNOWN"
    #     o = data["Users"][user]

    #     # Filter out jobs that did not run in the OS pool
    #     if not self.is_ospool_job(i):
    #         return

    #     # Add custom attrs to the list of attrs
    #     filter_attrs = DEFAULT_FILTER_ATTRS.copy()
    #     filter_attrs = filter_attrs + ["ScheddName", "ProjectName"]

    #     # Count number of DAGNode Jobs
    #     if i.get("DAGNodeName") is not None:
    #         o["_NumDAGNodes"].append(1)
    #     else:
    #         o["_NumDAGNodes"].append(0)

    #     # Count number of history ads (i.e. number of unique job ids)
    #     o["_NumJobs"].append(1)

    #     # Count number of checkpointable jobs
    #     if  (
    #             i.get("SuccessCheckpointExitBySignal", False) or
    #             i.get("SuccessCheckpointExitCode") is not None
    #         ):
    #         o["_NumCkptJobs"].append(1)
    #     else:
    #         o["_NumCkptJobs"].append(0)

    #     # Compute badput fields
    #     if (
    #             i.get("NumJobStarts", 0) > 1 and
    #             i.get("RemoteWallClockTime", 0) > 0 and
    #             i.get("RemoteWallClockTime") != i.get("CommittedTime")
    #         ):
    #         o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
    #         o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
    #     else:
    #         o["_BadWallClockTime"].append(0)
    #         o["_NumBadJobStarts"].append(0)

    #     # Compute job units
    #     if i.get("RemoteWallClockTime", 0) > 0:
    #         o["NumJobUnits"].append(get_job_units(
    #             cpus=i.get("RequestCpus", 1),
    #             memory_gb=i.get("RequestMemory", 1024)/1024,
    #             disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
    #         ))
    #     else:
    #         o["NumJobUnits"].append(None)

    #     # Add attr values to the output dict, use None if missing
    #     for attr in filter_attrs:
    #         # Use UNKNOWN for missing or blank ProjectName and ScheddName
    #         if attr in {"ScheddName", "ProjectName"}:
    #             o[attr].append(i.get(attr, i.get(attr.lower(), "UNKNOWN")) or "UNKNOWN")
    #         else:
    #             o[attr].append(i.get(attr, None))

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
        # filter_attrs = filter_attrs + ["User"]

        # Get list of sites and institutions this user has run at
        # resource = i.get("MachineAttrGLIDEIN_ResourceName0", i.get("MATCH_EXP_JOBGLIDEIN_ResourceName"))
        # if (resource is None) or (not resource):
        #     institution = "UNKNOWN"
        # elif "MachineAttrOSG_INSTITUTION_ID0" in i:
        #     osg_id_short = (i.get("MachineAttrOSG_INSTITUTION_ID0") or "").split("_")[-1]
        #     institution = INSTITUTION_DB.get(osg_id_short, {}).get("name", "UNKNOWN")
        # else:
        #     institution = RESOURCE_DATA.get(resource.lower(), {}).get("institution", "UNKNOWN")
        # o["_Institutions"].append(institution)
        # o["_Sites"].append(resource)

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))


    # def institution_filter(self, data, doc):

    #     # Get input dict
    #     i = doc["_source"]

    #     # Filter out jobs that did not run in the OS pool
    #     if not self.is_ospool_job(i):
    #         return

    #     # Filter out jobs that were removed
    #     if i.get("JobStatus", 4) == 3:
    #         return

    #     # Get output dict for this institution
    #     resource = i.get("MachineAttrGLIDEIN_ResourceName0", i.get("MATCH_EXP_JOBGLIDEIN_ResourceName"))
    #     if (resource is None) or (not resource):
    #         institution = "Unknown (resource name missing)"
    #     elif "MachineAttrOSG_INSTITUTION_ID0" in i:
    #         osg_id_short = (i.get("MachineAttrOSG_INSTITUTION_ID0") or "").split("_")[-1]
    #         institution = INSTITUTION_DB.get(osg_id_short, {}).get("name", f"Unmapped PRP resource: {osg_id_short}")
    #     else:
    #         institution = RESOURCE_DATA.get(resource.lower(), {}).get("institution", f"Unmapped resource: {resource}")
    #     o = data["Institution"][institution]
    #     o["_Sites"].append(resource)

    #     # Add custom attrs to the list of attrs
    #     filter_attrs = DEFAULT_FILTER_ATTRS.copy()
    #     filter_attrs = filter_attrs + ["User"]

    #     # Count number of DAGNode Jobs
    #     if i.get("DAGNodeName") is not None:
    #         o["_NumDAGNodes"].append(1)
    #     else:
    #         o["_NumDAGNodes"].append(0)

    #     # Count number of history ads (i.e. number of unique job ids)
    #     o["_NumJobs"].append(1)

    #     # Compute job units
    #     if i.get("RemoteWallClockTime", 0) > 0:
    #         o["NumJobUnits"].append(get_job_units(
    #             cpus=i.get("RequestCpus", 1),
    #             memory_gb=i.get("RequestMemory", 1024)/1024,
    #             disk_gb=i.get("RequestDisk", 1024**2)/1024**2,
    #         ))
    #     else:
    #         o["NumJobUnits"].append(None)

    #     # Add attr values to the output dict, use None if missing
    #     for attr in filter_attrs:
    #         o[attr].append(i.get(attr, None))

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            # self.schedd_filter,
            # self.user_filter,
            self.project_filter,
            # self.institution_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Project and Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        # if agg == "Users":
        #     columns[4] = "Most Used Project"
        #     columns[175] = "Most Used Schedd"
        # if agg == "Projects":
        #     columns[4] = "PI Institution"
        #     columns[10] = "Num Users"
        #     columns[11] = "Num Site Instns"
        #     columns[12] = "Num Sites"
        # if agg == "Institution":
        #     columns[10] = "Num Sites"
        #     columns[11] = "Num Users"
        #     # rm_columns = [5,6,30,45,50,51,52,53,54,55,56,57,70,80,180,181,182,190,191,192,300,305,325,330,340,350,355,390,600,610,620,630,640]
        #     [columns.pop(key) for key in rm_columns if key in columns]
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        if agg == "Institution":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All CPU Hours")] = "Final Exec Att CPU Hours"
            rows[0] = tuple(columns_sorted)
        return rows


    # def compute_institution_custom_columns(self, data, agg, agg_name):

    #     # Output dictionary
    #     row = {}

    #     # Compute goodput and total CPU hours columns
    #     goodput_cpu_time = []
    #     for (goodput_time, cpus) in zip(
    #             data["CommittedTime"],
    #             data["RequestCpus"]):
    #         if cpus is not None:
    #             cpus = max(cpus, 1)
    #         if None in [goodput_time, cpus]:
    #             goodput_cpu_time.append(None)
    #         else:
    #             goodput_cpu_time.append(goodput_time * cpus)

    #     # Short jobs are jobs that ran for < 1 minute
    #     is_short_job = []
    #     for (goodput_time, record_date, start_date) in zip(
    #             data["CommittedTime"],
    #             data["RecordTime"],
    #             data["JobCurrentStartDate"]):
    #         if (goodput_time is not None) and (goodput_time > 0):
    #             is_short_job.append(goodput_time < 60)
    #         elif None in (record_date, start_date):
    #             is_short_job.append(None)
    #         else:
    #             is_short_job.append((record_date - start_date) < 60)

    #     # "Long" (i.e. "normal") jobs ran >= 1 minute
    #     # We only want to use these when computing percentiles,
    #     # so filter out short jobs and removed jobs,
    #     # and sort them so we can easily grab the percentiles later
    #     long_times_sorted = []
    #     for (is_short, goodput_time) in zip(
    #             is_short_job,
    #             data["CommittedTime"]):
    #         if (is_short == False):
    #             long_times_sorted.append(goodput_time)
    #     long_times_sorted = self.clean(long_times_sorted)
    #     long_times_sorted.sort()

    #     # Activation metrics added in 9.4.1
    #     # Added to the OSG Connect access points at 1640100600
    #     activation_durations = []
    #     setup_durations = []
    #     act_cutoff_date = 1_640_100_600  # 2021-12-21 09:30:00
    #     for (start_date, current_start_date, activation_duration, setup_duration) in zip(
    #             data["JobStartDate"],
    #             data["JobCurrentStartDate"],
    #             data["ActivationDuration"],
    #             data["ActivationSetupDuration"]):
    #         start_date = current_start_date or start_date
    #         if None in [start_date, activation_duration, setup_duration]:
    #             continue
    #         if ((start_date > act_cutoff_date) and
    #             (activation_duration < (act_cutoff_date - 24*3600) and
    #             (setup_duration < (act_cutoff_date - 24*3600)))):
    #             activation_durations.append(activation_duration)
    #             setup_durations.append(setup_duration)

    #     # Compute columns
    #     row["All CPU Hours"]    = sum(self.clean(goodput_cpu_time)) / 3600
    #     row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
    #     row["Num Jobs Over Rqst Disk"] = sum([(usage or 0) > (request or 1)
    #         for (usage, request) in zip(data["DiskUsage"], data["RequestDisk"])])
    #     row["Num Short Jobs"]   = sum(self.clean(is_short_job))
    #     row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
    #     row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
    #     row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
    #     row["Max Rqst Disk GB"] = max(self.clean(data["RequestDisk"], allow_empty_list=False)) / (1000*1000)
    #     row["Max Used Disk GB"] = max(self.clean(data["DiskUsage"], allow_empty_list=False)) / (1000*1000)
    #     row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
    #     row["Num Users"]        = len(set(data["User"]))
    #     row["Num S'ty Jobs"]    = len(self.clean(data["SingularityImage"]))

    #     if row["Num Uniq Job Ids"] > 0:
    #         row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
    #         row["% Jobs Over Rqst Disk"] = 100 * row["Num Jobs Over Rqst Disk"] / row["Num Uniq Job Ids"]
    #         row["% Jobs using S'ty"] = 100 * row["Num S'ty Jobs"] / row["Num Uniq Job Ids"]
    #     else:
    #         row["% Short Jobs"] = 0
    #         row["% Jobs Over Rqst Disk"] = 0
    #         row["% Jobs using S'ty"] = 0

    #     # Compute activation time stats
    #     row["Mean Actv Hrs"] = ""
    #     row["Mean Setup Secs"] = ""
    #     if len(activation_durations) > 0:
    #         row["Mean Actv Hrs"] = (sum(activation_durations) / len(activation_durations)) / 3600
    #     if len(setup_durations) > 0:
    #         row["Mean Setup Secs"] = sum(setup_durations) / len(setup_durations)

    #     # Compute time percentiles and stats
    #     if len(long_times_sorted) > 0:
    #         row["Min Hrs"]  = long_times_sorted[ 0] / 3600
    #         row["25% Hrs"]  = long_times_sorted[  len(long_times_sorted)//4] / 3600
    #         row["Med Hrs"]  = stats.median(long_times_sorted) / 3600
    #         row["75% Hrs"]  = long_times_sorted[3*len(long_times_sorted)//4] / 3600
    #         row["95% Hrs"]  = long_times_sorted[int(0.95*len(long_times_sorted))] / 3600
    #         row["Max Hrs"]  = long_times_sorted[-1] / 3600
    #         row["Mean Hrs"] = stats.mean(long_times_sorted) / 3600
    #     else:
    #         for col in [f"{x} Hrs" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
    #             row[col] = 0

    #     if len(long_times_sorted) > 1:
    #         row["Std Hrs"] = stats.stdev(long_times_sorted) / 3600
    #     else:
    #         # There is no variance if there is only one value
    #         row["Std Hrs"] = 0

    #     # Compute job unit metrics
    #     row["Med Job Units"] = stats.median(self.clean(data["NumJobUnits"], allow_empty_list=False))
    #     row["Max Job Units"] = max(self.clean(data["NumJobUnits"], allow_empty_list=False))
    #     row["Job Unit Hours"] = sum(
    #         job_units*wallclocktime/3600 for job_units, wallclocktime in zip(
    #             data.get("NumJobUnits", []),
    #             data.get("RemoteWallClockTime", []),
    #         )
    #         if job_units is not None
    #     )

    #     # Compute mode for Project and Schedd columns in the Users table
    #     row["Num Users"] = len(set(data["User"]))

    #     # Compute number of unique Sites
    #     row["Num Sites"] = len(set(data["_Sites"]))

    #     return row

    def compute_custom_columns(self, data, agg, agg_name):

        # if agg == "Institution":
        #     row = self.compute_institution_custom_columns(data, agg, agg_name)
        #     return row

        # Output dictionary
        row = {"&nbsp;": ""}

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

        # Compute columns
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Num Short Jobs"]   = sum(self.clean(is_short_job))

        # # Compute derivative columns
        if row["Num Uniq Job Ids"] > 0:
            row["% Short Jobs"] = 100 * row["Num Short Jobs"] / row["Num Uniq Job Ids"]
        else:
            row["% Short Jobs"] = 0

        # Compute percentiles and stats
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
            row["Stdv Hrs"] = stats.stdev(long_times_sorted) / 3600
        else:
            # There is no variance if there is only one value
            row["Stdv Hrs"] = 0

        memory_requests_sorted = self.clean(data['RequestMemory'])
        memory_requests_sorted.sort()
        if len(memory_requests_sorted) > 0:
            row["Min Allo Mem"]  = memory_requests_sorted[ 0] / 1024
            row["25% Allo Mem"]  = memory_requests_sorted[  len(memory_requests_sorted)//4] / 1024
            row["Med Allo Mem"]  = stats.median(memory_requests_sorted) / 1024
            row["75% Allo Mem"]  = memory_requests_sorted[3*len(memory_requests_sorted)//4] / 1024
            row["95% Allo Mem"]  = memory_requests_sorted[int(0.95*len(memory_requests_sorted))] / 1024
            row["Max Allo Mem"]  = memory_requests_sorted[-1] / 1024
            row["Mean Allo Mem"] = stats.mean(memory_requests_sorted) / 1024
        else:
            for col in [f"{x} Allo Mem" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_requests_sorted) > 1:
            row["Stdv Allo Mem"] = stats.stdev(memory_requests_sorted) / 1024
        else:
            # There is no variance if there is only one value
            row["Stdv Allo Mem"] = 0

        memory_hours_requested_sorted = self.clean([x*y if (None not in (x, y)) else None for x, y in zip(data["RequestMemory"], data["RemoteWallClockTime"])])
        memory_hours_requested_sorted.sort()
        if len(memory_hours_requested_sorted) > 0:
            row["Min Allo Mem GBh"]  = memory_hours_requested_sorted[ 0] / (1024*3600)
            row["25% Allo Mem GBh"]  = memory_hours_requested_sorted[  len(memory_hours_requested_sorted)//4] / (1024*3600)
            row["Med Allo Mem GBh"]  = stats.median(memory_hours_requested_sorted) / (1024*3600)
            row["75% Allo Mem GBh"]  = memory_hours_requested_sorted[3*len(memory_hours_requested_sorted)//4] / (1024*3600)
            row["95% Allo Mem GBh"]  = memory_hours_requested_sorted[int(0.95*len(memory_hours_requested_sorted))] / (1024*3600)
            row["Max Allo Mem GBh"]  = memory_hours_requested_sorted[-1] / (1024*3600)
            row["Mean Allo Mem GBh"] = stats.mean(memory_hours_requested_sorted) / (1024*3600)
        else:
            for col in [f"{x} Allo Mem GBh" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_hours_requested_sorted) > 1:
            row["Stdv Allo Mem GBh"] = stats.stdev(memory_hours_requested_sorted) / (1024*3600)
        else:
            # There is no variance if there is only one value
            row["Stdv Allo Mem GBh"] = 0

        memory_usages_sorted = self.clean(data['MemoryUsage'])
        memory_usages_sorted.sort()
        if len(memory_usages_sorted) > 0:
            row["Min Use Mem"]  = memory_usages_sorted[ 0] / 1024
            row["25% Use Mem"]  = memory_usages_sorted[  len(memory_usages_sorted)//4] / 1024
            row["Med Use Mem"]  = stats.median(memory_usages_sorted) / 1024
            row["75% Use Mem"]  = memory_usages_sorted[3*len(memory_usages_sorted)//4] / 1024
            row["95% Use Mem"]  = memory_usages_sorted[int(0.95*len(memory_usages_sorted))] / 1024
            row["Max Use Mem"]  = memory_usages_sorted[-1] / 1024
            row["Mean Use Mem"] = stats.mean(memory_usages_sorted) / 1024
        else:
            for col in [f"{x} Use Mem" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_usages_sorted) > 1:
            row["Stdv Use Mem"] = stats.stdev(memory_usages_sorted) / 1024
        else:
            # There is no variance if there is only one value
            row["Stdv Use Mem"] = 0

        memory_hours_usages_sorted = self.clean([min(req, use)*t if (None not in (req, use, t)) else None for use, req, t in zip(data["MemoryUsage"], data["RequestMemory"], data["RemoteWallClockTime"])])
        memory_hours_usages_sorted.sort()
        # if len(memory_hours_usages_sorted) > 0:
        #     row["Min Use Mem GBh"]  = memory_hours_usages_sorted[ 0] / (1024*3600)
        #     row["25% Use Mem GBh"]  = memory_hours_usages_sorted[  len(memory_hours_usages_sorted)//4] / (1024*3600)
        #     row["Med Use Mem GBh"]  = stats.median(memory_hours_usages_sorted) / (1024*3600)
        #     row["75% Use Mem GBh"]  = memory_hours_usages_sorted[3*len(memory_hours_usages_sorted)//4] / (1024*3600)
        #     row["95% Use Mem GBh"]  = memory_hours_usages_sorted[int(0.95*len(memory_hours_usages_sorted))] / (1024*3600)
        #     row["Max Use Mem GBh"]  = memory_hours_usages_sorted[-1] / (1024*3600)
        #     row["Mean Use Mem GBh"] = stats.mean(memory_hours_usages_sorted) / (1024*3600)
        # else:
        #     for col in [f"{x} Use Mem GBh" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
        #         row[col] = 0
        # if len(memory_hours_usages_sorted) > 1:
        #     row["Stdv Use Mem GBh"] = stats.stdev(memory_hours_usages_sorted) / (1024*3600)
        # else:
        #     # There is no variance if there is only one value
        #     row["Stdv Use Mem GBh"] = 0

        memory_unusages_sorted = self.clean([max(req - use, 0) if (None not in (req, use)) else None for req, use in zip(data["RequestMemory"], data["MemoryUsage"])])
        memory_unusages_sorted.sort()
        if len(memory_unusages_sorted) > 0:
            row["Min Unuse Mem"]  = memory_unusages_sorted[ 0] / 1024
            row["25% Unuse Mem"]  = memory_unusages_sorted[  len(memory_unusages_sorted)//4] / 1024
            row["Med Unuse Mem"]  = stats.median(memory_unusages_sorted) / 1024
            row["75% Unuse Mem"]  = memory_unusages_sorted[3*len(memory_unusages_sorted)//4] / 1024
            row["95% Unuse Mem"]  = memory_unusages_sorted[int(0.95*len(memory_unusages_sorted))] / 1024
            row["Max Unuse Mem"]  = memory_unusages_sorted[-1] / 1024
            row["Mean Unuse Mem"] = stats.mean(memory_unusages_sorted) / 1024
        else:
            for col in [f"{x} Unuse Mem" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_unusages_sorted) > 1:
            row["Stdv Unuse Mem"] = stats.stdev(memory_unusages_sorted) / 1024
        else:
            # There is no variance if there is only one value
            row["Stdv Unuse Mem"] = 0

        memory_hours_unusages_sorted = self.clean([max(req - use, 0)*t if (None not in (req, use, t)) else None for req, use, t in zip(data["RequestMemory"], data["MemoryUsage"], data["RemoteWallClockTime"])])
        memory_hours_unusages_sorted.sort()
        if len(memory_hours_unusages_sorted) > 0:
            row["Min Unuse Mem GBh"]  = memory_hours_unusages_sorted[ 0] / (1024*3600)
            row["25% Unuse Mem GBh"]  = memory_hours_unusages_sorted[  len(memory_hours_unusages_sorted)//4] / (1024*3600)
            row["Med Unuse Mem GBh"]  = stats.median(memory_hours_unusages_sorted) / (1024*3600)
            row["75% Unuse Mem GBh"]  = memory_hours_unusages_sorted[3*len(memory_hours_unusages_sorted)//4] / (1024*3600)
            row["95% Unuse Mem GBh"]  = memory_hours_unusages_sorted[int(0.95*len(memory_hours_unusages_sorted))] / (1024*3600)
            row["Max Unuse Mem GBh"]  = memory_hours_unusages_sorted[-1] / (1024*3600)
            row["Mean Unuse Mem GBh"] = stats.mean(memory_hours_unusages_sorted) / (1024*3600)
        else:
            for col in [f"{x} Unuse Mem GBh" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_hours_unusages_sorted) > 1:
            row["Stdv Unuse Mem GBh"] = stats.stdev(memory_hours_unusages_sorted) / (1024*3600)
        else:
            # There is no variance if there is only one value
            row["Stdv Unuse Mem GBh"] = 0

        memory_utility_sorted = self.clean([100*x/y if ((None not in (x, y,)) and (y > 0)) else None for x, y in zip(data['MemoryUsage'], data['RequestMemory'])])
        memory_utility_sorted.sort()
        if len(memory_utility_sorted) > 0:
            row["Min Util% Mem"]  = memory_utility_sorted[ 0]
            row["25% Util% Mem"]  = memory_utility_sorted[  len(memory_utility_sorted)//4]
            row["Med Util% Mem"]  = stats.median(memory_utility_sorted)
            row["75% Util% Mem"]  = memory_utility_sorted[3*len(memory_utility_sorted)//4]
            row["95% Util% Mem"]  = memory_utility_sorted[int(0.95*len(memory_utility_sorted))]
            row["Max Util% Mem"]  = memory_utility_sorted[-1]
            row["Mean Util% Mem"] = stats.mean(memory_utility_sorted)
        else:
            for col in [f"{x} Util% Mem" for x in ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]]:
                row[col] = 0
        if len(memory_utility_sorted) > 1:
            row["Stdv Util% Mem"] = stats.stdev(memory_utility_sorted)
        else:
            # There is no variance if there is only one value
            row["Stdv Util% Mem"] = 0

        # Totals that use the metrics generated for the distributions
        row["Total Unuse Mem GBh"] = sum(memory_hours_unusages_sorted) / (1024*3600)
        row["Total Mem GBh Util%"] = 100 * sum(memory_hours_usages_sorted) / sum(memory_hours_requested_sorted)
        row["Total Allo Mem GBh"] = sum(memory_hours_requested_sorted) / (1024*3600)

        # Compute mode for Project and Schedd columns in the Users table
        # if agg == "Users":
        #     projects = self.clean(data["ProjectName"])
        #     if len(projects) > 0:
        #         row["Most Used Project"] = max(set(projects), key=projects.count)
        #     else:
        #         row["Most Used Project"] = "UNKNOWN"

        #     schedds = self.clean(data["ScheddName"])
        #     if len(schedds) > 0:
        #         row["Most Used Schedd"] = max(set(schedds), key=schedds.count)
        #     else:
        #         row["Most Used Schedd"] = "UNKNOWN"
        # if agg == "Projects":
        #     row["Num Users"] = len(set(data["User"]))
        #     row["Num Site Instns"] = len(set(data["_Institutions"]))
        #     row["Num Sites"] = len(set(data["_Sites"]))
        #     if agg_name != "TOTAL":
        #         project_map = self.topology_project_map.get(agg_name.lower(), self.topology_project_map["UNKNOWN"])
        #         row["PI Institution"] = project_map["institution"]
        #     else:
        #         row["PI Institution"] = ""

        return row

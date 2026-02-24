
import re
import htcondor
import pickle
import statistics as stats
from pathlib import Path
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    10: "Num Uniq Job Ids",
    20: "All CPU Hours",
    30: "CPU Hours / Exec Att",

    40: "% Jobs w/1+ Holds",
    50: "% Jobs w/o Shadw",
    60: "Shadw Starts / Job Id",
    70: "Exec Atts / Shadw Start",
    75: "Holds / Job Id",

    80: "Avg MB Sent",
    81: "Max MB Sent",
    90: "Avg MB Recv",
    91: "Max MB Recv",

    200: "Max Rqst Mem MB",
    210: "Med Used Mem MB",
    220: "Max Used Mem MB",
    230: "Max Rqst Cpus",

    300: "Rm'd Jobs w/o Shadw Start",
    310: "Num Exec Atts",
    320: "Num Shadw Starts",
    325: "Num Job Holds",
    330: "Num DAG Node Jobs",
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
    "JobStatus",
    "EnteredCurrentStatus",
    "BytesSent",
    "BytesRecvd",
]


class OsgScheddCpuRemovedFilter(BaseFilter):
    name = "OSG schedd removed job history"

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
                            {"term": {
                                "JobStatus": 3
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

        # Get computed fields (as single values instead of arrays)
        f = {k: v[0] for k, v in doc.get("fields", {}).items()}

        # Get output dict for this schedd
        schedd = i.get("ScheddName", "UNKNOWN") or "UNKNOWN"
        o = data["Schedds"][schedd]

        # Filter out jobs that were not removed
        if i.get("JobStatus",4) != 3:
            return

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

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 0 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            if i.get("CommittedTime", 0) > 0:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
                o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
            else:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"])
                o["_NumBadJobStarts"].append(i["NumJobStarts"])
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr.startswith("Request"):
                o[attr].append(f.get(f"Floored{attr}", i.get(attr, None)))
            else:
                o[attr].append(i.get(attr, None))

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get computed fields (as single values instead of arrays)
        f = {k: v[0] for k, v in doc.get("fields", {}).items()}

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o = data["Users"][user]

        # Filter out jobs that were not removed
        if i.get("JobStatus",4) != 3:
            return

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

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 0 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            if i.get("CommittedTime", 0) > 0:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
                o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
            else:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"])
                o["_NumBadJobStarts"].append(i["NumJobStarts"])
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            # Use UNKNOWN for missing or blank ProjectName and ScheddName
            if attr in {"ScheddName", "ProjectName"}:
                o[attr].append(i.get(attr, i.get(attr.lower(), "UNKNOWN")) or "UNKNOWN")
            elif attr.startswith("Request"):
                o[attr].append(f.get(f"Floored{attr}", i.get(attr, None)))
            else:
                o[attr].append(i.get(attr, None))

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get computed fields (as single values instead of arrays)
        f = {k: v[0] for k, v in doc.get("fields", {}).items()}

        # Get output dict for this project
        project = i.get("ProjectName", i.get("projectname", "UNKNOWN")) or "UNKNOWN"
        o = data["Projects"][project]

        # Filter out jobs that were not removed
        if i.get("JobStatus",4) != 3:
            return

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

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

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 0 and
                i.get("RemoteWallClockTime", 0) > 0 and
                i.get("RemoteWallClockTime") != i.get("CommittedTime")
            ):
            if i.get("CommittedTime", 0) > 0:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - i.get("CommittedTime", 0))
                o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
            else:
                o["_BadWallClockTime"].append(i["RemoteWallClockTime"])
                o["_NumBadJobStarts"].append(i["NumJobStarts"])
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr.startswith("Request"):
                o[attr].append(f.get(f"Floored{attr}", i.get(attr, None)))
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

    def compute_custom_columns(self, data, agg, agg_name):
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

        # Compute columns
        row["All CPU Hours"]    = sum(self.clean(total_cpu_time)) / 3600
        row["Good CPU Hours"]   = sum(self.clean(goodput_cpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Rm'd Jobs w/o Shadw Start"] = sum([starts in [0, None] for starts in data["NumShadowStarts"]])
        row["Num Job Holds"]    = sum(self.clean(data["NumHolds"]))
        row["Num Jobs w/1+ Holds"] = sum([holds > 0 for holds in self.clean(data["NumHolds"])])
        row["Avg MB Sent"]      = stats.mean(self.clean(data["BytesSent"], allow_empty_list=False)) / 1e6
        row["Max MB Sent"]      = max(self.clean(data["BytesSent"], allow_empty_list=False)) / 1e6
        row["Avg MB Recv"]      = stats.mean(self.clean(data["BytesRecvd"], allow_empty_list=False)) / 1e6
        row["Max MB Recv"]      = max(self.clean(data["BytesRecvd"], allow_empty_list=False)) / 1e6

        row["Num DAG Node Jobs"] = sum(data["_NumDAGNodes"])
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Med Used Mem MB"]  = stats.median(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Num Exec Atts"]    = sum(self.clean(num_exec_attempts))
        row["Num Shadw Starts"] = sum(self.clean(num_shadow_starts))

        # Compute derivative columns
        if row["Num Uniq Job Ids"] - row["Rm'd Jobs w/o Shadw Start"] > 0:
            row["Shadw Starts / Job Id"] = row["Num Shadw Starts"] / (row["Num Uniq Job Ids"] - row["Rm'd Jobs w/o Shadw Start"])
        else:
            row["Shadw Starts / Job Id"] = 0
        if row["Num Uniq Job Ids"] > 0:
            row["% Jobs w/o Shadw"] = 100 * (row["Rm'd Jobs w/o Shadw Start"] / row["Num Uniq Job Ids"])
            row["Holds / Job Id"] = row["Num Job Holds"] / row["Num Uniq Job Ids"]
            row["% Jobs w/1+ Holds"] = 100 * row["Num Jobs w/1+ Holds"] / row["Num Uniq Job Ids"]
        else:
            row["% Jobs w/o Shadw"] = 0
            row["Holds / Job Id"] = 0
            row["% Jobs w/1+ Holds"] = 0
        if row["Num Shadw Starts"] > 0:
            row["Exec Atts / Shadw Start"] = row["Num Exec Atts"] / row["Num Shadw Starts"]
        else:
            row["Exec Atts / Shadw Start"] = 0
        if sum(data["_NumBadJobStarts"]) > 0:
            row["CPU Hours / Exec Att"] = (sum(self.clean(badput_cpu_time)) / 3600) / sum(data["_NumBadJobStarts"])
        else:
            row["CPU Hours / Exec Att"] = 0

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

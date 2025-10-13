
import re
import sys
import pickle
from pathlib import Path
from .BaseFilter import BaseFilter

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
    5 : "Project",
    10: "Last Wall Hrs",
    20: "Total Wall Hrs",
    30: "Potent CPU Hrs",
    40: "Actual CPU Hrs",
    50: "% CPU Eff",

    100: "Job Id",
    110: "Access Point",
    120: "Project",

    200: "Last Site",
    210: "Last Wrkr Node",
    220: "Last Wrkr MIPS",

    300: "Num Exec Atts",
    310: "Num Shadw Starts",
    330: "Num Holds",

    400: "Rqst Cpus",
    405: "CPUs Used",
    410: "Rqst Gpus",
    420: "Rqst Mem GB",
    425: "Mem Used GB",
    430: "Rqst Disk GB",
    435: "Disk Used GB",
    440: "MB Sent",
    450: "MB Recvd",
}


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "LastRemoteWallClockTime",
    "CommittedTime",
    "GlobalJobId",
    "ScheddName",
    "ProjectName",
    "MATCH_EXP_JOBGLIDEIN_ResourceName",
    "LastRemoteHost",
    "MachineAttrMips0",
    "NumJobStarts",
    "NumShadowStarts",
    "NumHolds",
    "RequestCpus",
    "CPUsUsage",
    "CpusUsage",
    "RequestGpus",
    "RequestMemory",
    "MemoryUsage",
    "RequestDisk",
    "DiskUsage",
    "BytesSent",
    "BytesRecvd",
]


class OsgScheddLongJobFilter(BaseFilter):
    name = "OSG schedd long job history"

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
        self.sort_col = "Last Wall Hrs"

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
                                "CommittedTime": {
                                    "gt": 3*60*60
                                }
                            }},
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

    def scan_and_filter(self, es_index, start_ts, end_ts, **kwargs):
        return super().scan_and_filter(es_index, start_ts, end_ts, build_totals=False, **kwargs)

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

    def user_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o = data["Users"][user]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

        # Skip jobs that are shorter than the longest CommittedTime
        if len(o["CommittedTime"]) > 0 and i.get("CommittedTime", 0) < o["CommittedTime"][0]:
            return

        # Get list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr not in o:
                o[attr].append(None)
            elif len(o[attr]) == 0:
                o[attr].append(None)

            # Use UNKNOWN for missing or blank ProjectName and ScheddName
            if attr in {"GlobalJobId", "ScheddName", "ProjectName",
                            "MATCH_EXP_JOBGLIDEIN_ResourceName",
                            "LastRemoteHost"}:
                o[attr][0] = i.get(attr, i.get(attr.lower(), "UNKNOWN")) or "UNKNOWN"
            elif attr in {"RequestGpus"}:
                o[attr][0] = i.get(attr, 0)
            else:
                o[attr][0] = i.get(attr, None)

        if "_NumJobs" not in o:
            o["_NumJobs"] = [1]

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.user_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Project and Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        return columns


    def compute_custom_columns(self, data, agg, agg_name):
        cpus_usage = data["CPUsUsage"][0]
        if cpus_usage is None:
            cpus_usage = data["CpusUsage"][0]

        # Output dictionary
        row = {}
        row["Project"] = data["ProjectName"]

        row["Last Wall Hrs"] = (data["LastRemoteWallClockTime"][0] or data["CommittedTime"][0] or 0) / 3600
        row["Total Wall Hrs"] = data["RemoteWallClockTime"][0] / 3600
        row["Potent CPU Hrs"] = max(data["RequestCpus"][0] or 1, 1) * row["Last Wall Hrs"]
        try:
            row["Actual CPU Hrs"] = cpus_usage * row["Last Wall Hrs"]
            row["% CPU Eff"] = 100 * cpus_usage / max(data["RequestCpus"][0] or 1, 1)
        except TypeError:
            row["Actual CPU Hrs"] = "n/a"
            row["% CPU Eff"] = "n/a"

        row["Job Id"] = data["GlobalJobId"][0].split("#")[1]
        row["Access Point"] = data["ScheddName"][0]
        row["Project"] = data["ProjectName"][0]

        row["Last Site"] = data["MATCH_EXP_JOBGLIDEIN_ResourceName"][0]
        row["Last Wrkr Node"] = data["LastRemoteHost"][0].split("@")[-1]
        row["Last Wrkr MIPS"] = data["MachineAttrMips0"][0] or "n/a"

        row["Num Exec Atts"] = data["NumJobStarts"][0] or 0
        row["Num Shadw Starts"] = data["NumShadowStarts"][0] or 0
        row["Num Holds"] = data["NumHolds"][0] or 0

        row["Rqst Cpus"] = data["RequestCpus"][0] or 1
        row["CPUs Used"] = data["CPUsUsage"][0]
        row["Rqst Gpus"] = data["RequestGpus"][0]
        row["Rqst Mem GB"] = (data["RequestMemory"][0] or 0) / 1024
        row["Mem Used GB"] = (data["MemoryUsage"][0] or 0) / 1024
        row["Rqst Disk GB"] = (data["RequestDisk"][0] or 0) / 1024**2
        row["Disk Used GB"] = (data["DiskUsage"][0] or 0) / 1024**2
        row["MB Sent"] = (data["BytesSent"][0] or 0) / 1024**2
        row["MB Recvd"] = (data["BytesRecvd"][0] or 0) / 1024**2

        return row

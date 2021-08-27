import logging
import htcondor
from pathlib import Path
from collections import defaultdict
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    10: "All CPU Hours", # "Last Wall Hrs",
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
        self.collector_host = "flock.opensciencegrid.org"
        self.schedd_collector_host_map = {}
        super().__init__(**kwargs)

    def get_query(self, index, start_ts, end_ts, scroll="30s", size=1000):
        # Returns dict matching Elasticsearch.search() kwargs
        # (Dict has same structure as the REST API query language)

        query = {
            "index": index,
            "scroll": scroll,
            "size": size,
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
                            {"term": {
                                "JobUniverse": {
                                "value": 5,
                                }
                            }},
                        ]
                    }
                }
            }
        }
        return query

    def scan_and_filter(self, es_index, start_ts, end_ts, **kwargs):
        # Returns a 3-level dictionary that contains data gathered from
        # Elasticsearch and filtered through whatever methods have been
        # defined in self.get_filters()
        
        # Create a data structure for storing filtered data:
        # 3-level defaultdict -> list
        # First level - Aggregation level (e.g. Schedd, User, Project)
        # Second level - Aggregation name (e.g. value of ScheddName, UserName, ProjectName)
        # Third level - Field name to be aggregated (e.g. RemoteWallClockTime, RequestCpus)
        filtered_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        
        query = self.get_query(
            index=es_index,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        # Use the scan() helper function, which automatically scrolls results. Nice!
        for doc in elasticsearch.helpers.scan(
                client=self.client,
                query=query.pop("body"),
                **query,
                ):

            # Send the doc through the various filters,
            # which mutate filtered_data in place
            for filtr in self.get_filters():
                filtr(filtered_data, doc)

        return filtered_data

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

        # Skip jobs that are shorter than the longest CommittedTime
        if len(o["CommittedTime"]) > 0 and i.get("CommittedTime", 0) < o["CommittedTime"][0]:
            return

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            # Use UNKNOWN for missing or blank ProjectName and ScheddName
            if attr in ["GlobalJobId", "ScheddName", "ProjectName",
                            "MATCH_EXP_JOBGLIDEIN_ResourceName",
                            "LastRemoteHost"]:
                o[attr][0] = i.get(attr, "UNKNOWN") or "UNKNOWN"
            elif attr in ["RequestGpus"]:
                o[attr][0] = i.get(attr, 0)
            else:
                o[attr][0] = i.get(attr, None)

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

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        columns_sorted = list(rows[0])
        columns_sorted[columns_sorted.index("All CPU Hours")] = "Last Wall Hrs"
        rows[0] = tuple(columns_sorted)
        return rows

    def compute_custom_columns(self, data, agg, agg_name):
        # Output dictionary
        row = {}

        row["Last Wall Hrs"] = data["CommittedTime"][0] / 3600
        row["Total Wall Hrs"] = data["RemoteWallClockTime"][0] / 3600
        row["Potent CPU Hrs"] = data["RequestCpus"][0] * row["Last Wall Hrs"]
        row["Actual CPU Hrs"] = data["CPUsUsage"][0] * row["Last Wall Hrs"]
        row["% CPU Eff"] = 100 * data["CPUsUsage"][0] / data["RequestCpus"][0]

        row["JobId"] = data["GlobalJobId"][0].split("#")[1]
        row["Access Point"] = data["ScheddName"][0]
        row["Project"] = data["ProjectName"][0]

        row["Last Site"] = data["MATCH_EXP_JOBGLIDEIN_ResourceName"][0]
        row["Last Wrkr Node"] = data["LastRemoteHost"][0].split("@")[-1]
        row["Last Wrkr MIPS"] = data["MachineAttrMips0"][0] or "-"

        row["Num Exec Atts"] = data["NumJobStarts"][0]
        row["Num Shadw Starts"] = data["NumShadowStarts"][0]
        row["Num Holds"] = data["NumHolds"][0]

        row["Rqst Cpus"] = data["RequestCpus"][0]
        row["CPUs Used"] = data["CPUsUsage"][0]
        row["Rqst Gpus"] = data["RequestGpus"][0]
        row["Rqst Mem GB"] = data["RequestMemory"][0] / 1024
        row["Mem Used GB"] = data["MemoryUsage"][0] / 1024
        row["Rqst Disk GB"] = data["RequestDisk"][0] / 1024**2
        row["Disk Used GB"] = data["DiskUsage"][0] / 1024**2
        row["MB Sent"] = data["BytesSent"][0] / 1024**2
        row["MB Recvd"] = data["BytesRecvd"][0] / 1024**2

        row["All CPU Hours"] = row["Last Wall Hrs"]

        return row 

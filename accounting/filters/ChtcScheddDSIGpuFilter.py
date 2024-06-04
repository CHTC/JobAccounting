
import statistics as stats
from pathlib import Path
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    15: "All GPU Hours",
    20: "Num Uniq Job Ids",
    240: "Max Rqst Gpus",
}


DEFAULT_FILTER_ATTRS = [
    "RemoteWallClockTime",
    "CommittedTime",
    "RequestCpus",
    "RequestGpus",
    "RequestMemory",
    "RecordTime",
    "JobCurrentStartDate",
    "MemoryUsage",
    "NumJobStarts",
    "NumShadowStarts",
    "JobStatus",
    "EnteredCurrentStatus",
    "BytesSent",
    "BytesRecvd",
    "LastRemoteHost"
]


class ChtcScheddDSIGpuFilter(BaseFilter):
    name = "DSI GPU schedd job history"


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sort_col = "All GPU Hours"


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
                            }},
                            {"term": {
                                "JobStatus": 4
                            }},
                            {"term": {
                                "JobUniverse": 5
                            }},
                            {"regexp": {
                                "LastRemoteHost.keyword": ".*dsigpu-?[0-9]+[.]chtc[.]wisc[.]edu"
                            }},
                        ]
                    }
                }
            }
        })
        return query

    def project_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict for this project
        project = i.get("ProjectName", "UNKNOWN") or "UNKNOWN"
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
        if (
                (
                    i.get("WhenToTransferOutput", "").upper() == "ON_EXIT_OR_EVICT" and
                    i.get("Is_resumable", False)
                ) or (
                    i.get("SuccessCheckpointExitBySignal", False) or
                    i.get("SuccessCheckpointExitCode") is not None
                )):
            o["_NumCkptJobs"].append(1)
        else:
            o["_NumCkptJobs"].append(0)

        # Compute badput fields
        if (
                i.get("NumJobStarts", 0) > 1 and
                i.get("RemoteWallClockTime", 0) > 0 and
                #i.get("RemoteWallClockTime") != int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0))))
                i.get("RemoteWallClockTime") != i.get("CommittedTime", 0)
            ):
            o["_BadWallClockTime"].append(i["RemoteWallClockTime"] - int(float(i.get("lastremotewallclocktime", i.get("CommittedTime", 0)))))
            o["_NumBadJobStarts"].append(i["NumJobStarts"] - 1)
        else:
            o["_BadWallClockTime"].append(0)
            o["_NumBadJobStarts"].append(0)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            if attr in {"lastremotewallclocktime", "activationduration", "activationsetupduration"}:
                try:
                    o[attr].append(int(float(i.get(attr))))
                except TypeError:
                    o[attr].append(None)
            else:
                o[attr].append(i.get(attr, None))


    def machine_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that were removed
        if i.get("JobStatus", 4) == 3:
            return

        # Get output dict for this site
        site = i.get("LastRemoteHost", "UNKNOWN") or "UNKNOWN"
        site = site.split("@")[-1]
        if "wisc.edu" not in site.casefold():
            schedd = i.get("ScheddName", "UNKNOWN")
            site = f"OSG via {schedd}"

        o = data["Site"][site]

        # Add custom attrs to the list of attrs
        filter_attrs = DEFAULT_FILTER_ATTRS.copy()
        filter_attrs = filter_attrs + ["User"]

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

        # Add attr values to the output dict, use None if missing
        for attr in filter_attrs:
            o[attr].append(i.get(attr, None))

    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.machine_filter,
            self.project_filter,
        ]
        return filters

    def add_custom_columns(self, agg):
        # Add Project and Schedd columns to the Users table
        columns = DEFAULT_COLUMNS.copy()
        columns[5] = "Num Users"
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        if agg == "Site":
            columns_sorted = list(rows[0])
            columns_sorted[columns_sorted.index("All GPU Hours")] = "Final Exec Att GPU Hours"
            rows[0] = tuple(columns_sorted)
        return rows


    def compute_custom_columns(self, data, agg, agg_name):

        if agg == "Site":
            row = self.compute_site_custom_columns(data, agg, agg_name)
            return row

        # Output dictionary
        row = {}

        # Compute goodput and total GPU hours columns
        goodput_gpu_time = []
        badput_gpu_time = []
        total_gpu_time = []
        for (goodput_time, badput_time, total_time, gpus) in zip(
                data["CommittedTime"],
                data["_BadWallClockTime"],
                data["RemoteWallClockTime"],
                data["RequestGpus"]):
            if None in [goodput_time, gpus]:
                goodput_gpu_time.append(None)
            else:
                goodput_gpu_time.append(goodput_time * gpus)
            if None in [badput_time, gpus]:
                badput_gpu_time.append(None)
            else:
                badput_gpu_time.append(badput_time * gpus)
            if None in [total_time, gpus]:
                total_gpu_time.append(None)
            else:
                total_gpu_time.append(total_time * gpus)

        # Compute columns
        row["All GPU Hours"]    = sum(self.clean(total_gpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Max Rqst Gpus"]    = max(self.clean(data["RequestGpus"], allow_empty_list=False))

        if agg == "Projects":
            row["Num Users"] = len(set(data["User"]))

        return row


    def compute_site_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute goodput and total GPU hours columns
        goodput_gpu_time = []
        for (goodput_time, gpus) in zip(
                data["CommittedTime"],
                data["RequestGpus"]):
            if None in [goodput_time, gpus]:
                goodput_gpu_time.append(None)
            else:
                goodput_gpu_time.append(goodput_time * gpus)

        # Compute columns
        row["All GPU Hours"]    = sum(self.clean(goodput_gpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Max Rqst Gpus"]    = max(self.clean(data["RequestGpus"], allow_empty_list=False))
        row["Num Users"]        = len(set(data["User"]))

        return row


import htcondor
import pickle
from pathlib import Path
from .BaseFilter import BaseFilter


DEFAULT_COLUMNS = {
    10: "Num Jobs w/ Activation Atts",
    20: "All CPU Hours",  # "Num Jobs w/ Activation Failures"
    30: "% Jobs w/ Failures",

    50: "Num Atts",
    60: "Num Atts / Job",
    70: "Num Failures",
    80: "Num Failures / Job",
    90: "Failures / Att",

    200: "Failure Reasons:",
    210: "% Transfer Input",
    300: "% Other",
}

DEFAULT_FILTER_ATTRS = [
    "NumJobStarts",
    "NumShadowStarts",
    "NumHoldsByReason",
]

class OsgScheddCpuActivationFilter(BaseFilter):
    name = "OSG schedd activation failure job history"
    
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
                                "NumShadowStarts": {
                                    "gt": 0,
                                }
                            }},
                            {"term": {
                                "JobUniverse": 5,
                            }},
                        ]
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

            for collector_host in self.collector_hosts:
                if collector_host in {"flock.opensciencegrid.org"}:
                    continue
                collector = htcondor.Collector(collector_host)
                ads = collector.query(
                    htcondor.AdTypes.Schedd,
                    constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                    projection=["CollectorHost"],
                )
                ads = list(ads)
                if len(ads) == 0:
                    continue
                if len(ads) > 1:
                    self.logger.warning(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

                # Cache the CollectorHost in the map
                if "CollectorHost" in ads[0]:
                    schedd_collector_hosts = set()
                    for schedd_collector_host in ads[0]["CollectorHost"].split(","):
                        schedd_collector_host = schedd_collector_host.strip().split(":")[0]
                        if schedd_collector_host:
                            schedd_collector_hosts.add(schedd_collector_host)
                    if schedd_collector_hosts:
                        self.schedd_collector_host_map[schedd] = schedd_collector_hosts
                        break
            else:
                self.logger.warning(f"Did not find Machine == {schedd} in collectors")

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

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

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

        # Count number of history ads (i.e. number of unique job ids)
        o["_NumJobs"].append(1)

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
        if not self.is_ospool_job(i):
            return

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
        if agg == "Projects":
            columns[5] = "Num Users"
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        columns_sorted = list(rows[0])
        columns_sorted[columns_sorted.index("All CPU Hours")] = "Num Jobs w/ Activation Failures"
        rows[0] = tuple(columns_sorted)
        return rows

    def compute_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute failures
        num_failures = 0
        num_job_failures = 0
        num_shadow_failures = 0
        for (
                num_shadow_starts,
                num_job_starts
            ) in zip(
                data["NumShadowStarts"],
                data["NumJobStarts"]
            ):

            if num_job_starts is None:
                num_job_starts = 0

            if num_shadow_starts > num_job_starts:
                num_failures += num_shadow_starts - num_job_starts
                num_jobs_failures += 1

        # Compute failure reasons
        transfer_input_hold_reasons = {
            "TransferInputError",
            "UploadFileError",
        }
        num_failures_transfer_input = 0
        for job_hold_reasons in data["NumHoldsByReason"]:
            if job_hold_reasons is None:
                continue

            if transfer_input_hold_reasons & set(job_hold_reasons):
                num_failures_transfer_input = sum([
                    int(job_hold_reasons.get(reason, 0))
                    for reason in transfer_input_error_reasons
                ])
        # Everything else falls under other reasons
        num_failures_other = num_failures - (
                               num_failures_transfer_input
                            )

        # Compute columns
        row["Num Jobs w/ Activation Atts"] = sum(data["_NumJobs"])
        row["Num Jobs w/ Activation Failures"] = num_job_failures
        row["% Jobs w/ Failures"] = 100 * row["Num Jobs w/ Activation Failures"] / row["Num Jobs w/ Activation Atts"]

        row["Num Atts"] = sum(self.clean(data["NumShadowStarts"], allow_empty_list=False))
        row["Num Atts / Job"] = row["Num Atts"] / row["Num Jobs w/ Activation Atts"]
        row["Num Failures"] = num_failures
        row["Num Failures / Job"] = row["Num Failures"] / row["Num Jobs w/ Activation Atts"]
        row["Failures / Att"] = row["Num Failures"] / row["Num Atts"]

        row["Failure Reasons:"] = ""
        if row["Num Failures"] > 0:
            row["% Transfer Input"] = 100 * num_failures_transfer_input / row["Num Failures"]
            row["% Other"] = 100 * num_failures_other / row["Num Failures"]
        else:
            row["% Transfer Input"] = row["% Other"] = "n/a"

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

        row["All CPU Hours"] = row["Num Jobs w/ Activation Failures"]

        return row 

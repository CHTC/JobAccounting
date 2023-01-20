
import htcondor
import pickle
from pathlib import Path
from .BaseFilter import BaseFilter
from accounting.pull_hold_reasons import get_hold_reasons


DEFAULT_COLUMNS = {
    10: "All CPU Hours",  # Num Uniq Job Ids
    20: "Shadow Starts / Job Id",
    30: "Non Success Shadows (NSS)",

    50: "% Jobs w/ >1 Shadow Starts",
    60: "% Jobs w/ >0 Input Xfer Errs",
    70: "% NSS due to Input Xfer Errs",
}

DEFAULT_FILTER_ATTRS = [
    "NumShadowStarts",
    "NumHolds",
    "NumHoldsByReason",
    "JobStatus",
]

class OsgScheddCpuRetryFilter(BaseFilter):
    name = "OSG schedd retried job history"
    
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
            columns[175] = "Most Used Schedd"
        if agg == "Projects":
            columns[5] = "Num Users"
        return columns

    def merge_filtered_data(self, data, agg):
        rows = super().merge_filtered_data(data, agg)
        columns_sorted = list(rows[0])
        columns_sorted[columns_sorted.index("All CPU Hours")] = "Num Uniq Job Ids"
        rows[0] = tuple(columns_sorted)
        return rows

    def compute_custom_columns(self, data, agg, agg_name):

        # Output dictionary
        row = {}

        # Compute non successful shadow starts
        non_success_shadow_starts = 0
        num_jobs_multi_shadows = 0
        for (
                num_shadow_starts,
                job_status
            ) in zip(
                data["NumShadowStarts"],
                data["JobStatus"]
            ):
            if None in [num_shadow_starts, job_status]:
                continue

            if num_shadow_starts > 1:
                num_jobs_multi_shadows += 1
            if job_status == 3:  # removed jobs never succeeded
                non_success_shadow_starts += num_shadow_starts
            else:  # assume last shadow start succeeded
                non_success_shadow_starts += max(0, num_shadow_starts - 1)

        # Compute transfer input hold counts
        transfer_input_error_reasons = {
            "TransferInputError",
            "UploadFileError",
        }
        num_holds_input_transfer_errors = 0
        num_jobs_input_transfer_errors = 0
        condor_hold_reasons = get_hold_reasons()
        for job_hold_reasons in data["NumHoldsByReason"]:
            if job_hold_reasons is None:
                continue

            if transfer_input_error_reasons & set(job_hold_reasons):
                transfer_input_holds = sum([
                    int(job_hold_reasons.get(reason, 0))
                    for reason in transfer_input_error_reasons
                ])
                num_holds_input_transfer_errors += transfer_input_holds
                num_jobs_input_transfer_errors += int(transfer_input_holds > 0)

        # Compute columns
        row["Num Uniq Job Ids"] = sum(data["_NumJobs"])
        row["Non Success Shadows (NSS)"] = non_success_shadow_starts
        if row["Num Uniq Job Ids"] > 0:
            row["Shadow Starts / Job Id"] = sum(self.clean(data["NumShadowStarts"], allow_empty_list=False)) / row["Num Uniq Job Ids"]
            row["% Jobs w/ >1 Shadow Starts"] = 100 * num_jobs_multi_shadows / row["Num Uniq Job Ids"]
            row["% Jobs w/ >0 Xfer Input Errors"] = 100 * num_jobs_input_transfer_errors / row["Num Uniq Job Ids"]
        else:
            row["Shadw Starts / Job Id"] = "n/a"
            row["% Jobs w/ >1 Shadow Starts"] = "n/a"
            row["% Jobs w/ >0 Xfer Input Errors"] = "n/a"
        if row["Non Success Shadows (NSS)"] > 0:
            row["% NSS due to Input Xfer Errs"] = 100 * num_holds_input_transfer_errors / non_success_shadow_starts

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

        row["All CPU Hours"] = row["Num Uniq Job Ids"]

        return row 

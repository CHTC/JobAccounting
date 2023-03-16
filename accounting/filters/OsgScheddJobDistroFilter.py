
import htcondor
import pickle
from pathlib import Path
from elasticsearch import Elasticsearch
import elasticsearch.helpers
from .BaseFilter import BaseFilter
from functools import lru_cache
from collections import defaultdict


OSG_CONNECT_APS = {
    "login04.osgconnect.net",
    "login05.osgconnect.net",
    "login-test.osgconnect.net",
    "ap2007.chtc.wisc.edu",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
}

DISK_COLUMNS = {x: f"({x}, {x+2}]" for x in range(0, 20, 2)}
DISK_COLUMNS[20] = "(20,)"
DISK_QUANTILES = list(DISK_COLUMNS.keys())
DISK_QUANTILES.sort()


MEMORY_ROWS = {y: f"({y}, {y+1}]" for y in range(0, 8, 1)}
MEMORY_ROWS[8] = "(8,)"
MEMORY_QUANTILES = list(MEMORY_ROWS.keys())
MEMORY_QUANTILES.sort()

class OsgScheddJobDistroFilter(BaseFilter):
    name = "OSG schedd job distribution"


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
                            {"term": {
                                "JobUniverse": {
                                    "value": 5,
                                }
                            }},
                            {"terms": {
                                "ScheddName.keyword": list(OSG_CONNECT_APS)
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


    @lru_cache(maxsize=1024)
    def is_ospool_job(self, schedd_name, last_remote_pool):
        remote_pool = set()
        if last_remote_pool is not None:
            if last_remote_pool.strip():
                return last_remote_pool in self.collector_hosts
        if schedd_name is not None:
            if schedd_name.strip():
                remote_pools = self.schedd_collector_host(schedd_name)
                return bool(remote_pools & self.collector_hosts)
        return False


    def scan_and_filter(self, es_index, start_ts, end_ts, build_totals=False, **kwargs):
        # Returns a 3-level dictionary that contains data gathered from
        # Elasticsearch and filtered through whatever methods have been
        # defined in self.get_filters()

        # Create a data structure for storing filtered data:
        filtered_data = {
            "JobRequests": {},
            "JobUsages": {}
        }

        # Get list of indices so we can use one at a time
        indices = list(self.client.indices.get_alias(index=es_index).keys())
        indices.sort(reverse=True)
        indices.insert(0, indices.pop())  # make sure the first index gets checked first
        self.logger.debug(f"Querying at most {len(indices)} indices matching {es_index}.")
        got_initial_data = False  # only stop after we've seen data

        for index in indices:

            query = self.get_query(
                index=index,
                start_ts=start_ts,
                end_ts=end_ts,
            )

            # Use the scan() helper function, which automatically scrolls results. Nice!
            self.logger.debug(f"Querying {index}.")
            got_index_data = False
            for doc in elasticsearch.helpers.scan(
                    client=self.client,
                    query=query.pop("body"),
                    **query,
                    ):
                got_initial_data = True
                got_index_data = True

                # Send the doc through the various filters,
                # which mutate filtered_data in place
                for filtr in self.get_filters():
                    filtr(filtered_data, doc)

            # Break early if not finding more results
            if got_initial_data and not got_index_data:
                self.logger.debug(f"Exiting scan early since no docs were found")
                break

        return filtered_data


    @lru_cache(maxsize=1024)
    def quantize_disk(self, disk_kb):
        if disk_kb <= 0:
            return 0
        q = 0
        for q_disk_gb in DISK_QUANTILES:
            q_disk_kb = q_disk_gb * (1024 * 1024)
            if disk_kb > q_disk_kb:
                q = q_disk_gb
            else:
                break
        return q


    @lru_cache(maxsize=1024)
    def quantize_memory(self, memory_mb):
        if memory_mb <= 0:
            return 0
        q = 0
        for q_memory_gb in MEMORY_QUANTILES:
            q_memory_mb = q_memory_gb * 1024
            if memory_mb > q_memory_mb:
                q = q_memory_gb
            else:
                break
        return q


    def job_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i.get("ScheddName"), i.get("LastRemotePool")):
            return

        # Get output dict
        requests = data["JobRequests"]
        usages = data["JobUsages"]

        # Check for missing attrs
        request_disk = i.get("RequestDisk")
        request_memory = i.get("RequestMemory")
        skip_requests = None in [request_disk, request_memory]

        usage_disk = i.get("DiskUsage_RAW", i.get("DiskUsage"))
        usage_memory = i.get("MemoryUsage_RAW", i.get("MemoryUsage"))
        skip_usages = None in [usage_disk, usage_memory]

        if not skip_requests:
            total_jobs = requests.get("TotalJobs", 0)
            requests["TotalJobs"] = total_jobs + 1
            # Filter out jobs that request more than one core
            if not i.get("RequestCpus", 1) > 1:
                histogram = requests.get("Histogram", defaultdict(int))
                q_request_disk = self.quantize_disk(request_disk)
                q_request_memory = self.quantize_memory(request_memory)
                histogram[(q_request_disk, q_request_memory)] += 1
                requests["Histogram"] = histogram
                jobs = requests.get("SingleCoreJobs", 0)
                requests["SingleCoreJobs"] = jobs + 1

        if not skip_usages:
            total_jobs = usages.get("TotalJobs", 0)
            usages["TotalJobs"] = total_jobs + 1
            # Filter out jobs that request more than one core
            if not i.get("RequestCpus", 1) > 1:
                histogram = usages.get("Histogram", defaultdict(int))
                q_usage_disk = self.quantize_disk(usage_disk)
                q_usage_memory = self.quantize_memory(usage_memory)
                histogram[(q_usage_disk, q_usage_memory)] += 1
                usages["Histogram"] = histogram
                jobs = usages.get("SingleCoreJobs", 0)
                usages["SingleCoreJobs"] = jobs + 1


    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.job_filter,
        ]
        return filters


    def compute_frequency_histogram(self, data):

        histogram = data["Histogram"]
        for k, v in histogram.items():
            histogram[k] = 100*v/data["SingleCoreJobs"]

        return histogram


    def merge_filtered_data(self, data, agg):
        # Return data sheet
        # Columns are disk requests
        # Rows are memory requests

        histogram = self.compute_frequency_histogram(data[agg])
        single_core_jobs = data[agg]["SingleCoreJobs"]
        total_jobs = data[agg]["TotalJobs"]
        jobs_note = f"{single_core_jobs}/{total_jobs}"
        xs = list(DISK_COLUMNS.keys())
        xs.sort()
        ys = list(MEMORY_ROWS.keys())
        ys.sort()

        rows = []
        header_row = [jobs_note]
        for key in xs:
            header_row.append(DISK_COLUMNS[key])
        rows.append(tuple(header_row))

        for y in ys:
            row = [MEMORY_ROWS[y]]
            for x in xs:
                row.append(histogram[(x, y)])
            rows.append(tuple(row))

        return rows
import elasticsearch.helpers
from .BaseFilter import BaseFilter
from functools import lru_cache
from collections import defaultdict


CHTC_APS = {
    "ap2001.chtc.wisc.edu",
    "ap2002.chtc.wisc.edu",
    "atlassubmit1000.chtc.wisc.edu",
    "atlassubmit1001.chtc.wisc.edu",
    "atlassubmit1002.chtc.wisc.edu",
    "atlassubmit2000.chtc.wisc.edu",
    "atlassubmit2001.chtc.wisc.edu",
    "batlabsubmit0001.chtc.wisc.edu",
    "cm3000.chtc.wisc.edu",
    "cosmos0001.chtc.wisc.edu",
    "deepdivesubmit2000.chtc.wisc.edu",
    "jupyter0000.chtc.wisc.edu",
    "keles-submit3000.chtc.wisc.edu",
    "learn.chtc.wisc.edu",
    "oconnorsubmit3000.chtc.wisc.edu",
    "pagesubmit3000.chtc.wisc.edu",
    "submit-1.chtc.wisc.edu",
    "submit2.chtc.wisc.edu",
    "submit3.chtc.wisc.edu",
    "submit4.chtc.wisc.edu",
    "submit5.chtc.wisc.edu",
    "submittest0000.chtc.wisc.edu",
    "tgrant0000.chtc.wisc.edu",
    "tiger0000.chtc.wisc.edu",
    "townsend-submit.chtc.wisc.edu",
    "wrightsubmit3000.chtc.wisc.edu",
}

DISK_COLUMNS = {x: f"({x}, {x+2}]" for x in range(0, 20, 2)}
DISK_COLUMNS[20] = "(20,)"
DISK_QUANTILES = list(DISK_COLUMNS.keys())
DISK_QUANTILES.sort()


MEMORY_ROWS = {y: f"({y}, {y+1}]" for y in range(0, 8, 1)}
MEMORY_ROWS[8] = "(8,)"
MEMORY_QUANTILES = list(MEMORY_ROWS.keys())
MEMORY_QUANTILES.sort()

class ChtcScheddJobDistroFilter(BaseFilter):
    name = "CHTC schedd job distribution"


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
                                "ScheddName.keyword": list(CHTC_APS)
                            }},
                        ]
                    }
                }
            }
        })
        return query


    def scan_and_filter(self, es_index, start_ts, end_ts, build_totals=False, **kwargs):
        # Returns a 3-level dictionary that contains data gathered from
        # Elasticsearch and filtered through whatever methods have been
        # defined in self.get_filters()

        # Create a data structure for storing filtered data:
        filtered_data = {
            "JobRequests": {},
            "JobUsages": {}
        }

        query = self.get_query(
            index=es_index,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        # Get total number of matching docs for progress logging
        total_hits = self.client.count(
            index=es_index,
            body=query.get("body"),
        )["count"]
        self.logger.debug(f"Scanning {total_hits} docs matching {es_index}.")

        # Use the scan() helper function, which automatically scrolls results. Nice!
        for i, doc in enumerate(elasticsearch.helpers.scan(
                client=self.client,
                query=query.pop("body"),
                **query,
                )):
            if i > 0 and i % 50_000 == 0:
                self.logger.debug(f"Processed {i}/{total_hits} docs ({100*i/total_hits:.0f}%)")

            # Send the doc through the various filters,
            # which mutate filtered_data in place
            for filtr in self.get_filters():
                filtr(filtered_data, doc)

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

        # Get computed fields (as single values instead of arrays)
        f = {k: v[0] for k, v in doc.get("fields", {}).items()}

        # Get output dict
        requests = data["JobRequests"]
        usages = data["JobUsages"]

        # Check for missing attrs
        request_disk = f.get("FlooredRequestDisk")
        request_memory = f.get("FlooredRequestMemory")
        skip_requests = None in [request_disk, request_memory]

        usage_disk = i.get("DiskUsage_RAW", i.get("DiskUsage"))
        usage_memory = i.get("MemoryUsage_RAW", i.get("MemoryUsage"))
        skip_usages = None in [usage_disk, usage_memory]

        if not skip_requests:
            total_jobs = requests.get("TotalJobs", 0)
            requests["TotalJobs"] = total_jobs + 1
            # Filter out jobs that request more than one core
            if not f.get("FlooredRequestCpus", 1) > 1:
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
            if not f.get("FlooredRequestCpus", 1) > 1:
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

import htcondor
import pickle
from pathlib import Path
from elasticsearch import Elasticsearch
import elasticsearch.helpers
from .BaseFilter import BaseFilter
from functools import lru_cache
from collections import defaultdict


DEFAULT_FILTER_ATTRS = [
    "RequestMemory",
    "RequestDisk",
]

OSG_CONNECT_APS = {
    "login04.osgconnect.net",
    "login05.osgconnect.net",
    "login-test.osgconnect.net",
    "ap2007.chtc.wisc.edu",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
}

DISK_COLUMNS = {x: f"{x} to <{x+2} GB" for x in range(0, 20, 2)}
DISK_COLUMNS[20] = "20+ GB"

MEMORY_ROWS = {y: f"{y} to <{y+1} GB" for y in range(0, 8, 1)}
MEMORY_ROWS[8] = "8+ GB"

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
                        ]
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


    def scan_and_filter(self, es_index, start_ts, end_ts, build_totals=False, **kwargs):
        # Returns a 3-level dictionary that contains data gathered from
        # Elasticsearch and filtered through whatever methods have been
        # defined in self.get_filters()

        # Create a data structure for storing filtered data:
        filtered_data = {"Jobs": defaultdict(list)}

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


    def job_filter(self, data, doc):

        # Get input dict
        i = doc["_source"]

        # Get output dict
        o = data["Jobs"]

        # Filter out jobs that did not run from an OSG Connect AP
        ap = i.get("GlobalJobId", "UNKNOWN").split("#")[0]
        if not (ap in OSG_CONNECT_APS):
            return

        # Filter out jobs that request more than one core
        if i.get("RequestCpus", 1) > 1:
            return

        # Filter out jobs that did not run in the OS pool
        if not self.is_ospool_job(i):
            return

        # Add attr values to the output dict, use None if missing
        for attr in DEFAULT_FILTER_ATTRS:
            o[attr].append(i.get(attr, None))


    def get_filters(self):
        # Add all filter methods to a list
        filters = [
            self.job_filter,
        ]
        return filters


    def compute_histogram(self, data):

        disk_quantiles = list(DISK_COLUMNS.keys())
        disk_quantiles.sort()

        memory_quantiles = list(MEMORY_ROWS.keys())
        memory_quantiles.sort()

        @lru_cache(maxsize=1024)
        def quantize_disk(disk_kb):
            disk_gb = int(disk_kb / (1024*1024))  # can chop off the decimal
            if disk_gb < 0:
                return 0
            q = 0
            for q_check in disk_quantiles:
                if disk_gb >= q_check:
                    q = q_check
                else:
                    break
            return q

        @lru_cache(maxsize=1024)
        def quantize_memory(memory_mb):
            memory_gb = int(memory_mb / 1024)  # can chop off the decimal
            if memory_gb < 0:
                return 0
            q = 0
            for q_check in memory_quantiles:
                if memory_gb >= q_check:
                    q = q_check
                else:
                    break
            return q


        histogram = defaultdict(int)
        for disk, memory in zip(data["RequestDisk"], data["RequestMemory"]):
            if None in [disk, memory]:
                continue
            d, m = quantize_disk(disk), quantize_memory(memory)
            histogram[(d, m)] += 1

        return histogram


    def merge_filtered_data(self, data, *args):
        # Return data sheet
        # Columns are disk requests
        # Rows are memory requests

        histogram = self.compute_histogram(data["Jobs"])
        xs = list(DISK_COLUMNS.keys())
        xs.sort()
        ys = list(MEMORY_ROWS.keys())
        ys.sort()

        rows = []
        header_row = ["Disk/Memory"]
        for key in xs:
            header_row.append(DISK_COLUMNS[key])
        rows.append(tuple(header_row))

        for y in ys:
            row = [MEMORY_ROWS[y]]
            for x in xs:
                row.append(histogram[(x, y)])
            rows.append(tuple(row))

        return rows
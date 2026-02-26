import logging
from collections import defaultdict
from functools import partial
from operator import itemgetter
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import importlib


class BaseFilter:
    name = "job history"

    def __init__(self, skip_init=False, **kwargs):
        self.sort_col = "All CPU Hours"
        self.logger = logging.getLogger("accounting.filter")
        if skip_init:
            return
        self.client = self.connect(**kwargs)
        self.data = self.scan_and_filter(**kwargs)

    def connect(self, es_host, es_user, es_pass, es_use_https, es_ca_certs, **kwargs):
        # Returns Elasticsearch client

        # Split off port from host if included
        if ":" in es_host and len(es_host.split(":")) == 2:
            [es_host, es_port] = es_host.split(":")
            es_port = int(es_port)
        elif ":" in es_host:
            self.logger.error(f"Ambiguous hostname:port in given host: {es_host}")
            sys.exit(1)
        else:
            es_port = 9200
        es_client = {
            "host": es_host,
            "port": es_port
        }

        # Include username and password if both are provided
        if es_user is None and es_pass is None:
            pass
        elif (es_user is None) != (es_pass is None):
            self.logger.warning("Only one of es_user and es_pass have been defined")
            self.logger.warning("Connecting to Elasticsearch anonymously")
        else:
            es_client["http_auth"] = (es_user, es_pass)

        # Only use HTTPS if CA certs are given or if certifi is available
        if es_use_https:
            if es_ca_certs is not None:
                self.logger.info(f"Using CA from {es_ca_certs}")
                es_client["ca_certs"] = es_ca_certs
            elif importlib.util.find_spec("certifi") is not None:
                self.logger.info("Using CA from certifi library")
            else:
                self.logger.error("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
                sys.exit(1)
            es_client["use_ssl"] = True
            es_client["verify_certs"] = True

        return Elasticsearch([es_client])

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
                        ],
                        "must_not": [
                            {"terms": {
                                "JobUniverse": [7, 12]
                            }},
                        ],
                    }
                }
            }
        }
        return query

    def user_filter(self, data, doc):
        # Example filter that accumulates job attr values
        # into "data", aggregated by the User job attribute.
        # Feel free to override this filter
        # and to add your own filters, being sure to
        # add each filter method to the get_filters() method.

        # List of job ad attrs to accumulate
        filter_attrs = [
            "RemoteWallClockTime",
            "CommittedTime",
            "RequestCpus",
            "RequestMemory",
            "MemoryUsage",
            "BytesSent",
            "BytesRecvd",
        ]

        # Get input dict
        i = doc["_source"]

        # Get output dict for this user
        user = i.get("User", "UNKNOWN") or "UNKNOWN"
        o_user = data["Users"][user]

        # Count number of history ads (i.e. number of unique job ids)
        o_user["_NumJobs"].append(1)

        # Append filtered data, use None if missing
        for attr in filter_attrs:
            o_user[attr].append(i.get(attr, None))

    def get_filters(self):
        # Returns a list of filter methods
        # This method should be overridden,
        # unless you really want to use the example user filter
        filters = [
            self.user_filter,
        ]
        return filters

    def scan_and_filter(self, es_index, start_ts, end_ts, build_totals=True, **kwargs):
        # Returns a 3-level dictionary that contains data gathered from
        # Elasticsearch and filtered through whatever methods have been
        # defined in self.get_filters()

        # Create a data structure for storing filtered data:
        # 3-level defaultdict -> list
        # First level - Aggregation level (e.g. Schedd, User, Project)
        # Second level - Aggregation name (e.g. value of ScheddName, UserName, ProjectName)
        # Third level - Field name to be aggregated (e.g. RemoteWallClockTime, RequestCpus)
        filtered_data = defaultdict(partial(defaultdict, partial(defaultdict, list)))

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

        # Build totals
        if build_totals:
            for agg in filtered_data.keys():
                total = defaultdict(list)
                for agg_name in filtered_data[agg].keys():
                    for field, data in filtered_data[agg][agg_name].items():
                        total[field] += data
                filtered_data[agg]["TOTAL"] = total

        return filtered_data

    def get_filtered_data(self):
        return self.data

    def add_custom_columns(self, *args, **kwargs):
        # Example method for defining columns.
        # Override this method to add custom columns.
        # Must return a dict formatted like:
        # { 123: "ColumnName", 555: "OtherColumn" }

        # The resulting data structure will have columns sorted by the
        # values of the keys in this dictionary.

        columns = {
            10: "All CPU Hours",
            20: "% Good CPU Hours",
            30: "Good CPU Hours",
            40: "Num Uniq Job Ids",
            50: "Max Rqst Mem MB",
            60: "Max Used Mem MB",
            70: "Max Rqst Cpus",
            80: "Max MB Sent",
            90: "Max MB Recv",
        }

        return columns

    def clean(self, dirty_list, allow_empty_list=True):
        # Remove None from dirty_list
        cleaned = [x for x in dirty_list if x is not None]
        if len(cleaned) == 0 and not allow_empty_list:
            cleaned = [-999]
        return cleaned

    def compute_custom_columns(self, data, *args, **kwargs):
        # Example method for computing columns.
        # Override this method to compute custom column values.
        # Must return a dict formatted like:
        # { "ColumnName": 50, "OtherColumn": 999.999 }

        # Output dictionary
        row = {}

        # Compute goodput and total CPU hours columns
        goodput_cpu_time = []
        total_cpu_time = []
        for (goodput_time, total_time, cpus) in zip(
                data["CommittedTime"],
                data["RemoteWallClockTime"],
                data["RequestCpus"]):
            if cpus is not None:
                cpus = max(cpus, 1)  # assume at least 1 CPU even if 0 CPUs were stored in Elasticsearch
            if None in [goodput_time, cpus]:
                goodput_cpu_time.append(None)
            else:
                goodput_cpu_time.append(goodput_time * cpus)
            if None in [total_time, cpus]:
                total_cpu_time.append(None)
            else:
                total_cpu_time.append(total_time * cpus)

        # Compute columns
        row["All CPU Hours"]    = sum(self.clean(total_cpu_time)) / 3600
        row["Good CPU Hours"]   = sum(self.clean(goodput_cpu_time)) / 3600
        row["Num Uniq Job Ids"] = sum(data['_NumJobs'])
        row["Max Rqst Mem MB"]  = max(self.clean(data['RequestMemory'], allow_empty_list=False))
        row["Max Used Mem MB"]  = max(self.clean(data["MemoryUsage"], allow_empty_list=False))
        row["Max Rqst Cpus"]    = max(self.clean(data["RequestCpus"], allow_empty_list=False))
        row["Max MB Sent"]      = max(self.clean(data["BytesSent"], allow_empty_list=False)) / 1e6
        row["Max MB Recv"]      = max(self.clean(data["BytesRecvd"], allow_empty_list=False)) / 1e6

        if row["All CPU Hours"] > 0:
            row["% Good CPU Hours"] = 100 * row["Good CPU Hours"] / row["All CPU Hours"]
        else:
            row["% Good CPU Hours"] = 0

        return row

    def merge_filtered_data(self, data, agg):
        # Takes filtered data and an aggregation level (e.g. Users, Schedds,
        # Projects) and returns a list of tuples, with the first item
        # containing a tuple of column names in the order that matches
        # the following tuples of values

        # Get the dict of columns
        columns = self.add_custom_columns(agg)

        # Make the 0th column the aggregation level
        # e.g. agg on "Users"   -> "User" column
        #      agg on "Schedds" -> "Schedd" column
        columns[0] = agg.rstrip("s")

        # Get the names of the columns in order
        columns_sorted = [col for (n, col) in sorted(columns.items())]

        # Loop over aggregated data and store computed data in rows
        rows = []
        for agg_name, d in data[agg].items():
            row = {}

            # It's possible to have no job data stored
            # if the dict was initialized but then job data was skipped
            if len(d["_NumJobs"]) == 0:
                continue

            # Put the name for this aggregation
            # (e.g. user name, schedd name, project name)
            # as the value for the 0th column
            row[columns[0]] = agg_name

            # Compute any custom columns
            row.update(self.compute_custom_columns(d, agg, agg_name))

            # Store a tuple of all column data in order
            rows.append(tuple(row[col] for col in columns_sorted))

        # Sort rows
        rows.sort(reverse=True, key=itemgetter(columns_sorted.index(self.sort_col)))

        # Ensure the TOTAL row is at the top
        try:
            rows.insert(0, rows.pop(list(row[0] for row in rows).index("TOTAL")))
        except ValueError:
            pass

        # Prepend the header row
        rows.insert(0, tuple(columns_sorted))

        return rows

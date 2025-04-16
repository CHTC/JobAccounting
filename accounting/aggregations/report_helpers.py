import elasticsearch
from elasticsearch_dsl import Search, Q, A
from datetime import datetime, timedelta
from collections import namedtuple
from operator import itemgetter
from pprint import pprint

# each aggregation is initially stored as an elasticseach 'A' object
# in a named tuple along with it's name, 'pretty' name, and type (metric, bucket, or pipeline)
Aggregation = namedtuple("Aggregation", ['object', 'name', 'pretty_name', 'type', 'mult_names'],
                         defaults=[None, None, None, None, []])


def add_runtime_script(search: Search, field_name: str, script: str, ret_type: str):
    """ Modify an elasticsearch_dsl Search object by adding a runtime_mapping
        params:
            search      - the elasticsearch_dsl Search object to modify
            field_name  - the name of the runtime field the script will generate
            script      - the script's source code
            ret_type    - the type of the field the script will produce 

    """
    
    d = { field_name : {
            "type": ret_type,
            "script": {
                "language": "painless",
                "source": script,
            }
        }
    }
    
    maps = search.to_dict().get("runtime_mappings", {})
    maps.update(d)

    search.update_from_dict({"runtime_mappings" : maps})

def get_percent_bucket_script(want_percent: str, out_of: str) -> A:
    """ returns an 'A' object that uses a bucket_script aggregation
        to compute a percentage across two other metrics
        NOTE: the returned aggregation must be nested under a multi-bucket aggregation
        and cannot be applied at the top level
         
        example: to calculate percent goodput use
        get_percent_metric("good_cpu_hours", "total_cpu_hours")
        if you have already created 2 metrics good_cpu_hours and total_cpu_hours
    """
    
    return A("bucket_script",
            buckets_path={"a" : want_percent,
                          "b"  : out_of},
            script="params.a / params.b * 100"                 
            )

def table(rows: list):
    """ Generates a table to display the report on the command line
        params:
            rows - a list of dicts that map column names to values
    """

    try:
        from tabulate import tabulate

    except Exception:
        print("run 'pip install tabulate' to see a nicer table!\n")
        # print for debugging
        print("\t".join(list(rows[0].keys())))
        for row in rows:
            pprint(row.values())
            print()

    # print a nice table if tabulate is installed 
    # NOTE: the table is very wide, should pipe into 'less -S'
    print(tabulate(rows, 
                   headers="keys", 
                   tablefmt="grid"
    ))

def print_error(d, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")



"""
Helper functions and constants for OSPool metrics collection.

Includes Elasticsearch connection setup, OSPool access point discovery,
and output formatting utilities.
"""

from __future__ import annotations

import re
import sys
import time
import pickle
import argparse
import importlib.util

from pathlib import Path
from datetime import datetime

import shapefile
import elasticsearch

from shapely.geometry import Point, shape

try:
    import htcondor2 as htcondor
except ImportError:
    try:
        import htcondor
    except ImportError:
        htcondor = None


OSPOOL_COLLECTORS = {"cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"}

OSPOOL_APS = {
    "amundsen.grid.uchicago.edu",
    "ap20.uc.osg-htc.org",
    "ap2007.chtc.wisc.edu",
    "ap21.uc.osg-htc.org",
    "ap22.uc.osg-htc.org",
    "ap23.uc.osg-htc.org",
    "ap40.uw.osg-htc.org",
    "ap41.uw.osg-htc.org",
    "ap42.uw.osg-htc.org",
    "ap43.uw.osg-htc.org",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
    "ce1.opensciencegrid.org",
    "comses.sol.rc.asu.edu",
    "condor.scigap.org",
    "descmp3.cosmology.illinois.edu",
    "gremlin.phys.uconn.edu",
    "grid-submitter.icecube.wisc.edu",
    "htcss-dev-ap.ospool.opensciencegrid.org",
    "huxley-osgsub-001.sdmz.amnh.org",
    "lambda06.rowan.edu",
    "login-el7.xenon.ci-connect.net",
    "login-test.osgconnect.net",
    "login.ci-connect.uchicago.edu",
    "login.collab.ci-connect.net",
    "login.duke.ci-connect.net",
    "login.snowmass21.io",
    "login.veritas.ci-connect.net",
    "login04.osgconnect.net",
    "login05.osgconnect.net",
    "mendel-osgsub-001.sdmz.amnh.org",
    "nsgosg.sdsc.edu",
    "os-ce1.opensciencegrid.org",
    "os-ce1.osgdev.chtc.io",
    "osg-moller.jlab.org",
    "osg-prp-submit.nautilus.optiputer.net",
    "osg-solid.jlab.org",
    "osg-vo.isi.edu",
    "ospool-eht.chtc.wisc.edu",
    "scott.grid.uchicago.edu",
    "xd-submit0000.chtc.wisc.edu",
    "testbed",
}


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def connect(
        es_host="localhost:9200",
        es_user="",
        es_pass="",
        es_use_https=False,
        es_ca_certs=None,
        es_url_prefix=None,
        es_timeout=120,
        **kwargs,
    ) -> elasticsearch.Elasticsearch:
    """Create and return an Elasticsearch client.

    Requires either both es_user and es_pass or neither. HTTPS requires es_ca_certs
    or the certifi package to be installed.
    """

    # Split off port from host if included
    if ":" in es_host and len(es_host.split(":")) == 2:
        [es_host, es_port] = es_host.split(":")
        es_port = int(es_port)
    elif ":" in es_host:
        print(f"Ambiguous hostname:port in given host: {es_host}")
        sys.exit(1)
    else:
        es_port = 9200
    es_client = {
        "host": es_host,
        "port": es_port
    }

    # Include username and password if both are provided
    if (not es_user) ^ (not es_pass):
        print("Only one of es_user and es_pass have been defined")
        print("Connecting to Elasticsearch anonymously")
    elif es_user and es_pass:
        es_client["http_auth"] = (es_user, es_pass)

    if es_url_prefix:
        es_client["url_prefix"] = es_url_prefix

    # Only use HTTPS if CA certs are given or if certifi is available
    if es_use_https:
        if es_ca_certs is not None:
            es_client["ca_certs"] = str(es_ca_certs)
        elif importlib.util.find_spec("certifi") is not None:
            pass
        else:
            print("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
            sys.exit(1)
        es_client["use_ssl"] = True
        es_client["verify_certs"] = True

    es_client["timeout"] = es_timeout
    es_client.update(kwargs)

    return elasticsearch.Elasticsearch([es_client])


def get_ospool_aps(include_jupyter_aps: bool = True, pickled_ap_collector_hosts_cache: str | Path | None = None) -> set:
    """Return the set of known OSPool access point (schedd) hostnames.

    Combines three sources: a hardcoded fallback set (OSPOOL_APS), a pickle cache of
    previously seen APs and their collector hosts, and a live query to each OSPool
    collector (requires htcondor). The pickle cache is updated in place after each
    live query.
    """
    ap_collector_hosts_cache = {}
    cached_aps = set()
    if pickled_ap_collector_hosts_cache is not None:
        try:
            with Path(pickled_ap_collector_hosts_cache).open("rb") as f:
                ap_collector_hosts_cache = pickle.load(f)
                for ap, collector_hosts in ap_collector_hosts_cache.items():
                    if not include_jupyter_aps and (ap.lower().startswith("jupyter-notebook-") or ap.lower().startswith("jupyterlab-")):
                        continue
                    if len(set(collector_hosts) & OSPOOL_COLLECTORS) > 0:
                        cached_aps.add(ap)
        except Exception:
            print(f"Could not open {pickled_ap_collector_hosts_cache}, not using CollectorHost cache")
            pass
    current_ospool_aps = set()
    if htcondor is None:
        print("Could not import htcondor, not querying APs")
    else:
        for collector_host in OSPOOL_COLLECTORS:
            try:
                collector = htcondor.Collector(collector_host)
                aps = collector.query(htcondor.AdTypes.Schedd, projection=["Machine", "CollectorHost"])
            except Exception:
                continue
            for ap in aps:
                collector_hosts = set(re.split(r"[\s,]+", ap["CollectorHost"]))
                ap_collector_hosts_cache[ap["Machine"]] = list(collector_hosts)
                if not include_jupyter_aps and (ap["Machine"].lower().startswith("jupyter-notebook-") or ap["Machine"].lower().startswith("jupyterlab-")):
                    continue
                if collector_hosts & OSPOOL_COLLECTORS:
                    current_ospool_aps.add(ap["Machine"])
                if pickled_ap_collector_hosts_cache is not None:
                    try:
                        with Path(pickled_ap_collector_hosts_cache).open("wb") as f:
                            pickle.dump(ap_collector_hosts_cache, f)
                    except Exception:
                        print(f"Could not write to {pickled_ap_collector_hosts_cache}, not saving CollectorHost cache")
                        pass
    return current_ospool_aps | cached_aps | OSPOOL_APS


def print_es_error(d, depth=0, **kwargs):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards":
            print(f"{pre}{k}:", **kwargs)
            print_es_error(v[0], depth=depth+1, **kwargs)
        elif k == "root_cause":
            print(f"{pre}{k}:")
            print_es_error(v[0], depth=depth+1, **kwargs)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_es_error(v, depth=depth+1, **kwargs)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}", **kwargs)
        else:
            print(f"{pre}{k}:\t{v}", **kwargs)


def execute_async(es, query, poll_interval=10):
    response = es.async_search.submit(
        index=query._index,
        body=query.to_dict(),
        wait_for_completion_timeout="0ms",
        keep_on_completion=True,
        keep_alive="1h",
    )
    search_id = response["id"]
    start_time = time.monotonic()

    try:
        while True:
            result = es.async_search.get(id=search_id, keep_alive="1h")
            if not result["is_running"]:
                break
            elapsed = time.monotonic() - start_time
            shards = result.get("response", {}).get("_shards", {})
            total = shards.get("total", 0)
            done = shards.get("successful", 0) + shards.get("skipped", 0)
            if total:
                print(f"Query running... {done}/{total} shards ({done / total * 100:.0f}%) [{elapsed:.0f}s elapsed]", file=sys.stderr)
            else:
                print(f"Query running... [{elapsed:.0f}s elapsed]", file=sys.stderr)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\nInterrupted, cancelling query...", file=sys.stderr)
        es.async_search.delete(id=search_id)
        raise

    elapsed = time.monotonic() - start_time
    print(f"Query complete in {elapsed:.0f}s", file=sys.stderr)
    es.async_search.delete(id=search_id)
    return result["response"]


def load_state_geometry(shapefile_path: str | Path, state: str):
    """Return a Shapely geometry for the given US state abbreviation from a TIGER/Line shapefile."""
    with shapefile.Reader(str(shapefile_path)) as sf:
        for rec, geom in zip(sf.records(), sf.shapes()):
            if rec["STUSPS"] == state:
                return shape(geom)
    raise ValueError(f"State {state!r} not found in {shapefile_path}")


def point_in_state(longitude: float, latitude: float, state_geometry) -> bool:
    """Return True if the given coordinates fall within the state geometry."""
    return Point(longitude, latitude).within(state_geometry)


def short(n: int | float) -> str:
    """Format a large number with K/M suffix for compact display (e.g. 1461000 -> '1.461M')."""
    if not (isinstance(n, int) or isinstance(n, float)):
        print(f"{n} is not numeric")
        return str(n)
    suffixes = ["", "K", "M"]
    out = str(int(n))
    for i, suffix in enumerate(suffixes):
        reduced = n / 10**(3*i)
        if reduced >= 1 and i > 0:
            out = f"{round(reduced, 3):.3f}{suffix}"
    return out

import sys
import math
import time
import argparse
import traceback

from datetime import datetime
from typing import List, Tuple, Dict, Set, Union
from pprint import pprint

try:
    import htcondor2 as htcondor
    import classad2 as classad
except ImportError:
    print("Could not import from htcondor2, falling back to htcondor", file=sys.stderr)
    try:
        import htcondor
        import classad
    except ImportError:
        print("Could not import htcondor", file=sys.stderr)
        raise


DEFAULT_COLLECTOR = "cm-1.ospool.osg-htc.org"
JOB_UNIVERSE = {
    5: "vanilla",
    7: "scheduler",
    8: "mpi",
    9: "grid",
    10: "java",
    11: "parallel",
    12: "local",
    13: "vm",
}
JOB_STATUS = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "transferring output",
    7: "suspended",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector", default=DEFAULT_COLLECTOR, help="Collector (default: %(default)s)")
    parser.add_argument("--skip-ap", default=[], action="append", help="Skip AP (can be set multiple times)")
    return parser.parse_args()


def get_schedds(collector_address: str) -> List[classad.ClassAd]:
    collector = htcondor.Collector(collector_address)
    projection = [
        "Name",
        "Machine",
        "AddressV1",
        "MyAddress",
        "CondorVersion",
        "CondorPlatform",
        "CollectorHost",
    ]
    schedds = collector.query(htcondor.AdTypes.Schedd, projection=projection)
    return schedds


def get_jobs(schedd_address: classad.ClassAd) -> List[classad.ClassAd]:
    schedd = htcondor.Schedd(schedd_address)
    constraint = "RequestGpus > 0"
    projection = [
        "JobUniverse",
        "JobStatus",
        "RequestCpus",
        "RequestMemory",
        "RequestDisk",
        "ProjectName",
        "Owner",
        "EnteredCurrentStatus",
        "NumHolds",
        "NumJobMatches",
        "NumJobStarts",
        "RequestGPUs",
        "RequireGPUs",
        "GPUsMaxCapability",
        "GPUsMinCapability",
        "GPUsMinMemory",
        "GPUsMinRuntime",
        "WantGlidein",
    ]
    jobs = schedd.query(constraint=constraint, projection=projection)
    return jobs


def summarize_jobs(access_point_ad: classad.ClassAd, job_ads: List[classad.ClassAd]) -> Dict[Tuple[Union[int, str]], Union[int, Set[str]]]:
    job_summaries = {}

    for job_ad in job_ads:

        # annoying cleanup (e.g. if someone sets RequestDisk = DiskUsage)
        rm_attrs = set()
        for attr, value in job_ad.items():
            if attr.lower().startswith("request") and isinstance(value, classad.ExprTree):
                value = value.eval()
                if value in {classad.Value.Undefined, classad.Value.Error}:
                    rm_attrs.add(attr)
                else:
                    job_ad[attr] = value
        for attr in rm_attrs:
            del job_ad[attr]

        universe = JOB_UNIVERSE[job_ad["JobUniverse"]]
        status = JOB_STATUS[job_ad["JobStatus"]]
        cpus = max(job_ad.get("RequestCpus", 1), 1)
        memory_gb = math.ceil(job_ad.get("RequestMemory", 1024) / 1024)
        disk_gb = math.ceil(job_ad.get("RequestDisk", 1024*1024) / (1024*1024))
        want_glidein = str(job_ad.get("WantGlidein", "Undefined"))
        project = job_ad.get("ProjectName", "Unknown")
        owner = job_ad.get("Owner", "Unknown")
        gpus = max(job_ad.get("RequestGPUs", 1), 1)

        require_gpus_expr = str(job_ad.get("RequireGPUs", ""))

        require_gpus_attrs = set()
        for gpu_attr in ["DriverVersion", "MaxSupportedVersion", "Capability", "GlobalMemoryMB"]:
            if gpu_attr.lower() in require_gpus_expr.lower():
                require_gpus_attrs.add(gpu_attr)

        gpus_max_capability = job_ad.get("GPUsMaxCapability", "*" if "Capability" in require_gpus_attrs else "")
        gpus_min_capability = job_ad.get("GPUsMinCapability", "*" if "Capability" in require_gpus_attrs else "")
        gpus_min_memory = job_ad.get("GPUsMinMemory", "*" if "GlobalMemoryMB" in require_gpus_attrs else "")
        gpus_min_runtime = job_ad.get("GPUsMinRuntime", "*" if {"DriverVersion", "MaxSupportedVersion"} & require_gpus_attrs else "")

        remarks = set()
        failing_to_match = False
        if status == "idle" and job_ad.get("EnteredCurrentStatus", time.time()) < time.time() - 24*3600:
            remarks.add("IDLE > 1 day")
            if job_ad.get("NumJobMatches", 0) == 0:
                failing_to_match = True

        if status == "held" and job_ad.get("EnteredCurrentStatus", time.time()) < time.time() - 24*3600:
            remarks.add("HELD > 1 day")

        if universe != "vanilla":
            remarks.add(f"Universe = {universe}")

        if "DeviceName" in require_gpus_expr:
            remarks.add("DeviceName in require_gpus expression")

        job_shape = (
            gpus,
            cpus,
            memory_gb,
            disk_gb,
            gpus_min_capability,
            gpus_max_capability,
            gpus_min_memory,
            gpus_min_runtime,
            access_point_ad["Name"],
            access_point_ad["CollectorHost"],
            want_glidein,
            require_gpus_expr,
            project,
            owner,
        )

        job_summary = job_summaries.get(job_shape, {k: 0 for k in JOB_STATUS.values()})
        job_summary[status] += 1

        if "failing_to_match" not in job_summary:
            job_summary["failing_to_match"] = 0
        if failing_to_match:
            job_summary["failing_to_match"] += 1

        if "remarks" not in job_summary:
            job_summary["remarks"] = set()
        if remarks:
            job_summary["remarks"] = job_summary["remarks"] | remarks

        job_summaries[job_shape] = job_summary

    return job_summaries


def write_job_summaries(job_summaries: Dict[Tuple[Union[int, str]], Union[int, Set[str]]], collector: str):
    job_shapes = list(job_summaries.keys())
    job_shapes.sort(reverse=True)
    job_shape_desc = (
        "Request GPUs",
        "Request CPUs",
        "Request Memory GB",
        "Request Disk GB",
        "GPU Min Capability",
        "GPU Max Capability",
        "GPU Memory",
        "GPU Max Runtime",
        "AP",
        "AP Collector",
        "Want Glidein",
        "Require GPUs Expr",
        "Project",
        "Owner",
    )

    columns = [
        "Request GPUs",
        "Request CPUs",
        "Request Memory GB",
        "Request Disk GB",
        "GPU Min Capability",
        "GPU Max Capability",
        "GPU Memory",
        "GPU Max Runtime",
        "AP",
        "Is OSPool AP",
        "Want Glidein",
        "Require GPUs Expr",
        "Project",
        "Owner",
        "Idle",
        "Active",
        "Held",
        "Failing to Match",
        "Remarks",
    ]

    with open(f"ospool_queued_gpu_jobs_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.csv", "w") as f:
        print(",".join(columns), file=f)
        for job_shape in job_shapes:
            job_stats = dict(zip(job_shape_desc, job_shape))
            job_stats["Is OSPool AP"] = ["no", "yes"][collector in job_stats["AP Collector"]]
            job_stats["Idle"] = job_summaries[job_shape]["idle"]
            job_stats["Active"] = job_summaries[job_shape]["running"] + job_summaries[job_shape]["transferring output"]
            job_stats["Held"] = job_summaries[job_shape]["held"]
            job_stats["Failing to Match"] = job_summaries[job_shape]["failing_to_match"]
            job_stats["Remarks"] = "; ".join(job_summaries[job_shape]["remarks"])
            print(",".join([str(job_stats[col]) for col in columns]), file=f)


def main():
    args = parse_args()
    pprint(args)
    job_summaries = {}
    schedd_ads = get_schedds(args.collector)
    for schedd_ad in schedd_ads:
        if schedd_ad["Name"] in args.skip_ap or schedd_ad["Machine"] in args.skip_ap:
            print(f"Skipping {schedd_ad['Name']} ({schedd_ad['Machine']})...")
            continue
        try:
            print(f"Getting ads from {schedd_ad['Name']} ({schedd_ad['Machine']})...")
            job_ads = get_jobs(schedd_ad)
        except Exception:
            print(f"Failed to get ads from {schedd_ad['Name']} ({schedd_ad['Machine']}):")
            print(traceback.format_exc())
        else:
            print(f"Summarizing {len(job_ads)} ads from {schedd_ad['Name']} ({schedd_ad['Machine']})...")
            job_summaries.update(summarize_jobs(schedd_ad, job_ads))
    write_job_summaries(job_summaries, args.collector)


if __name__ == "__main__":
    main()

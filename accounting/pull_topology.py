import pickle
import tempfile
import os
import time
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from pathlib import Path


RESOURCE_SUMMARY_URL = "https://topology.opensciencegrid.org/rgsummary/xml"
TOPOLOGY_PICKLE = Path("topology_site_map.pkl")


MANUAL_MAPPINGS = {
    "GPGrid": "Fermi National Accelerator Laboratory",
    "SURFsara": "SURFsara",
    "IN2P3-CC": "IN2P3",
    "NIKHEF-ELPROD": "Nikhef",
    "ISI_ImageTest": "University of Southern California",
    "WISC-PATH-EP": "University of Wisconsin",
    "GP-ARGO-doane-backfill": "Great Plains Network",
    "GP-ARGO-cameron-backfill": "Great Plains Network",
}


def get_latest_site_map():
    """Gets latest site map from topology XML"""

    xmltree = ET.parse(urlopen(RESOURCE_SUMMARY_URL))
    xmlroot = xmltree.getroot()

    site_map = {k: {"Name": v} for k,v in MANUAL_MAPPINGS.copy().items()}

    for resource_group in xmlroot:

        facility = {
            "Name": resource_group.find("Facility").find("Name").text,
            "InstitutionID": resource_group.find("Facility").find("InstitutionID").text
        }

        # Facility names should map to themsleves
        site_map[facility['Name']] = facility

        # Site names should map to the facility
        site_name = resource_group.find("Site").find("Name").text
        site_map[site_name] = facility

        # Group names should map to the facility
        group_name = resource_group.find("GroupName").text
        site_map[group_name] = facility

        # All resources should map to the facility
        resources = resource_group.find("Resources")
        for resource in resources:
            resource_name = resource.find("Name").text
            site_map[resource_name] = facility

    return site_map


def update_topology_pickle(topology_pickle):
    """Updates (or creates) site map pickle file"""

    site_map = get_latest_site_map()

    # Write atomically
    with tempfile.NamedTemporaryFile(delete=False, dir=str(Path.cwd())) as tf:
        tmpfile = Path(tf.name)
        with tmpfile.open("wb") as f:
            pickle.dump(site_map, f)
            f.flush()
            os.fsync(f.fileno())
    tmpfile.rename(topology_pickle)


def get_site_map(topology_pickle=TOPOLOGY_PICKLE, force_update=False):
    """Returns a recently updated resource to site map"""

    # Update site map if older than a day
    try:
        if (time.time() - topology_pickle.stat().st_mtime > 23*3600) or force_update:
            update_topology_pickle(topology_pickle)
        return pickle.load(topology_pickle.open("rb"))
    except FileNotFoundError:
        update_topology_pickle(topology_pickle)
    return pickle.load(topology_pickle.open("rb"))


if __name__ == "__main__":
    print(get_site_map(force_update=True))

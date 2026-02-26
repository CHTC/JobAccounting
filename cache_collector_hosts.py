import re
import sys
import pickle
from pathlib import Path

try:
    import htcondor2 as htcondor
except ImportError:
    print("Could not import from htcondor2, falling back to htcondor", file=sys.stderr)
    try:
        import htcondor
    except ImportError:
        print("Could not import htcondor", file=sys.stderr)
        raise


CUSTOM_MAPPING = {
    "osg-login2.pace.gatech.edu": {"osg-login2.pace.gatech.edu"},
    "ce1.opensciencegrid.org": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "login-test.osgconnect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "scosg16.jlab.org": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
    "scosgdev16.jlab.org": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
    "submit6.chtc.wisc.edu": {"htcondor-cm-path.osg.chtc.io"},
    "login-el7.xenon.ci-connect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "login.collab.ci-connect.net": {"cm-2.ospool.osg-htc.org", "cm-1.ospool.osg-htc.org"},
    "uclhc-2.ps.uci.edu": {"uclhc-2.ps.uci.edu"},
    "osgsub01.sdcc.bnl.gov": {"scicollector.jlab.org", "osg-jlab-1.t2.ucsd.edu"},
}


def main():

    collector_host = "cm-1.ospool.osg-htc.org"
    collector_hosts = {"cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org"}
    schedd_collector_host_map_pickle = Path("ospool-host-map.pkl")
    schedd_collector_host_map = {}
    if schedd_collector_host_map_pickle.exists():
        try:
            schedd_collector_host_map = pickle.load(open(schedd_collector_host_map_pickle, "rb"))
        except IOError:
            pass
    schedd_collector_host_map.update(CUSTOM_MAPPING)

    collector = htcondor.Collector(collector_host)
    schedds = [ad["Machine"] for ad in collector.locateAll(htcondor.DaemonTypes.Schedd)]

    for schedd in schedds:
        schedd_collector_host_map[schedd] = set()

        for collector_host in collector_hosts:
            collector = htcondor.Collector(collector_host)
            ads = collector.query(
                htcondor.AdTypes.Schedd,
                constraint=f'''Machine == "{schedd.split('@')[-1]}"''',
                projection=["Machine", "CollectorHost"],
            )
            ads = list(ads)
            if len(ads) == 0:
                continue
            if len(ads) > 1:
                print(f'Got multiple Schedd ClassAds for Machine == "{schedd}"')

            # Cache the CollectorHost in the map
            if "CollectorHost" in ads[0]:
                schedd_collector_hosts = set()
                for schedd_collector_host in re.split(r'[, ]+', ads[0]["CollectorHost"]):
                    schedd_collector_host = schedd_collector_host.strip().split(":")[0]
                    if schedd_collector_host:
                        schedd_collector_hosts.add(schedd_collector_host)
                if schedd_collector_hosts:
                    schedd_collector_host_map[schedd] = schedd_collector_hosts
                    break
        else:
            print(f"Did not find Machine == {schedd} in collectors")

    # Update the pickle
    with open(schedd_collector_host_map_pickle, "wb") as f:
        pickle.dump(schedd_collector_host_map, f)

if __name__ == "__main__":
    main()

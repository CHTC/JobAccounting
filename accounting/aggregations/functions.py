import re
import sys
import time
import json
import pickle
import smtplib
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import HTTPError
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from pathlib import Path

try:
    import htcondor
except ModuleNotFoundError:
    htcondor = None
from dns.resolver import query as dns_query


OSDF_DIRECTOR_SERVER_URL = "https://osdf-director.osg-htc.org/api/v1.0/director_ui/servers"
UWDF_DIRECTOR_SERVER_URL = "https://uwdf-director.chtc.wisc.edu/api/v1.0/director_ui/servers"
INSTITUTION_DATABASE_URL = "https://topology-institutions.osg-htc.org/api/institution_ids"
TOPOLOGY_PROJECT_DATA_URL = "https://topology.opensciencegrid.org/miscproject/xml"
TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"

OSPOOL_APS = {
    "ap20.uc.osg-htc.org",
    "ap2007.chtc.wisc.edu",
    "ap21.uc.osg-htc.org",
    "ap22.uc.osg-htc.org",
    "ap23.uc.osg-htc.org",
    "ap40.uw.osg-htc.org",
    "ap41.uw.osg-htc.org",
    "ap42.uw.osg-htc.org",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
    "ce1.opensciencegrid.org",
    "comses.sol.rc.asu.edu",
    "condor.scigap.org",
    "descmp3.cosmology.illinois.edu",
    "gremlin.phys.uconn.edu",
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
    "osg-prp-submit.nautilus.optiputer.net",
    "osg-vo.isi.edu",
    "ospool-eht.chtc.wisc.edu",
    "xd-submit0000.chtc.wisc.edu",
    "testbed",
}
OSPOOL_COLLECTORS = {
    "cm-1.ospool.osg-htc.org",
    "cm-2.ospool.osg-htc.org",
    "flock.opensciencegrid.org",
}
NON_OSPOOL_RESOURCES = {
    "SURFsara",
    "NIKHEF-ELPROD",
    "INFN-T1",
    "IN2P3-CC",
    "UIUC-ICC-SPT",
    "TACC-Frontera-CE2",
}


def get_osdf_director_servers(cache_file=Path("./osdf_director_servers.pickle")) -> dict:
    osdf_director_servers = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            osdf_director_servers = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(osdf_director_servers) > 0 and cache_file.stat().st_mtime > time.time() - 1200:
        return osdf_director_servers

    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(OSDF_DIRECTOR_SERVER_URL) as f:
                for server in json.load(f):
                    for url in ["url", "authUrl", "webUrl", "brokerUrl"]:
                        if url in server and len(server[url]) > 0:
                            osdf_director_servers[server[url]] = server
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries and len(osdf_director_servers) == 0:
                raise
        else:
            break

    pickle.dump(osdf_director_servers, cache_file.open("wb"))
    return osdf_director_servers


def get_uwdf_director_servers(cache_file=Path("./uwdf_director_servers.pickle")) -> dict:
    uwdf_director_servers = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            uwdf_director_servers = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(uwdf_director_servers) > 0 and cache_file.stat().st_mtime > time.time() - 1200:
        return uwdf_director_servers

    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(UWDF_DIRECTOR_SERVER_URL) as f:
                for server in json.load(f):
                    for url in ["url", "authUrl", "webUrl", "brokerUrl"]:
                        if url in server and len(server[url]) > 0:
                            uwdf_director_servers[server[url]] = server
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries and len(uwdf_director_servers) == 0:
                raise
        else:
            break

    pickle.dump(uwdf_director_servers, cache_file.open("wb"))
    return uwdf_director_servers


def get_institution_database(cache_file=Path("./institution_database.pickle")) -> dict:
    institution_db = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            institution_db = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(institution_db) > 0 and cache_file.stat().st_mtime > time.time() - 1200:
        return institution_db

    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(INSTITUTION_DATABASE_URL) as f:
                for institution in json.load(f):
                    institution_id = institution.get("id")
                    if not institution_id:
                        continue
                    institution_id_short = institution_id.split("/")[-1]
                    institution["id_short"] = institution_id_short
                    institution_db[institution_id] = institution
                    institution_db[institution_id_short] = institution

                    # OSG_INSTITUTION_IDS mistakenly had the ROR IDs before ~2024-11-07,
                    # so we map those too (as long as they don't conflict with OSG IDs)
                    ror_id_short = (institution.get("ror_id") or "").split("/")[-1]
                    if ror_id_short and ror_id_short not in institution_db:
                        institution_db[ror_id_short] = institution
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries and len(institution_db) == 0:
                raise
        else:
            break

    pickle.dump(institution_db, cache_file.open("wb"))
    return institution_db


def get_topology_project_data(cache_file=Path("./topology_project_data.pickle")) -> dict:
    projects_data = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            projects_data = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(projects_data) > 0 and cache_file.stat().st_mtime > time.time() - 1200:
        return projects_data

    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_PROJECT_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries and len(projects_data) == 0:
                raise
        else:
            break
    else:  # Return cached data if we can't contact topology
        return projects_data

    projects = xmltree.getroot()
    projects_data = {
        "Unknown": {
            "name": "Unknown",
            "pi": "Unknown",
            "pi_institution": "Unknown",
            "field_of_science": "Unknown",
        }
    }

    institution_db = get_institution_database()

    for project in projects:
        project_map = {}
        project_institution_id = project.find("InstitutionID").text

        if project_institution_id in institution_db:
            project_institution = institution_db[project_institution_id]["name"]
        else:
            project_institution = project.find("Organization").text

        project_map["name"] = project.find("Name").text
        project_map["id"] = project.find("ID").text
        project_map["institution"] = project_institution
        project_map["institution_id"] = project_institution_id
        project_map["field_of_science"] = project.find("FieldOfScience").text
        project_map["field_of_science_id"] = project.find("FieldOfScienceID").text
        projects_data[project_map["name"].lower()] = project_map.copy()

    pickle.dump(projects_data, cache_file.open("wb"))
    return projects_data


def get_topology_resource_data(cache_file=Path("./topology_resource_data.pickle")) -> dict:
    resources_data = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            resources_data = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(resources_data) >  0 and cache_file.stat().st_mtime > time.time() - 1200:
            return resources_data

    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries and len(resources_data) == 0:
                raise
        else:
            break
    else:  # Returned cached data if we can't contact topology
        return resources_data

    resource_groups = xmltree.getroot()
    resources_data = {
        "Unknown": {
            "name": "Unknown",
            "institution": "Unknown",
        }
    }

    institution_db = get_institution_database()

    for resource_group in resource_groups:
        resource_map = {}

        resource_institution_id = resource_group.find("Facility").find("InstitutionID").text
        if resource_institution_id in institution_db:
            resource_institution = institution_db[resource_institution_id]["name"]
        else:
            resource_institution = resource_group.find("Facility").find("Name").text

        resource_map["institution"] = resource_institution
        resource_map["institution_id"] = resource_institution_id

        resource_group_name = resource_group.find("GroupName").text
        resource_map["group_name"] = resource_group_name

        resource_site_name = resource_group.find("Site").find("Name").text
        resource_map["site_name"] = resource_site_name
        resource_map["name"] = resource_site_name

        resources = resource_group.find("Resources")
        for resource in resources:
            resource_name = resource.find("Name").text
            resource_map["resource_name"] = resource_name
            resource_map["name"] = resource_name
            resources_data[resource_name.lower()] = resource_map.copy()
        resources_data[resource_group_name.lower()] = resource_map.copy()
        resources_data[resource_site_name.lower()] = resource_map.copy()

    pickle.dump(resources_data, cache_file.open("wb"))
    return resources_data


def get_ospool_aps(include_jupyter_aps=True, pickled_ap_collector_hosts_cache=None) -> set:
    cached_aps = set()
    if pickled_ap_collector_hosts_cache is not None:
        with Path(pickled_ap_collector_hosts_cache).open("rb") as f:
            ap_collector_hosts_cache = pickle.load(f)
            for ap, collector_hosts in ap_collector_hosts_cache.items():
                if not include_jupyter_aps and (ap.lower().startswith("jupyter-notebook-") or ap.lower().startswith("jupyterlab-")):
                    continue
                if len(set(collector_hosts) & OSPOOL_COLLECTORS) > 0:
                    cached_aps.add(ap)
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
                if not include_jupyter_aps and (ap["Machine"].lower().startswith("jupyter-notebook-") or ap["Machine"].lower().startswith("jupyterlab-")):
                    continue
                if set(re.split(r"[\s,]+", ap["CollectorHost"])) & OSPOOL_COLLECTORS:
                    current_ospool_aps.add(ap["Machine"])
    return current_ospool_aps | cached_aps | OSPOOL_APS


def _smtp_mail(msg, recipient, smtp_server="", smtp_username=None, smtp_password=""):
    sent = False
    result = None
    tries = 0
    sleeptime = 0
    smtp_args = (smtp_server, 0)
    try:
        smtp_args = (smtp_server.split(":")[0], int(smtp_server.split(":")[1]),)
    except (ValueError, IndexError):
        pass
    while tries < 3 and sleeptime < 600:
        try:
            if smtp_username is None:
                smtp = smtplib.SMTP(*smtp_args)
            else:
                smtp = smtplib.SMTP_SSL(*smtp_args)
                smtp.login(smtp_username, smtp_password)
        except Exception:
            print(f"Could not connect to {smtp_server}", file=sys.stderr)
            continue

        try:
            result = smtp.sendmail(msg["From"], recipient, msg.as_string())
            if len(result) > 0:
                print(f"Could not send email to {recipient} using {smtp_server}:\n{result}", file=sys.stderr)
            else:
                sent = True
        except Exception as err:
            print(f"Could not send to {recipient} using {smtp_server}", file=sys.stderr)
            print(str(err), file=sys.stderr)
        finally:
            try:
                smtp.quit()
            except smtplib.SMTPServerDisconnected:
                pass
        if sent:
            break

        sleeptime = int(min(30 * 1.5**tries, 600))
        print(f"Sleeping for {sleeptime} seconds before retrying servers", file=sys.stderr)
        time.sleep(sleeptime)
        tries += 1

    else:
        print(f"Failed to send email after {tries} loops", file=sys.stderr)

    return sent


def send_email(
        subject,
        from_addr,
        to_addrs=[],
        text="",
        html="",
        cc_addrs=[],
        bcc_addrs=[],
        reply_to_addr=None,
        attachments=[],
        smtp_server=None,
        smtp_username=None,
        smtp_password_file=None,
        **kwargs):
    if len(to_addrs) == 0:
        print("ERROR: No recipients in the To: field, not sending email", file=sys.stderr)
        return

    msg = MIMEMultipart("mixed")
    if len(html) > 0 and len(text) > 0:
        msg_text = MIMEMultipart("alternative")
        msg_text.attach(MIMEText(html, "html"))
        msg_text.attach(MIMEText(text, "plain"))
        msg.attach(msg_text)
    elif len(html) > 0:
        msg.attach(MIMEText(html, "html"))
    else:
        msg.attach(MIMEText(text, "plain"))

    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if len(cc_addrs) > 0:
        msg["Cc"] = ", ".join(cc_addrs)
    if len(bcc_addrs) > 0:
        msg["Bcc"] = ", ".join(bcc_addrs)
    if reply_to_addr is not None:
        msg["Reply-To"] = reply_to_addr
    msg["Subject"] = subject
    msg["Message-ID"] = f"<{subject}-{time.time()}-{from_addr}>".replace(" ", "-").casefold()
    msg["Date"] = formatdate(localtime=True)

    for fname in attachments:
        fpath = Path(fname)
        part = MIMEBase("application", "octet-stream")
        with fpath.open("rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename = fpath.name)
        msg.attach(part)

    if smtp_server is not None:
        recipient = list(set(to_addrs + cc_addrs + bcc_addrs))
        smtp_password = None
        if smtp_password_file is not None:
            smtp_password = smtp_password_file.open("r").read().strip()
        _smtp_mail(msg, recipient, smtp_server, smtp_username, smtp_password)

    else:
        for recipient in set(to_addrs + cc_addrs + bcc_addrs):
            domain = recipient.split("@")[1]
            sent = False
            for mxi, mx in enumerate(dns_query(domain, "MX")):
                smtp_server = str(mx).split()[1][:-1]

                try:
                    sent = _smtp_mail(msg, recipient, smtp_server)
                except Exception:
                    continue
                if sent:
                    break

                sleeptime = int(min(30 * 1.5**mxi, 600))
                print(f"Sleeping for {sleeptime} seconds before trying next server", file=sys.stderr)
                time.sleep(sleeptime)

            else:
                print("Failed to send email after trying all servers", file=sys.stderr)

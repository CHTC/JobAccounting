import sys
from datetime import datetime, timedelta
import logging
import csv
import smtplib
import time
import pickle
import json
import xml.etree.ElementTree as ET
import re

from urllib.request import urlopen
from urllib.error import HTTPError
from pathlib import Path
from math import ceil
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders

import htcondor

from dns.resolver import query as dns_query

INSTITUTION_DATABASE_URL = "https://topology-institutions.osg-htc.org/api/institution_ids"
TOPOLOGY_PROJECT_DATA_URL = "https://topology.opensciencegrid.org/miscproject/xml"
TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"


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
        "UNKNOWN": {
            "name": "UNKNOWN",
            "id": "UNKNOWN",
            "institution": "UNKNOWN",
            "field_of_science": "UNKNOWN",
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
        "UNKNOWN": {
            "name": "UNKNOWN",
            "institution": "UNKNOWN",
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


def get_ospool_collectors() -> list:
    ospool_collectors = ["cm-1.ospool.osg-htc.org", "cm-2.ospool.osg-htc.org", "flock.opensciencegrid.org"]
    return ospool_collectors


def get_ospool_aps(cache_file=Path("./ospool-host-map.pkl")) -> list:
    ospool_collectors = set(get_ospool_collectors())
    aps_collectors = {}

    # Use cache if less than 20 minutes old
    if cache_file.exists():
        try:
            aps_collectors = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
    if len(aps_collectors) > 0 and cache_file.stat().st_mtime > time.time() - 1200:
        return [ap for ap, ap_collectors in aps_collectors.items() if set(ap_collectors) & ospool_collectors]

    for ospool_collector in ospool_collectors:
        if ospool_collector in {"flock.opensciencegrid.org"}:
            continue
        collector = htcondor.Collector(ospool_collector)
        try:
            ads = collector.query(
                htcondor.AdTypes.Schedd,
                projection=["Name", "CollectorHost"],
            )
        except htcondor.HTCondorIOError:
            continue

        for ad in ads:
            if "Name" not in ad or "CollectorHost" not in ad:
                continue
            ap = ad["Name"]
            aps_collectors[ap] = re.split(r'[, ]+', ad["CollectorHost"])

    pickle.dump(aps_collectors, cache_file.open("wb"))
    return [ap for ap, ap_collectors in aps_collectors.items() if set(ap_collectors) & ospool_collectors]


def get_non_fairshare_resources() -> list:
    non_fairshare_resources = [
        "SURFsara",
        "NIKHEF-ELPROD",
        "INFN-T1",
        "IN2P3-CC",
        "UIUC-ICC-SPT",
        "TACC-Frontera-CE2",
    ]
    return non_fairshare_resources


def get_timestamps(report_period, start_ts, end_ts):
    if report_period == "custom" and None in (start_ts, end_ts):
        raise ValueError("START_TS and END_TS cannot be None when REPORT_PERIOD is custom")
    if report_period != "custom" and None not in (start_ts, end_ts):
        raise ValueError("REPORT_PERIOD must be custom if both START_TS and END_TS defined")

    if report_period == "custom":
        return (start_ts, end_ts)

    if start_ts is not None:
        sign = 1
        ts0 = start_ts
    elif end_ts is not None:
        sign = -1
        ts0 = end_ts
    else:  # Default ending timestamp to midnight today
        end_dt = datetime.now()
        end_dt = datetime(end_dt.year, end_dt.month, end_dt.day)
        sign = -1
        ts0 = end_dt.timestamp()

    if report_period == "daily":
        ts1 = ts0 + sign*timedelta(days=1).total_seconds()
    elif report_period == "weekly":
        ts1 = ts0 + sign*timedelta(days=7).total_seconds()
    elif report_period == "monthly":
        dt0 = datetime.fromtimestamp(ts0)
        try:
            dt1 = dt0.replace(month=dt0.month + sign)
        except ValueError:
            if sign > 0:
                dt1 = dt0.replace(year=dt0.year+1, month=1)
            else:
                dt1 = dt0.replace(year=dt0.year-1, month=12)
        ts1 = dt1.timestamp()
    else:
        raise ValueError("REPORT_PERIOD is not one of [daily, weekly, monthly]")

    return tuple(sorted([int(ts0), int(ts1)]))


def write_csv(table, filter_name, table_name, start_ts, report_period, csv_dir, **kwargs):
    # Write a CSV to file named:
    #   1. Filter name (e.g. job history)
    #   2. Table name (e.g. Users, Schedds, Projects)
    #   3. Date (Starting + Reporting Period or Starting + Ending)
    filter_name = filter_name.replace(" ", "-")
    table_name = table_name.replace(" ", "-")

    # Format date(s)
    start = datetime.fromtimestamp(start_ts)
    if report_period in ["daily", "weekly", "monthly"]:
        start_date = start.strftime("%Y-%m-%d")
        date = f"{report_period}_{start_date}"
    else:
        end = datetime.fromtimestamp(kwargs["end_ts"])
        start_date = start.strftime("%Y%m%d%H%M%S")
        end_date = end.strftime("%Y%m%d%H%M%S")
        date = f"{start_date}_{end_date}"

    # Set filename and path
    filename = f"{filter_name}_{table_name}_{date}.csv"
    filepath = Path(csv_dir) / filename

    # Write CSV
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        for row in table:
            # Cap floats at 1e-4 precision
            writer.writerow([f"{x:.4f}" if isinstance(x, float) else x for x in row])

    # Return path to CSV
    return filepath


def _smtp_mail(msg, recipient, smtp_server=None, smtp_username=None, smtp_password=None):
    logger = logging.getLogger("accounting.send_email")
    sent = False
    result = None
    tries = 0
    sleeptime = 0
    while tries < 3 and sleeptime < 600:
        try:
            logger.debug(f"Connecting to mailserver {smtp_server}")
            if smtp_username is None:
                smtp = smtplib.SMTP(smtp_server)
            else:
                smtp = smtplib.SMTP_SSL(smtp_server)
                smtp.login(smtp_username, smtp_password)
        except Exception:
            logger.error(f"Could not connect to {smtp_server}")
            continue

        try:
            logger.debug(f"Sending email to {recipient}")
            result = smtp.sendmail(msg["From"], recipient, msg.as_string())
            if len(result) > 0:
                logger.error(f"Could not send email to {recipient} using {smtp_server}:\n{result}")
            else:
                sent = True
        except Exception:
            logger.exception(f"Could not send to {recipient} using {smtp_server}")
        finally:
            try:
                smtp.quit()
            except smtplib.SMTPServerDisconnected:
                pass
        if sent:
            break

        sleeptime = int(min(30 * 1.5**tries, 600))
        logger.info(f"Sleeping for {sleeptime} seconds before retrying servers")
        time.sleep(sleeptime)
        tries += 1

    else:
        logger.error(f"Failed to send email after {tries} loops")

    return sent


def send_email(subject, from_addr, to_addrs, cc_addrs, bcc_addrs, reply_to_addr, html,
               table_files=[], smtp_server=None, smtp_username=None, smtp_password_file=None, **kwargs):
    logger = logging.getLogger("accounting.send_email")
    if len(to_addrs) == 0:
        logger.error("No recipients in the To: field, not sending email")
        print("ERROR: No recipients in the To: field, not sending email", file=sys.stderr)
        return

    msg = MIMEMultipart()
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

    msg.attach(MIMEText(html, "html"))

    for fname in table_files:
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
                logger.info(f"Sleeping for {sleeptime} seconds before trying next server")
                time.sleep(sleeptime)

            else:
                logger.error("Failed to send email after trying all servers")


def get_job_units(cpus, memory_gb, disk_gb):
    unit_size = {
        "cpus": 1,
        "memory_gb": 4,
        "disk_gb": 4,
    }
    job_units = max([
        max(1, cpus/unit_size["cpus"]),
        max(0, memory_gb/unit_size["memory_gb"]),
        max(0, disk_gb/unit_size["disk_gb"])
    ])
    return ceil(job_units)

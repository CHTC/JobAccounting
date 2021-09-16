import os
import sys
from datetime import datetime, timedelta
import logging
import csv
import smtplib
import dns.resolver
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email import encoders


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


def send_email(subject, from_addr, to_addrs, cc_addrs, bcc_addrs, reply_to_addr, html, table_files, **kwargs):
    if len(to_addrs) == 0:
        logging.error("No recipients in the To: field, not sending email")
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

    msg.attach(MIMEText(html, "html"))
    
    for fname in table_files:
        fpath = Path(fname)
        part = MIMEBase("application", "octet-stream")
        with fpath.open("rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename = fpath.name)
        msg.attach(part)

    for recipient in to_addrs + cc_addrs + bcc_addrs:
        domain = recipient.split("@")[1]
        sent = False
        result = None
        for mx in dns.resolver.query(domain, "MX"):
            mailserver = str(mx).split()[1][:-1]
            try:
                smtp = smtplib.SMTP(mailserver)
                result = smtp.sendmail(from_addr, recipient, msg.as_string())
                smtp.quit
            except Exception:
                if result is not None:
                    logging.error(f"Got result: {result}")
                logging.exception(f"Could not send to {recipient} using {mailserver}")
            else:
                sent = True
            if sent:
                break

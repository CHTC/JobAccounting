import argparse
import traceback
import json
import re
import importlib
import sys

from datetime import datetime, timedelta
from pathlib import Path

import elasticsearch


LOG_SCHEDD_ENTRY_RE = re.compile(r"(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(,\d+)? : (?P<logger>[^:]+):(?P<log_level>\S+) - Schedd\s+(?P<schedd>\S+)\s+history:\s+response count:\s+(?P<num_ads>\d+);\s+last completion\s+(?P<last_job_seen>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2});")
LOG_SCHEDD_FAILURE_RE = re.compile(r"(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(,\d+)? : (?P<logger>[^:]+):(?P<log_level>\S+) - Failed to query schedd for job history:\s+(?P<schedd>\S+)")
LOG_SCHEDD_TIMEOUT_RE = re.compile(r"(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(,\d+)? : (?P<logger>[^:]+):(?P<log_level>\S+) - Daemon\s+(?P<schedd>\S+)\s+history timed out")
LOG_CONVERT_FAILURE_RE = re.compile(r"(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(,\d+)? : (?P<logger>[^:]+):(?P<log_level>\S+) - Failure when converting document on\s+(?P<schedd>\S+)\s+history")
LOG_HTCONDOR_ERROR_RE = re.compile(r"htcondor.HTCondorIOError:\s+(?P<error>.*)")
LOG_TIMEOUT_ERROR_RE = re.compile(r"multiprocessing\.context\.TimeoutError")
LOG_ES_CONNECTION_ERROR_ER = re.compile(r"(ConnectionRefusedError|urllib3\.exceptions\.NewConnectionError|elasticsearch\.exceptions\.ConnectionError)")
LINE_REGEXES = {
    "schedd_entry": LOG_SCHEDD_ENTRY_RE,
    "schedd_failure": LOG_SCHEDD_FAILURE_RE,
    "schedd_timeout": LOG_SCHEDD_TIMEOUT_RE,
    "convert_failure": LOG_CONVERT_FAILURE_RE,
}


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", type=Path)
    parser.add_argument("--checkpoint", type=Path, help="JSON file to keep track of last line read for a log file")
    parser.add_argument("--start", type=valid_date, help="Start date (defaults to yesterday)")
    parser.add_argument("--end", type=valid_date, help="End date (defaults to today)")
    parser.add_argument("--dry-run", action="store_true", help="Just print out stats, do not update file or ES")
    return parser.parse_args()


def load_checkpoints(ckpt_file: Path) -> dict:
    checkpoints = {}
    try:
        with ckpt_file.open("r") as f:
            checkpoints = json.load(f)
    except IOError:
        print(f"WARNING: Could not open checkpoint file {ckpt_file}, creating new checkpoint file")
    return checkpoints


def get_checkpoint(ckpt_file: Path, log_file: Path, start_date: datetime) -> int:
    path = log_file.as_posix()
    timestamp = str(int(start_date.timestamp()))
    checkpoints = load_checkpoints(ckpt_file)
    ckpt = checkpoints.get(path, {}).get(timestamp, 0)
    if ckpt == 0:
        print(f"WARNING: Could not find checkpoint for {log_file} at {start_date}, reading from beginning of file")
    return ckpt


def store_checkpoint(ckpt: int, ckpt_file: Path, log_file: Path, end_date: datetime):
    path = log_file.as_posix()
    timestamp = str(int(end_date.timestamp()))
    checkpoints = load_checkpoints(ckpt_file)
    if path not in checkpoints:
        checkpoints[path] = {}
        checkpoints[path][timestamp] = ckpt
    elif timestamp not in checkpoints[path]:
        checkpoints[path][timestamp] = ckpt
    elif ckpt > checkpoints[path][timestamp]:
        checkpoints[path][timestamp] = ckpt
    else:  # no update needed
        return
    with ckpt_file.open("w") as f:
        json.dump(checkpoints, f, indent=2)


def connect(es_host, es_user, es_pass, es_use_https, es_ca_certs, **kwargs) -> elasticsearch.Elasticsearch:
    # Returns Elasticsearch client

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
    if es_user is None and es_pass is None:
        pass
    elif (es_user is None) != (es_pass is None):
        print("Only one of es_user and es_pass have been defined")
        print("Connecting to Elasticsearch anonymously")
    else:
        es_client["http_auth"] = (es_user, es_pass)

    if "es_url_prefix" in kwargs:
        es_client["url_prefix"] = kwargs.pop("es_url_prefix")

    # Only use HTTPS if CA certs are given or if certifi is available
    if es_use_https:
        if es_ca_certs is not None:
            print(f"Using CA from {es_ca_certs}")
            es_client["ca_certs"] = es_ca_certs
        elif importlib.util.find_spec("certifi") is not None:
            print("Using CA from certifi library")
        else:
            print("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
            sys.exit(1)
        es_client["use_ssl"] = True
        es_client["verify_certs"] = True

    return elasticsearch.Elasticsearch([es_client], **kwargs)


def make_index(client, index):
    index_client = elasticsearch.client.IndicesClient(client)
    if index_client.exists(index):
        return

    properties = {
        "timestamp_start": {"type": "date", "format": "epoch_second"},
        "timestamp_end": {"type": "date", "format": "epoch_second"}
    }
    dynamic_templates = [
        {
            "strings_as_keywords": {
                "match_mapping_type": "string",
                "mapping": {"type": "keyword", "norms": "false", "ignore_above": 512},
            }
        },
    ]
    mappings = {
        "dynamic_templates": dynamic_templates,
        "properties": properties,
        "date_detection": False,
        "numeric_detection": True,
    }

    body = json.dumps({"mappings": mappings})
    client.indices.create(index=index, body=body)


def push_stats_to_es(schedd_stats, index, **es_args):
    es = connect(**es_args)
    print(f"Making index {index}")
    make_index(es, index)
    for schedd, stats in schedd_stats.items():
        print(f"Pushing data for AP {schedd}")
        doc_id = f"schedd-stats_{schedd}_{stats.get('timestamp_start')}_{stats.get('timestamp_end')}"
        try:
            es.index(index=index, id=doc_id, body=stats)
        except Exception as err:
            print(f"Got error {str(err)} when pushing {doc_id}, continuing...")


def main():
    args = parse_args()
    if args.start is None and args.end is None:
        args.start = (datetime.now()-timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        args.end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    elif args.start is None:
        args.start = (args.end-timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif args.end is None:
        args.end = (args.start+timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    ckpt = 0
    if args.checkpoint:
        ckpt = get_checkpoint(args.checkpoint, args.log_file, args.start)

    in_traceback = False
    skip_traceback = False
    at_start = False
    last_ckpt = 0
    start_date = args.start.strftime(r"%Y-%m-%d")
    current_traceback = []
    last_error = {}
    successes = []
    errors = []
    with args.log_file.open("rb") as f:
        f.seek(ckpt)
        for line in f:
            line = line.decode()

            if at_start:
                pass
            elif not at_start and not line.startswith(start_date):
                last_ckpt = f.tell()
                continue
            elif not at_start and line.startswith(start_date):
                at_start = True
                if ckpt == 0:
                    store_checkpoint(last_ckpt, args.checkpoint, args.log_file, args.start)

            if line.startswith("Traceback"):
                in_traceback = True
                current_traceback = []
                current_traceback.append(line)
                last_ckpt = f.tell()
                continue
            elif in_traceback and (line.strip() == "" or line.startswith("elasticsearch.exceptions.ConnectionError")):
                in_traceback = False
                if skip_traceback or line.startswith("elasticsearch.exceptions.ConnectionError"):  # skip ES connection errors
                    if line.startswith("elasticsearch.exceptions.ConnectionError"):
                        current_traceback.append(line)
                    skip_traceback = False
                    last_ckpt = f.tell()
                    continue
                skip_traceback = False
                #print(f"Got trackback: {current_traceback}")
                m = LOG_HTCONDOR_ERROR_RE.match(current_traceback[-1])
                if m:
                    last_error["htcondor_error"] = m.group("error")
                elif LOG_TIMEOUT_ERROR_RE.match(current_traceback[-1]):
                    last_error["htcondor_error"] = "Timed out while reading history"
                else:
                    last_error["htcondor_error"] = current_traceback[-1].strip()
                errors.append(last_error)
                last_ckpt = f.tell()
                continue
            elif in_traceback:
                if LOG_ES_CONNECTION_ERROR_ER.match(line.strip()):  # skip ES connection errors
                    skip_traceback = True
                current_traceback.append(line)
                last_ckpt = f.tell()
                continue

            try:
                if datetime.strptime(line[:10], r"%Y-%m-%d") >= args.end:
                    break
            except ValueError:
                pass

            for line_type, line_re in LINE_REGEXES.items():
                m = line_re.match(line)
                if m:
                    #print(f"Found {line_type}: {m.groupdict()}")
                    if line_type in {"schedd_failure", "schedd_timeout", "convert_failure"}:
                        last_error = m.groupdict()
                    else:
                        successes.append(m.groupdict())
                    break

            last_ckpt = f.tell()

    if not args.dry_run:
        store_checkpoint(last_ckpt, args.checkpoint, args.log_file, args.end)

    schedd_stats = {}
    for success in successes:
        schedd = success["schedd"]
        if not schedd in schedd_stats:
            schedd_stats[schedd] = {"schedd": schedd, "num_seen": 0, "num_failures": 0, "num_ads": 0, "failures": []}
        ads = int(success["num_ads"])
        schedd_stats[schedd]["num_seen"] += 1
        schedd_stats[schedd]["num_ads"] += int(success["num_ads"])
    for error in errors:
        schedd = error["schedd"]
        if not schedd in schedd_stats:
            schedd_stats[schedd] = {"schedd": schedd, "num_seen": 0, "num_failures": 0, "num_ads": 0, "failures": []}
        schedd_stats[schedd]["num_failures"] += 1
        schedd_stats[schedd]["failures"].append(error.get("htcondor_error", "Unknown/non-HTCondor error"))
    for stats in schedd_stats.values():
        stats["period_days"] = (args.end - args.start).days
        stats["timestamp_start"] = int(args.start.timestamp())
        stats["timestamp_end"] = int(args.end.timestamp())
        failures = stats.pop("failures")
        if len(failures) > 0:
            stats["most_common_failure"] = max(set(failures), key=failures.count)

    print(json.dumps(schedd_stats, indent=2))

    if not args.dry_run:
        if Path("tiger-es-ap-stats-config.json").exists():
            print("Pushing AP data to Tiger Elasticsearch")
            tiger_args = json.load(Path("tiger-es-ap-stats-config.json").open("r"))
            push_stats_to_es(schedd_stats, "adstash-ospool-schedd-stats-000001", **tiger_args)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

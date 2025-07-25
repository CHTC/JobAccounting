import os
import sys
import argparse
import json

from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from functions import send_email

import requests
import yaml


NOW = datetime.now()


EMAIL_ARGS = {
    "--from": {"dest": "from_addr", "default": "no-reply@chtc.wisc.edu"},
    "--reply-to": {"default": "ospool-reports@g-groups.wisc.edu"},
    "--to": {"action": "append", "default": []},
    "--error-to": {"action": "append", "default": []},
    "--cc": {"action": "append", "default": []},
    "--bcc": {"action": "append", "default": []},
    "--smtp-server": {},
    "--smtp-username": {},
    "--smtp-password-file": {"type": Path}
}


DEFAULT_COMPARISON_PERIODS = {
    "half hour": {"diff": 1800,      "err": 600},
    "hour":      {"diff": 3600,      "err": 900},
    "day":       {"diff": 24*3600,   "err": 3600},
    "week":      {"diff": 7*24*3600, "err": 24*3600},
}


def log(msg: str, level="INFO", **kwargs):
    out = {
        "timestamp": datetime.now().isoformat(),
        "level": level.upper(),
        "message": msg,
        **kwargs
    }
    print(json.dumps(out), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--counts-file",
        required=True,
        type=Path,
        help="(JSON-formatted) file to store recent index counts"
    )
    parser.add_argument(
        "--poll-config",
        required=True,
        type=Path,
        help="YAML-formatted file containing config for each index to be polled"
    )
    parser.add_argument(
        "--push-config",
        type=Path,
        help="(Optional) YAML-formatted file containing config for the index to push data to"
    )
    email_args = parser.add_argument_group("email-related options")
    for name, properties in EMAIL_ARGS.items():
        email_args.add_argument(name, **properties)
    return parser.parse_args()


def read_config_file(config_path: Path) -> dict:
    try:
        config = yaml.safe_load(config_path.open())
    except OSError as e:
        log(f"Failed to open config file {config_path} due to {e.errno}: {e.strerror}")
        sys.exit(1)
    except yaml.YAMLError:
        log(f"Failed to parse config file {config_path} as YAML")
        sys.exit(1)
    log(f"Loaded config from {config_path}")
    return config


def fetch_index_counts(index_name: str, index_config: dict) -> dict:
    base_url = f"http{'s' if index_config.get('es_use_https') else ''}://{index_config.get('es_host', 'localhost:9200')}/{index_config.get('es_url_prefix', '')}".rstrip("/")
    get_kwargs = {}
    if index_config.get("es_user"):
        username = index_config.get("es_user")
        password = index_config.get("es_pass")
        if not password:
            try:
                password = open(index_config.get("es_password_file")).read().rstrip()
            except Exception as err:
                log(f"Failed to get password for ES user {username}: {str(err)}")
                return {}
        get_kwargs["auth"] = (username, password)
    if index_config.get("es_ca_cert_file"):
        get_kwargs["verify"] = index_config["es_ca_cert_file"]

    # GET _cat/indices/<alias>?h=docs.count,store.size,pri.store.size&bytes=b
    endpoint = f"_cat/indices/{index_config.get('es_index', 'htcondor-000001')}"
    params = {
        "h": "docs.count,store.size,pri.store.size",
        "bytes": "b"  # get raw byte integers instead of suffixed byte strings
    }
    ts = int(datetime.now().timestamp())
    result = requests.get(f"{base_url}/{endpoint}", params=params, **get_kwargs)
    try:
        result.raise_for_status()
    except requests.HTTPError:
        log(f"Failed to get counts for {index_name}",
            details=f"GET {base_url}/{endpoint} returned {result.status_code}: {result.reason}",
            level="ERROR")
        return {}

    # sum up all the indices
    cols = params["h"].split(",")
    total = {col: 0 for col in cols}
    for line in result.text.split("\n"):
        if not line.strip():
            continue
        index = dict(zip(cols, [int(v) for v in line.strip().split()]))
        for col in cols:
            total[col] += index[col]

    counts = {
        "@timestamp": ts,
        "docs_count": total["docs.count"],
        "store_size": total["store.size"],
        "pri_store_size": total["pri.store.size"]
    }

    log(f"{index_name} is at {counts['docs_count']:,d} docs")
    return counts


def upload_indices_counts(indices_counts: dict, counts_config: dict) -> set:
    uploaded_indices_counts = set()
    base_url = f"http{'s' if counts_config.get('es_use_https') else ''}://{counts_config.get('es_host', 'localhost:9200')}/{counts_config.get('es_url_prefix', '')}".rstrip("/")
    put_kwargs = {}
    if counts_config.get("es_user"):
        username = counts_config.get("es_user")
        password = counts_config.get("es_pass")
        if not password:
            try:
                password = open(counts_config.get("es_password_file")).read().rstrip()
            except Exception as err:
                log(f"Failed to get password for ES user {username}: {str(err)}")
                return uploaded_indices_counts
        put_kwargs["auth"] = (username, password)
    if counts_config.get("es_ca_cert_file"):
        put_kwargs["verify"] = counts_config["es_ca_cert_file"]

    for index_name, index_counts in indices_counts.items():

        doc = index_counts.copy()
        doc["index_name"] = index_name
        doc_id = f"{index_name}_{index_counts['@timestamp']}"

        # PUT <counts_index>/_create/<_id>
        endpoint = f"{counts_config.get('es_index', 'index-counts')}/_create/{doc_id}"
        result = requests.put(f"{base_url}/{endpoint}", json=doc, **put_kwargs)
        try:
            result.raise_for_status()
        except requests.HTTPError:
            log(f"Failed to put counts for {index_name}",
                details=f"PUT {base_url}/{endpoint} returned {result.status_code}: {result.reason}",
                level="ERROR")
        else:
            uploaded_indices_counts.add(index_name)
            log(f"Put {index_name} counts to {counts_config.get('es_index', 'index-counts')}")

    return uploaded_indices_counts


def read_indices_counts_json(db_path: Path) -> dict | None:
    try:
        indices_counts_db = json.load(db_path.open())
    except FileNotFoundError:
        log(f"Could not find counts file at {db_path}, assuming we need a new one")
        return {}
    except OSError as e:
        log(f"Failed to open the counts file at {db_path} due to {e.errno}: {e.strerror}", level="ERROR")
        return None
    except json.JSONDecodeError:
        log(f"Failed to parse the counts file as JSON at {db_path}", level="ERROR")
        return None
    log(f"Loaded the counts file from {db_path}")
    return indices_counts_db


def write_indices_counts_json(db: dict, db_path: Path) -> bool:
    with NamedTemporaryFile(mode="w", delete=False, dir=str(db_path.parent)) as f:
        tmp_path = Path(f.name)
        try:
            json.dump(db, f, indent=2)
            f.flush()
            os.fsync(f)
        except OSError as e:
            log(f"Failed to write to tmpfile {e.filename} due to {e.errno}: {e.strerror}", level="ERROR")
            return False

    try:
        tmp_path.replace(db_path)
    except OSError as e:
        log(f"Failed to move {tmp_path} to {db_path} due to {e.errno}: {e.strerror}", level="ERROR")
        tmp_path.unlink(missing_ok=True)
        return False

    log(f"Updated the JSON database at {db_path}")
    return True


def compare_counts(counts_db: dict, indices_config: dict) -> dict:
    '''Find the nearest timestamp within some error range to the last half hour, hour, day, or week,
    then compute the difference and rate of change of doc counts and disk sizes for each period.'''

    alert_periods = {}
    for index, index_config in indices_config.items():
        alert_seconds = int(index_config.get("alert_seconds", 3600))
        alert_periods[index] = {"diff": alert_seconds, "err": alert_seconds // 4}

    closest_timestamp = {
        period: {"delta": 2e16, "ts": None}
        for period in DEFAULT_COMPARISON_PERIODS | alert_periods
    }

    now_ts = int(NOW.timestamp())
    for ts in counts_db.keys():
        seconds_ago = now_ts - int(ts)
        for period, threshold in (DEFAULT_COMPARISON_PERIODS | alert_periods).items():
            delta = abs(seconds_ago - threshold["diff"])
            if delta < threshold["err"] and delta < closest_timestamp[period]["delta"]:
                closest_timestamp[period] = {"delta": delta, "ts": ts}

    # don't need the deltas anymore
    closest_timestamp = {k: v["ts"] for k, v in closest_timestamp.items()}

    comparisons = {}
    for period, then_ts in closest_timestamp.items():
        attrs = ["docs_count", "store_size", "pri_store_size"]

        if period in DEFAULT_COMPARISON_PERIODS:
            if then_ts is None:
                log(f"No comparable data from a {period} ago", level="WARN")
                continue
            delta_t = int(now_ts) - int(then_ts)
            now = counts_db[now_ts]
            then = counts_db[then_ts]

            now_indices = set(now.keys())
            then_indices = set(then.keys())
            for missing_index in now_indices ^ then_indices:
                log(f"Index {missing_index} has no comparable numbers from a {period} ago", level="ERROR")

            comparisons[period] = {
                index: {
                    **{attr: now[index][attr] - then[index][attr] for attr in attrs},
                    **{f"{attr}_per_s": (now[index][attr] - then[index][attr])/delta_t for attr in attrs},
                    "bytes_per_doc": (now[index]["pri_store_size"] - then[index]["pri_store_size"])/max(now[index]["docs_count"] - then[index]["docs_count"], 1),
                } for index in now_indices & then_indices
            }

        if period in alert_periods:
            index = period
            if then_ts is None:
                log(f"No comparable data from {alert_periods[period]['diff']} seconds ago for index {index}", level="ERROR")
                continue
            delta_t = int(now_ts) - int(then_ts)
            now = counts_db[now_ts]
            then = counts_db[then_ts]
            if index not in set(then.keys()) & set(now.keys()):
                log(f"No comparable data from {alert_periods[period]['diff']} seconds ago for index {index}", level="ERROR")
                continue

            comparisons[period] = {
                index: {
                    **{attr: now[index][attr] - then[index][attr] for attr in attrs},
                    **{f"{attr}_per_s": (now[index][attr] - then[index][attr])/max(delta_t, 1) for attr in attrs},
                    "bytes_per_doc": (now[index]["pri_store_size"] - then[index]["pri_store_size"])/max(now[index]["docs_count"] - then[index]["docs_count"], 1),
                }
            }

    return comparisons


def main():
    errors = []
    args = parse_args()

    indices_config = read_config_file(args.poll_config)
    indices_counts = {}
    for index_name, index_config in indices_config.items():
        index_counts = fetch_index_counts(index_name, index_config)
        if not index_counts:
            errors.append(f"<li>Failed to get updated counts from index {index_name}</li>")
            continue
        indices_counts[index_name] = index_counts

    counts_db = read_indices_counts_json(args.counts_file)
    counts_comparisons = None
    if counts_db is None:
        errors.append(f"<li>Failed to open counts from {args.counts_file}</li>")
    else:
        counts_db[int(NOW.timestamp())] = indices_counts
        if not write_indices_counts_json(counts_db, args.counts_file):
            errors.append(f"<li>Failed to write updated counts to {args.counts_file}</li>")
        counts_comparisons = compare_counts(counts_db, indices_config)

    if args.push_config:
        counts_config = read_config_file(args.push_config)
        indices_uploaded = upload_indices_counts(indices_counts, counts_config)
        for index in set(indices_config.keys()) - indices_uploaded:
            errors.append(f"<li>Failed to upload updated counts to Elasticsearch for index {index}</li>")

    ingest_errors = False
    for index_name, index_config in indices_config.items():
        if index_name not in counts_comparisons.get(index_name, {}):
            errors.insert(0, f"<li><strong>Index {index_name} could not be compared between now and {int(index_config.get('alert_seconds', 3600)) // 60} minutes ago</strong></li>")
            continue
        counts = counts_comparisons[index_name][index_name]
        if counts["docs_count"] == 0:
            ingest_errors = True
            errors.insert(0, f"<li><strong>Index {index_name} has not any new docs ingested for the past {int(index_config.get('alert_seconds', 3600)) // 60} minutes</strong></li>")

    if errors and args.error_to:
        subject = f"{len(errors)} ES ingest monitor errors at {datetime.now().strftime(r'%Y-%m-%d %H:%M')}"
        if ingest_errors:
            subject = f"!! {len(errors)} ES ingest errors at {datetime.now().strftime(r'%Y-%m-%d %H:%M')}"
        lr = "\n"
        send_email(
            subject=subject,
            html=f"<ol>\n{lr.join(errors)}\n</ol>",
            from_addr=args.from_addr,
            reply_to_addr=args.reply_to,
            to_addrs=args.error_to,
            cc_addrs=[],
            bcc_addrs=[],
            smtp_server=args.smtp_server,
            smtp_username=args.smtp_username,
            smtp_password_file=args.smtp_password_file,
        )

    html = []
    if counts_comparisons and args.to:
        subject = f"Elasticsearch ingest summary at {datetime.now().strftime(r'%Y-%m-%d %H:%M')}"
        html.append("<ul>")
        for period in DEFAULT_COMPARISON_PERIODS:
            if period in counts_comparisons:
                html.append(f"<li>In the past {period}:</li>")
                html.append("<ul>")
                for index, counts in counts_comparisons[period].items():
                    html.append(f"<li>{index} ingested {counts['docs_count']:,} docs and increased by {counts['store_size']/1e6:,.0f} MB</li>")
                html.append("</ul>")
        html.append("</ul>")
        send_email(
            subject=subject,
            html="\n".join(html),
            from_addr=args.from_addr,
            reply_to_addr=args.reply_to,
            to_addrs=args.error_to,
            cc_addrs=[],
            bcc_addrs=[],
            smtp_server=args.smtp_server,
            smtp_username=args.smtp_username,
            smtp_password_file=args.smtp_password_file,
        )


if __name__ == "__main__":
    main()

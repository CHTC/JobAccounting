#!/usr/bin/env python3

import os; os.environ["CONDOR_CONFIG"] = "/dev/null"  # squelch warning
import sys
from datetime import timedelta
import time
import elasticsearch
import accounting
from accounting.push_totals_to_es import push_totals_to_es
import pickle
from traceback import print_exc, print_tb
import logging, logging.handlers
from pathlib import Path

args = accounting.parse_args(sys.argv[1:])

logger = logging.getLogger("accounting")
logger.setLevel(logging.INFO)
if args.debug:
    logger.setLevel(logging.DEBUG)
if args.log_file is not None:
    fh = logging.handlers.RotatingFileHandler(str(args.log_file), backupCount=9, maxBytes=10_000_000)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    fh.setLevel(logger.getEffectiveLevel())
    logger.addHandler(fh)
    logger.info(f"=== {sys.argv[0]} STARTING UP ({' '.join(sys.argv[1:])}) ===")
if not args.quiet:
    sh = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(message)s")
    sh.setFormatter(formatter)
    sh.setLevel(logger.getEffectiveLevel())
    logger.addHandler(sh)


for tries in range(3):
    try:
        logger.info(f"Filtering data using {args.filter.__name__}")
        filtr = args.filter(**vars(args))
        raw_data = filtr.get_filtered_data()
    except elasticsearch.exceptions.ConnectionTimeout:
        logger.info(f"Elasticsearch connection timed out, trying again (try {tries+1})")
        time.sleep(4**(tries+1))
        continue
    break
else:
    logger.error(f"Could not connect to Elasticsearch")
    sys.exit(1)

if args.report_period == "daily":
    last_data_file = Path(f"last_data_{args.filter.__name__}.pickle")
    logger.debug(f"Dumping data to {last_data_file}")
    with last_data_file.open("wb") as f:
        pickle.dump(raw_data, f, pickle.HIGHEST_PROTOCOL)

table_names = list(raw_data.keys())
logger.debug(f"Got {len(table_names)} tables: {', '.join(table_names)}")
csv_files = {}
for table_name in table_names:
    logger.debug(f"Collapsing data for {table_name} table")
    table_data = filtr.merge_filtered_data(filtr.get_filtered_data(), table_name)
    logger.debug(f"{table_name} table has {len(table_data)} rows")
    logger.debug(f"Generating CSV for {table_name}")
    csv_files[table_name] = accounting.write_csv(table_data, filtr.name, table_name, **vars(args))

table_files = [csv_files[name] for name in ["Projects", "Users", "Schedds", "Site", "Institution", "Machine", "Jobs"] if name in csv_files]
logger.info(f"Formatting data using {args.formatter.__name__}")
formatter = args.formatter(table_files, **vars(args))
logger.debug(f"Generating HTML")
html = formatter.get_html()

last_html_file = Path(f"last_html_{args.formatter.__name__}.html")
logger.debug(f"Dumping HTML to {last_html_file}")
with last_html_file.open("w") as f:
    f.write(html)

logger.info("Sending email")
try:
    accounting.send_email(
        subject=formatter.get_subject(**vars(args)),
        html=html,
        table_files=table_files,
        **vars(args))
except Exception:
    logger.exception("Caught exception while sending email")
    if args.quiet:
        print_tb(file=sys.stderr)
        print_exc(file=sys.stderr)

if args.report_period in ["daily", "weekly", "monthly"] and not args.do_not_upload:
    logger.info("Pushing daily totals to Elasticsearch")
    try:
        push_totals_to_es(table_files, "daily_totals", **vars(args))
    except Exception as e:
        logger.error("Could not push daily totals to Elasticsearch")
        if args.debug:
            logger.exception("Error follows")

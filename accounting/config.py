import os
import sys
import argparse
import logging
from .functions import get_timestamps
import accounting.filters as _filters
import accounting.formatters as _formatters

FILTERS = [x for x in dir(_filters) if x[0] != "_"]
FORMATTERS = [x for x in dir(_formatters) if x[0] != "_"]


def parse_args(args_in=sys.argv[1:]):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--filter",
        default=os.environ.get("FILTER", "BaseFilter"),
        help=f"Filter class, one of [{', '.join(FILTERS)}] (default: %(default)s)",
    )
    parser.add_argument(
        "--formatter",
        default=os.environ.get("FORMATTER", "BaseFormatter"),
        help=f"Formatter class, one of [{', '.join(FORMATTERS)}] (default: %(default)s)",
    )
    parser.add_argument(
        "--es_index",
        default=os.environ.get("ES_INDEX", "htcondor-000001"),
        help="Elasticsearch index (default: %(default)s)",
    )
    parser.add_argument(
        "--es_host",
        default=os.environ.get("ES_HOST", "localhost"),
        help="Elasticsearch hostname[:port] (default: %(default)s)",
    )
    parser.add_argument(
        "--es_user",
        default=os.environ.get("ES_USER"),
        help="Elasticsearch username",
    )
    parser.add_argument(
        "--es_pass",
        default=os.environ.get("ES_PASS"),
        help=argparse.SUPPRESS,  # Elasticsearch password
    )
    parser.add_argument(
        "--es_use_https",
        action="store_const",
        const=True,
        default=os.environ.get("ES_USE_HTTPS", str(False)).lower()[0] in ["1", "t", "y"],
        help="Use HTTPS with Elasticsearch (default: ES_USE_HTTPS=%(default)s)",
    )
    parser.add_argument(
        "--es_ca_certs",
        default=os.environ.get("ES_CA_CERTS"),
        help="Elasticsearch custom CA certs",
    )
    parser.add_argument(
        "--daily",
        dest="report_period",
        action="store_const",
        const="daily",
        help='Set report period to last full day (REPORT_PERIOD=daily, default)',
    )
    parser.add_argument(
        "--weekly",
        dest="report_period",
        action="store_const",
        const="weekly",
        help='Set report period to last full week (REPORT_PERIOD=weekly)',
    )
    parser.add_argument(
        "--monthly",
        dest="report_period",
        action="store_const",
        const="monthly",
        help='Set report peroid to last full month (REPORT_PERIOD=monthly)',
    )
    parser.add_argument(
        "--start_ts",
        type=int,
        default=os.environ.get("START_TS"),
        help="Custom starting timestamp",
    )
    parser.add_argument(
        "--end_ts",
        type=int,
        default=os.environ.get("END_TS"),
        help="Custom ending timestamp",
    )
    parser.add_argument(
        "--csv_dir",
        default=os.environ.get("CSV_DIR", "csv"),
        help="Output directory for CSVs (default: %(default)s)",
    )
    parser.add_argument(
        "--from_addr",
        default=os.environ.get("FROM_ADDR", "accounting@chtc.wisc.edu"),
        help="From: email address (default: %(default)s)",
    )
    parser.add_argument(
        "--to_addr",
        metavar="TO_ADDR",
        dest="to_addrs",
        action="append",
        default=[x.strip() for x in os.environ.get("TO_ADDRS", "").split(",") if x != ""],
        help="To: email address (can be specified multiple times)",
    )
    parser.add_argument(
        "--cc_addr",
        metavar="CC_ADDR",
        dest="cc_addrs",
        action="append",
        default=[x.strip() for x in os.environ.get("CC_ADDRS", "").split(",") if x != ""],
        help="CC: email address (can be specified multiple times)",
    )
    parser.add_argument(
        "--bcc_addr",
        metavar="BCC_ADDR",
        dest="bcc_addrs",
        action="append",
        default=[x.strip() for x in os.environ.get("BCC_ADDRS", "").split(",") if x != ""],
        help="BCC: email address (can be specified multiple times)",
    )
    parser.add_argument(
        "--reply_to_addr",
        metavar="REPLY_TO_ADDR",
        dest="reply_to_addr",
        help="Reply-To: email address",
    )
    args = parser.parse_args(args_in)

    # Get filter and formatter classes
    fail = False
    try:
        args.filter = getattr(_filters, args.filter)
    except AttributeError:
        logging.error(f"{args.filter} is not a valid filter")
        fail = True
    try:       
        args.formatter = getattr(_formatters, args.formatter)
    except AttributeError:
        logging.error(f"{args.formatter} is not a valid formatter")
        fail = True
    if fail:
        sys.exit(1)

    # Set reporting period
    args.report_period = os.environ.get("REPORT_PERIOD", args.report_period)
    if None not in (args.start_ts, args.end_ts,):
        args.report_period = "custom"
    if args.report_period is None:
        args.report_period = "daily"

    (args.start_ts, args.end_ts) = get_timestamps(args.report_period, args.start_ts, args.end_ts)

    return args

if __name__ == "__main__":
    args = vars(parse_args())
    del args["es_pass"]
    print(args)

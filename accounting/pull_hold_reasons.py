import pickle
import tempfile
import os
import time
import re
from urllib.request import urlopen
from pathlib import Path


CONDOR_HOLDCODES_URL = "https://raw.githubusercontent.com/htcondor/htcondor/main/src/condor_utils/condor_holdcodes.h"
HOLD_REASONS_PICKLE = Path("hold_reasons.pkl")
HOLD_REASON_RE = re.compile(r"\s*(\w+)\s*=\s*(\d+)\s*,?")


def parse_latest_hold_reasons():
    """Gets latest hold reasons"""

    hold_reasons = {}
    condor_holdcodes_h = urlopen(CONDOR_HOLDCODES_URL)
    in_comment_block = False

    for line in condor_holdcodes_h:
        try:
            line = line.decode()
        except RuntimeError:
            continue
        if line.strip() == "":
            continue
        if line.lstrip().startswith("/*"):
            in_comment_block = True
        if line.rstrip().endswith("*/"):
            in_comment_block = False
            continue
        if in_comment:
            continue
        if line.lstrip().startswith("#") or line.lstrip().startswith("//"):
            continue

        match = HOLD_REASON_RE.match(line)
        if match:
            hold_reasons[match.group(1)] = int(match.group(2))

    return hold_reasons


def update_hold_reasons_pickle(hold_reasons_pickle):
    """Updates (or creates) hold reason pickle file"""

    hold_reasons = parse_latest_hold_reasons()

    # Write atomically
    with tempfile.NamedTemporaryFile(delete=False, dir=str(Path.cwd())) as tf:
        tmpfile = Path(tf.name)
        with tmpfile.open("wb") as f:
            pickle.dump(hold_reasons, f)
            f.flush()
            os.fsync(f.fileno())
    tmpfile.rename(hold_reasons_pickle)


def get_hold_reasons(hold_reasons_pickle=HOLD_REASONS_PICKLE, force_update=False):
    """Returns hold reasons, updated if needed"""

    if force_update:
        update_hold_reasons_pickle(hold_reasons_pickle)
    try:
        hold_reasons = pickle.load(hold_reasons_pickle.open("rb"))
    except FileNotFoundError:
        update_hold_reasons_pickle(hold_reasons_pickle)
        hold_reasons = pickle.load(hold_reasons_pickle.open("rb"))
    return hold_reasons


if __name__ == "__main__":
    print(get_hold_reasons(force_update=True))

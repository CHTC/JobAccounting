import logging
import csv
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path


def break_chars(s):
    # Break after [@_.]
    # Don't break after [-]
    zero_width_space = "&#8203;"
    non_breaking_hyphen = "&#8209;"
    for char in ["@", "_", "."]:
        s = s.replace(char, f"{char}{zero_width_space}")
    s = s.replace("-", non_breaking_hyphen)
    return s


DEFAULT_TEXT_FORMAT    = lambda x: f'<td class="text">{break_chars(x)}</td>'
DEFAULT_NUMERIC_FORMAT = lambda x: f"<td>{int(x):,}</td>"
DEFAULT_COL_FORMATS    = {
    "% Good CPU Hours": lambda x: f"<td>{float(x):.1f}</td>",

}

DEFAULT_STYLES = {
    "body": [
        "font-size: 11pt",
        "font-family: sans-serif"
        ],
    "h1": [
        "font-size: 12pt",
        "text-align: center",
        ],
    "table": [
        "font-size: 10pt",
        "border-collapse: collapse",
        "border-color: #ffffff",
        ],
    "tr.odd": ["background-color: #fee"],
    "tr.even": ["background-color: #fff"],
    "th": [
        "border: 1px solid black",
        "font-weight: bold",
        "text-align: center",
        "background-color: #ddd",
        "min-width: 1px",
        ],
    "td": [
        "border: 1px solid black",
        "text-align: right",
        "min-width: 1px",
        ],
    "td.text": ["text-align: left"],
}

DEFAULT_LEGEND = OrderedDict()
DEFAULT_LEGEND["All CPU Hours"]    = "Total CPU hours for all execution attempts, including preemption and removal"
DEFAULT_LEGEND["Num Uniq Job Ids"] = "Number of unique job ids across all execution attempts"
DEFAULT_LEGEND["% Good CPU Hours"] = "Good CPU Hours per All CPU Hours, as a percentage"
DEFAULT_LEGEND["Good CPU Hours"]   = "Total CPU hours for execution attempts that ran to completion"
DEFAULT_LEGEND["Max Rqst Mem MB"]  = "Maximum memory requested across all submitted jobs in MB"
DEFAULT_LEGEND["Max Used Mem MB"]  = "Maximum measured memory usage across all submitted jobs' last execution attempts in MB"
DEFAULT_LEGEND["Max Rqst Cpus"]    = "Maximum number of CPUs requested across all submitted jobs"
DEFAULT_LEGEND["Max MB Sent"]      = "Maximum MB sent to a job sandbox from a submit point"
DEFAULT_LEGEND["Max MB Recv"]      = "Maximum MB sent to a submit point from a job sandbox"


class BaseFormatter:
    def __init__(self, table_files, *args, **kwargs):
        self.html_tables = []
        self.table_files = table_files
        for table_file in table_files:
            self.html_tables.append(self.get_table_html(table_file, **kwargs))

    def parse_table_filename(self, table_file):
        basename = Path(table_file).stem
        [name, agg, duration, start] = basename.split("_")[:4]
        name = name.replace("-", " ")  # remove dashses
        return {"name": name, "agg": agg, "duration": duration, "start": start}

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"{info['name']} {report_period.capitalize()} per {info['agg'].rstrip('s')} usage for jobs completed on {start_date}"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(kwargs["end_ts"])
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"{info['name']} per {info['agg'].rstrip('s')} usage for jobs completed from {start_date} to {end_date}"
        else:
            end = datetime.fromtimestamp(kwargs["end_ts"])
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"{info['name']} per {info['agg'].rstrip('s')} usage for jobs completed from {start_date} to {end_date}"
        return title_str

    def get_subject(self, *args, **kwargs):
        info = self.parse_table_filename(self.table_files[0])
        subject_str = f"{info['duration']} {info['name']} Usage Report {info['start']}"
        return subject_str

    def load_table(self, filename):
        with open(filename) as f:
            reader = csv.reader(f)
            header = [""] + next(reader)
            rows = [[""] + row for row in reader]
        data = {
            "header": header,
            "rows": rows,
        }
        return data

    def format_rows(self,
                    header,
                    rows,
                    custom_fmts={},
                    skip_cols=[],
                    default_text_fmt=None,
                    default_numeric_fmt=None
                        ):
        fmts = DEFAULT_COL_FORMATS.copy()
        fmts.update(custom_fmts)
        if default_text_fmt is None:
            default_text_fmt = DEFAULT_TEXT_FORMAT
        if default_numeric_fmt is None:
            default_numeric_fmt = DEFAULT_NUMERIC_FORMAT

        rows = rows.copy()
        for i, row in enumerate(rows):

            # pop skipped columns from the right
            curr_header = header.copy()
            if len(skip_cols) > 0:
                for j in range(len(row)-1, -1, -1):
                    col = header[j]
                    if col in skip_cols:
                        row.pop(j)
                        curr_header.pop(j)

            for j, value in enumerate(row):
                col = curr_header[j]

                # First column (blank header) contains row number
                # except total row contains total number of rows
                if col == "" and i == 0:
                    rows[i][j] = default_numeric_fmt(len(rows)-1)
                    continue
                elif col == "" and value == "":
                    rows[i][j] = default_numeric_fmt(float(i))
                    continue

                # Any column with a numeric value < 0 is undefined
                try:
                    if float(value) < 0:
                        value = ""
                except ValueError:
                    pass

                # Try to format columns as numeric (or as defined
                # in fmts), otherwise it's a string
                if col in fmts:
                    try:
                        rows[i][j] = fmts[col](float(value))
                    except ValueError:
                        rows[i][j] = default_text_fmt(value)
                else:
                    try:
                        rows[i][j] = default_numeric_fmt(float(value))
                    except ValueError:
                        rows[i][j] = default_text_fmt(value)

        return curr_header, rows
    
    def get_table_html(self, table_file, report_period, start_ts, end_ts, skip_cols=[], **kwargs):
        table_data = self.load_table(table_file)
        header, rows = self.format_rows(table_data["header"], table_data["rows"], skip_rows=skip_rows)

        rows_html = []
        for i, row in enumerate(rows):
            tr_class = ["even", "odd"][i % 2]
            rows_html.append(f'<tr class="{tr_class}">{"".join(row)}</tr>')
            
        newline = "\n  "
        html = f"""
<h1>{self.get_table_title(table_file, report_period, start_ts, end_ts)}</h1>
<table>
  <tr><th>{'</th><th>'.join(header)}</th></tr>
  {newline.join(rows_html)}
</table>
"""
        return html

    def get_css(self, custom_styles={}):
        styles = DEFAULT_STYLES.copy()
        styles.update(custom_styles)

        style = "\n"
        newline_tab = "\n  "
        for tag, attrs in styles.items():
            attrs = [f"{attr};" for attr in attrs]
            style += f"{tag} {{\n  {newline_tab.join(attrs)}\n}}\n"

        return style

    def get_legend(self, custom_items=OrderedDict()):
        legend_items = DEFAULT_LEGEND.copy()
        legend_items.update(custom_items)
        html_item = lambda k, v: f"<strong>{k}:</strong> {v}"
        list_delim = "</li>\n  <li>"
        html = f"""
<p>Legend:
<ul>
  <li>{list_delim.join([html_item(k, v) for k, v in legend_items.items()])}</li>
</ul>
</p>
"""
        return html
    
    def get_html(self):
        newline = "\n"
        html = f"""
<html>
<head>
<style>{self.get_css()}</style>
</head>
<body>
{newline.join(self.html_tables)}
{self.get_legend()}
</body>
</html>
"""
        return html

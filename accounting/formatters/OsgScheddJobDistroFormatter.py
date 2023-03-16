import csv
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal


def break_chars(s):
    # Break after [@_.]
    # Don't break after [-]
    zero_width_space = "&#8203;"
    non_breaking_hyphen = "&#8209;"
    for char in ["@", "_", "."]:
        s = s.replace(char, f"{char}{zero_width_space}")
    s = s.replace("-", non_breaking_hyphen)
    s = s.replace("<", "&lt;").replace(">", "&gt;")
    return s


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
    "th, td": [
        "border: 1px solid black",
        "text-align: right",
        "min-width: 1px",
        ],
    "th": [
        "background-color: #ddd",
    ]
}


class OsgScheddJobDistroFormatter:
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
        res_type = info["agg"][3:].lower()
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"{report_period.capitalize()} resource {res_type} histogram for jobs completed on {start_date}"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"Resource {res_type} histogram for jobs completed from {start_date} to {end_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"Resource {res_type} histogram for jobs completed from {start_date} to {end_date}"
        return title_str


    def get_subject(self, *args, **kwargs):
        info = self.parse_table_filename(self.table_files[0])
        subject_str = f"{info['duration'].capitalize()} OSG Connect Job Resource Histogram {info['start']}"
        return subject_str


    def load_table(self, filename):
        with open(filename) as f:
            reader = csv.reader(f)
            header = None
            rows = [row for row in reader]
        data = {
            "header": header,
            "rows": rows,
        }
        return data


    def format_rows(self, header, rows, res_type="requests"):

        jobs_note = rows[0][0]
        single_core_jobs, total_jobs = tuple(int(x) for x in jobs_note.split("/"))

        # shade the cell green if close to the max
        def numeric_fmt(x):
            x = float(x)
            if x < 1e-12:  # hide 0s
                return "<td></td>"
            if int(x) < 1:
                return "<td>0</td>"
            rgb = (100-x/2, 100, 100-x/2)
            return f'<td style="background-color: rgb({",".join([f"{v}%" for v in rgb])})">{x:.0f}</td>'

        col_header_fmt = lambda x: f'<th style="text-align: center; font-weight: bold">{break_chars(x)}</th>'
        row_header_fmt = lambda x: f'<td style="background-color: #ddd; text-align: right; font-weight: bold">{break_chars(x)}</td>'

        rows = rows.copy()
        for i, row in enumerate(rows):
            for j, value in enumerate(row):

                if i == 0 and j == 0:
                    rows[i][j] = """<th style="font-family: monospace; white-space: pre; margin: 0; text-align: left">      Disk
Memory</th>"""
                elif i == 0:
                    rows[i][j] = col_header_fmt(value)
                elif j == 0:
                    rows[i][j] = row_header_fmt(value)
                else:
                    try:
                        rows[i][j] = numeric_fmt(value)
                    except TypeError:
                        rows[i][j] = "<td>n/a</td>"

        # Extra header row
        rows.insert(0, [
            f"""<th style="text-align: center"></th>""",
            f"""<th style="text-align: center" colspan="{len(rows[0])-1}">Percentage of {single_core_jobs:,d} single-core jobs ({single_core_jobs/total_jobs:.1%} of all jobs).<br>Memory and disk {res_type} in GB.</th>""",
            ])

        return rows

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        table_data = self.load_table(table_file)
        info = self.parse_table_filename(table_file)
        rows = self.format_rows(table_data["header"], table_data["rows"], res_type=info["agg"][3:].lower())
        rows_html = [f'<tr>{"".join(row)}</tr>' for row in rows]
        newline = "\n  "
        html = f"""
<h1>{self.get_table_title(table_file, report_period, start_ts, end_ts)}</h1>
<table>
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


    def get_html(self):
        newline = "\n"
        html = f"""
<html>
<head>
<style>{self.get_css()}</style>
</head>
<body>
{newline.join(self.html_tables)}
</body>
<p><strong>Note:</strong> Blank values denote no jobs with the corresponding resource requests,
while values of 0 denote fewer than 1% of jobs. The usage table may have fewer jobs due to 
missing usage data in job ads.
</html>
"""
        return html

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
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"{report_period.capitalize()} resource request histogram for jobs completed on {start_date}"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(kwargs["end_ts"])
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"Resource request histogram for jobs completed from {start_date} to {end_date}"
        else:
            end = datetime.fromtimestamp(kwargs["end_ts"])
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"Resource request histogram for jobs completed from {start_date} to {end_date}"
        return title_str


    def get_subject(self, *args, **kwargs):
        info = self.parse_table_filename(self.table_files[0])
        subject_str = f"{info['duration']} OSPool Resource Histogram Report {info['start']}"
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


    def format_rows(self, header, rows):

        # get the maximum
        n_max = 0
        for i, row in enumerate(rows):
            if i == 0:
                continue
            n_max = max(n_max, max([int(x) for x in row[1:]]))

        # shade the cell green if close to the max
        default_numeric_fmt = lambda x: f'<td style="background-color: rgb({1-(x/n_max)/2:.0%}, 100%, {1-(x/n_max)/2:.0%})">{int(x):,}</td>'
        default_col_header_fmt = lambda x: f'<th style="text-align: center; font-weight: bold">{break_chars(x)}</th>'
        default_row_header_fmt = lambda x: f'<td style="text-align: right; font-weight: bold">{break_chars(x)}</td>'

        rows = rows.copy()
        for i, row in enumerate(rows):
            for j, value in enumerate(row):

                if i == 0 and j == 0:
                    rows[i][j] = """<th>
<pre>      Disk
Memory</pre>
</th>"""
                elif i == 0:
                    rows[i][j] = default_col_header_fmt(value)
                elif j == 0:
                    rows[i][j] = default_row_header_fmt(value)
                else:
                    try:
                        rows[i][j] = default_numeric_fmt(int(value))
                    except TypeError:
                        rows[i][j] = "<td>n/a</td>"

        return rows

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        table_data = self.load_table(table_file)
        rows = self.format_rows(table_data["header"], table_data["rows"])

        rows_html = []
        for i, row in enumerate(rows):
            tr_class = ["even", "odd"][i % 2]
            rows_html.append(f'<tr class="{tr_class}">{"".join(row)}</tr>')

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
</html>
"""
        return html

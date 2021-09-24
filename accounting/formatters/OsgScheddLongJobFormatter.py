from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


DEFAULT_NUMERIC_FORMAT = lambda x: f"<td>{int(float(x)):,}</td>"

def hhmm(hours):
    # Convert float hours to HH:MM
    h = int(hours)
    m = int(60 * (float(hours) - int(hours)))
    return f"{h:01d}:{m:02d}"

def break_hyphens(s):
    # Break after [@_.-]
    zero_width_space = "&#8203;"
    for char in ["@", "_", "."]:
        s = s.replace(char, f"{char}{zero_width_space}")
    return s


class OsgScheddLongJobFormatter(BaseFormatter):

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"OSPool {info['agg']}' longest job completed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"OSPool {info['agg']}' longest job completed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"OSPool {info['agg']}' longest job completed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"OSPool {report_period.capitalize()} Longest Job Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Longest Job Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {}
        return super().rm_cols(data, cols=cols)

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        return super().get_table_html(table_file, report_period, start_ts, end_ts, **kwargs)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "Last Wall Hrs":     lambda x: f"<td>{hhmm(x)}</td>",
            "Total Wall Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Potent CPU Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Actual CPU Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "% CPU Eff":    lambda x: f"<td>{float(x):.1f}</td>",
            "CPUs Used":    lambda x: f"<td>{float(x):.3f}</td>",
            "Job Id":       lambda x: f'<td class="text">{x}</td>',
            "Last Site":      lambda x: f'<td class="text">{break_hyphens(x)}</td>',
            "Last Wrkr Node": lambda x: f'<td class="text">{break_hyphens(x)}</td>',
        }

        rows = super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)

        for i in range(len(rows)):
            rows[i][0] = f"<td>{i+1:,}</td>"

        return rows

    def get_legend(self, custom_items=OrderedDict()):
        legend_items = custom_items
        legend_items["Last Wall Hrs"] = "Wallclock hours for last execution attempt"
        legend_items["Total Wall Hrs"] = "Total wallclock hours across all execution attempts"
        legend_items["Potent CPU Hrs"] = "Potential CPU hours used during last execution attempt, based on RequestCpus"
        legend_items["Actual CPU Hrs"] = "Actual CPU hours used during last execution attempt, based on CPUsUsage"
        legend_items["% CPU Eff"] = "CPU efficiency, computed using CPUsUsage / RequestCpus"
        legend_items["Last Wrkr MIPS"] = 'Benchmark value last obtained on the machine that ran the last execution attempt, a value less than 11,800 is considered "slow"'
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



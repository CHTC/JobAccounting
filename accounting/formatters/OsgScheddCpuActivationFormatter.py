from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


class OsgScheddCpuActivationFormatter(BaseFormatter):

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs completed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"OSPool {report_period.capitalize()} Activation Failure Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Activation Failure Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {}
        return super().rm_cols(data, cols=cols)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "% Jobs w/ Failures":   lambda x: f"<td>{float(x):.1f}</td>",
            "Num Atts / Job":       lambda x: f"<td>{float(x):.2f}</td>",
            "Num Failures / Job":   lambda x: f"<td>{float(x):.2f}</td>",
            "Failures / Att":       lambda x: f"<td>{float(x):.2f}</td>",
            "Failure Reasons:":     lambda x: "<td></td>",
            "% Transfer Input":     lambda x: f"<td>{float(x):.1f}</td>",
            "% Other":              lambda x: f"<td>{float(x):.1f}</td>",
        }
        rows = super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)
        return rows

    def get_legend(self):
        custom_items = OrderedDict()
        custom_items["Num Jobs w/ Activation Atts"]   = "Number of jobs with at least one activation attempt (i.e. shadow start)"
        custom_items["Num Jobs w/ Activation Failures"] = "Number of jobs with at least one activation failure (i.e. shadow start w/ job start)"
        custom_items["% Jobs w/ Failures"] = "Percentage of jobs with at least one activation failure"
        custom_items["Num Atts"] = "Number of activation attempts across all jobs"
        custom_items["Num Atts / Job"] = "Activations attempted per job"
        custom_items["Num Failures"] = "Number of activation failures across all jobs"
        custom_items["Num Failures / Job"] = "Activations failed per job"
        custom_items["Failures / Att"] = "Activations failed per activation attempt"
        custom_items["Failure Reasons"] = "Percentage of activation attempts that failed for the given reason"
        html = super().get_legend(custom_items)
        return html


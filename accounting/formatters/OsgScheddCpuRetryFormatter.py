from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


class OsgScheddCpuRetryFormatter(BaseFormatter):

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
            subject_str = f"OSPool {report_period.capitalize()} Shadow Retry Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Shadow Retry Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {}
        return super().rm_cols(data, cols=cols)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "Shadow Starts / Job Id":       lambda x: f"<td>{float(x):.2f}</td>",
            "% Jobs w/ >1 Shadow Starts":   lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/ >0 Input Xfer Errs": lambda x: f"<td>{float(x):.1f}</td>",
            "% NSS due to Input Xfer Errs": lambda x: f"<td>{float(x):.1f}</td>",
        }
        rows = super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)
        return rows

    def get_legend(self):
        custom_items = OrderedDict()
        custom_items["Shadow Starts / Job Id"]   = "Shadow starts per job (that had at least one shadow start)"
        custom_items["Non Success Shadows (NSS)"] = "Shadow starts that did not result in job completion"
        custom_items["% Jobs w/ >1 Shadow Starts"] = "Percentage of jobs that had multiple shadow starts"
        custom_items["% Jobs w/ >0 Input Xfer Errs"] = "Percentage of jobs that had at least one transfer input error"
        custom_items["% NSS due to Input Xfer Errs"] = "Percentage of NSS's that were related to transfer input error"
        html = super().get_legend(custom_items)
        return html


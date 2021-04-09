from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


def hhmm(hours):
    # Convert float hours to HH:MM
    h = int(hours)
    m = int(60 * (float(hours) - int(hours)))
    return f"{h:02d}:{m:02d}"


class OsgScheddCpuRemovedFormatter(BaseFormatter):


    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs removed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs removed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"OSPool per {info['agg'].rstrip('s')} usage for jobs removed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"OSPool {report_period.capitalize()} Removed Jobs Usage Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Removed Jobs Usage Report {start_date} to {end_date}"
        return subject_str

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "Min Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "25% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Med Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "75% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Max Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Mean Hrs":   lambda x: f"<td>{hhmm(x)}</td>",
            "Std Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "CPU Hours / Bad Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Shadw Starts / Job Id":    lambda x: f"<td>{float(x):.2f}</td>",
            "Exec Atts / Shadw Start":  lambda x: f"<td>{float(x):.3f}</td>",
        }
        rows = super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)
        return rows
    
    def get_legend(self):
        custom_items = OrderedDict()
        custom_items["CPU Hours / Bad Exec Att"] = "Average CPU Hours used in a non-final execution attempt"
        custom_items["Shadw Starts / Job Id"] = "Num Shadw Starts per Num Uniq Job Ids"
        custom_items["Exec Atts / Shadw Start"] = "Num Exec Atts per Num Shadw Starts"
        custom_items["Num Rm'd Jobs"]    = "Number of jobs that were removed from the queue instead of allowing to complete"
        custom_items["Avg MB Sent"] = "Mean MB sent to a job sandbox from a submit point"
        custom_items["Avg MB Recv"] = "Mean MB sent to a submit point from a job sandbox"
        custom_items["Num Short Jobs"]   = "Number of execution attempts that completed in less than 60 seconds"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Hrs"] = "Final execution wallclock hours that a non-short job (Min-Max) or jobs (Mean/Std) ran for (excluding Short jobs, excluding Local and Scheduler Universe jobs)"
        custom_items["Med Used Mem MB"]  = "Median measured memory usage across all submitted jobs' last execution attempts in MB"
        custom_items["Num Exec Atts"]    = "Total number of execution attempts (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Shadw Starts"] = "Total times a condor_shadow was spawned across all submitted jobs (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Local Univ Jobs"] = "Number of jobs that used local universe"
        custom_items["Num Sched Univ Jobs"] = "Number of jobs that used scheduler universe"
        html = super().get_legend(custom_items)
        return html


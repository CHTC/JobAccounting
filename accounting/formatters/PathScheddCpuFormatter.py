from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


def hhmm(hours):
    # Convert float hours to HH:MM
    h = int(hours)
    m = int(60 * (float(hours) - int(hours)))
    return f"{h:02d}:{m:02d}"


class PathScheddCpuFormatter(BaseFormatter):

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"PATh Facility per {info['agg'].rstrip('s')} usage for jobs completed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"PATh Facility per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"PATh Facility per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"PATh Facility AP {report_period.capitalize()} Usage Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"PATh Facility AP Usage Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {
            "Good CPU Hours",
            "Num Exec Atts",
            "Num Shadw Starts",
            "Num Job Holds",
            "Num Rm'd Jobs",
            "Num Jobs w/>1 Exec Att",
            "Num Jobs w/1+ Holds",
            "Num Short Jobs",
        }
        return super().rm_cols(data, cols=cols)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        custom_fmts = {
            "Min Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "25% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Med Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "75% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "95% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Max Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Mean Hrs":   lambda x: f"<td>{hhmm(x)}</td>",
            "Std Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "CPU Hours / Bad Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Shadw Starts / Job Id":    lambda x: f"<td>{float(x):.2f}</td>",
            "Exec Atts / Shadw Start":  lambda x: f"<td>{float(x):.3f}</td>",
            "% Ckpt Able":          lambda x: f"<td>{float(x):.1f}</td>",
            "% Rm'd Jobs":          lambda x: f"<td>{float(x):.1f}</td>",
            "% Short Jobs":         lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/>1 Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/1+ Holds":    lambda x: f"<td>{float(x):.1f}</td>",
            "Input Files / Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Input MB / Exec Att":    lambda x: f"<td>{float(x):.1f}</td>",
            "Input MB / File":        lambda x: f"<td>{float(x):.1f}</td>",
            "Output Files / Job":     lambda x: f"<td>{float(x):.1f}</td>",
            "Output MB / Job":        lambda x: f"<td>{float(x):.1f}</td>",
            "Output MB / File":       lambda x: f"<td>{float(x):.1f}</td>",
        }
        rows = super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)
        return rows

    def get_legend(self):
        custom_items = OrderedDict()
        custom_items["All CPU Hours"]    = "Total CPU hours for all execution attempts, including preemption and removal"
        custom_items["Num Uniq Job Ids"] = "Number of unique job ids across all execution attempts"
        custom_items["% Good CPU Hours"] = "Good CPU Hours per All CPU Hours, as a percentage"
        custom_items["Good CPU Hours"]   = "Total CPU hours for execution attempts that ran to completion"
        custom_items["Max Rqst Mem MB"]  = "Maximum memory requested across all submitted jobs in MB"
        custom_items["Max Used Mem MB"]  = "Maximum measured memory usage across all submitted jobs' last execution attempts in MB"
        custom_items["Max Rqst Cpus"]    = "Maximum number of CPUs requested across all submitted jobs"
        
        custom_items["Num Shadw Starts"] = "Total times a condor_shadow was spawned across all submitted jobs (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Exec Atts"]    = "Total number of execution attempts (excluding Local and Scheduler Universe jobs)"
        custom_items["Num Rm'd Jobs"]    = "Number of jobs that were removed from the queue instead of allowing to complete"
        custom_items["Num Short Jobs"]   = "Number of execution attempts that completed in less than 60 seconds"
        custom_items["Num Jobs w/>1 Exec Att"] = "Number of unique jobs that were executed more than once"
        custom_items["Num Ckpt Able Jobs"] = "Number of unique jobs that may be checkpointing"

        custom_items["% Ckpt Able"] = "Percent of Num Uniq Job Ids that may be checkpointing"
        custom_items["% Rm'd Jobs"] = "Percent of Num Uniq Job Ids that were removed"
        custom_items["% Short Jobs"] = "Percent of Num Uniq Job Ids that were short jobs"
        custom_items["% Jobs w/>1 Exec Att"] = "Percent of Num Uniq Job Ids that had more than one execution attempt"
        custom_items["% Jobs w/1+ Holds"] = "Percent of Num Uniq Job Ids that had one or more jobs go on hold"
        custom_items["Total Files Xferd"] = "Total files transferred (input and output) across all transfer protocols and execution attempts"

        custom_items["Shadw Starts / Job Id"]   = "Num Shadw Starts per Num Uniq Job Ids"
        custom_items["Exec Atts / Shadw Start"] = "Num Exec Atts per Num Shadw Starts"
        custom_items["Holds / Job Id"] = "Num Job Holds per Num Uniq Job Ids"

        custom_items["Mean Actv Hrs"] = "Mean slot activation time (in hours)"
        custom_items["Mean Setup Secs"] = "Mean slot activation setup time (in seconds). The slot activation setup time is the duration from when a shadow sends a claim activation to when the shadow is told that a job's executable is running."

        custom_items["Min/25%/Median/75%/Max/Mean/Std Hrs"] = "Final execution wallclock hours that a non-short job (Min-Max) or jobs (Mean/Std) ran for (excluding Short jobs, excluding Local and Scheduler Universe jobs)"

        custom_items["Input Files / Exec Att"] = "Number of files (all protocols) sent as part of the input sandbox per job start"
        custom_items["Input MB / Exec Att"] = "Average size (in MB) of input sandboxes"
        custom_items["Input MB / File"] = "Average size of file in input sandboxes"

        custom_items["Output Files / Job"] = "Number of files transferred back to the access point (and other output locations) per (completed) job"
        custom_items["Output MB / Job"] = "Average size (in MB) of output sandboxes"
        custom_items["Output MB / File"] = "Average size of file in output sandboxes"

        custom_items["Med Used Mem MB"]  = "Median measured memory usage across all submitted jobs' last execution attempts in MB"

        custom_items["CPU Hours / Bad Exec Att"] = "Average CPU Hours used in a non-final execution attempt"
        html = super().get_legend(custom_items)
        return html


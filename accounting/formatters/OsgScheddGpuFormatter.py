import sys
from .BaseFormatter import BaseFormatter
from datetime import datetime
from collections import OrderedDict


def hhmm(hours):
    # Convert float hours to HH:MM
    h = int(hours)
    m = int(60 * (float(hours) - int(hours)))
    return f"{h:02d}:{m:02d}"


def handle_dashes(dtype, fmt, value):
    # Cast value to dtype and format it.
    # Right-align any non-castable values and return them as is.
    formatted_str = "<td></td>"
    try:
        value = dtype(value)
        formatted_str = f"<td>{value:{fmt}}</td>"
    except ValueError:
        formatted_str = f"""<td style="text-align: right">{value}</td>"""
    except Exception as err:
        print(f"Caught unexpected exception {str(err)} when converting {value} to {repr(dtype)}.", file=sys.stderr)
        formatted_str = f"""<td style="text-align: right">{value}</td>"""
    return formatted_str


class OsgScheddGpuFormatter(BaseFormatter):

    def get_table_title(self, table_file, report_period, start_ts, end_ts):
        info = self.parse_table_filename(table_file)
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily"]:
            start_date = start.strftime("%Y-%m-%d")
            title_str = f"OSPool GPUs per {info['agg'].rstrip('s')} usage for jobs completed on <strong>{start_date}</strong>"
        elif report_period in ["weekly", "monthly"]:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d")
            end_date = end.strftime("%Y-%m-%d")
            title_str = f"OSPool GPUs per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            title_str = f"OSPool GPUs per {info['agg'].rstrip('s')} usage for jobs completed from <strong>{start_date} to {end_date}</strong>"
        return title_str

    def get_subject(self, report_period, start_ts, end_ts, **kwargs):
        # Format date(s)
        start = datetime.fromtimestamp(start_ts)
        if report_period in ["daily", "weekly", "monthly"]:
            start_date = start.strftime("%Y-%m-%d")
            subject_str = f"OSPool GPUs {report_period.capitalize()} Usage Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool GPUs Usage Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {
            "Good CPU Hours",
            "Good GPU Hours",
            "Num Exec Atts",
            "Num Shadw Starts",
            "Num Job Holds",
            "Num Rm'd Jobs",
            "Num Jobs w/>1 Exec Att",
            "Num Jobs w/1+ Holds",
            "Num Short Jobs",
        }
        return super().rm_cols(data, cols=cols)

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        return super().get_table_html(table_file, report_period, start_ts, end_ts, **kwargs)

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
            "Mean Actv Hrs": lambda x: f"<td>{hhmm(x)}</td>",
            "CPU Hours / Bad Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "GPU Hours / Bad Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Shadw Starts / Job Id":    lambda x: f"<td>{float(x):.2f}</td>",
            "Exec Atts / Shadw Start":  lambda x: f"<td>{float(x):.3f}</td>",
            "% Good GPU Hours":     lambda x: f"<td>{float(x):.1f}</td>",
            "Holds / Job Id":           lambda x: f"<td>{float(x):.2f}</td>",
            "OSDF Files Xferd":     lambda x: handle_dashes(  int,   ",", x),
            "% OSDF Files":         lambda x: handle_dashes(float, ".1f", x),
            "% OSDF Bytes":         lambda x: handle_dashes(float, ".1f", x),
            "% Rm'd Jobs":          lambda x: f"<td>{float(x):.1f}</td>",
            "% Short Jobs":         lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/>1 Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs w/1+ Holds":    lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs Over Rqst Disk": lambda x: f"<td>{float(x):.1f}</td>",
            "% Ckpt Able":          lambda x: f"<td>{float(x):.1f}</td>",
            "% Jobs using S'ty":    lambda x: f"<td>{float(x):.1f}</td>",
            "Input Files / Exec Att": lambda x: f"<td>{float(x):.1f}</td>",
            "Input MB / Exec Att":    lambda x: f"<td>{float(x):.1f}</td>",
            "Input MB / File":        lambda x: f"<td>{float(x):.1f}</td>",
            "Output Files / Job":     lambda x: f"<td>{float(x):.1f}</td>",
            "Output MB / Job":        lambda x: f"<td>{float(x):.1f}</td>",
            "Output MB / File":       lambda x: f"<td>{float(x):.1f}</td>",
            "Max Rqst Disk GB":       lambda x: f"<td>{float(x):.1f}</td>",
            "Max Used Disk GB":       lambda x: f"<td>{float(x):.1f}</td>",
        }
        return super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)

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
        custom_items["Num Jobs Over Rqst Disk"] = "Number of unique jobs that went over their requested disk space"
        custom_items["Num Ckpt Able Jobs"] = "Number of unique jobs that may be using user-level checkpointing"

        custom_items["% Rm'd Jobs"] = "Percent of Num Uniq Job Ids that were removed"
        custom_items["% Short Jobs"] = "Percent of Num Uniq Job Ids that were short jobs"
        custom_items["% Jobs w/>1 Exec Att"] = "Percent of Num Uniq Job Ids that had more than one execution attempt"
        custom_items["% Jobs w/1+ Holds"] = "Percent of Num Uniq Job Ids that had one or more jobs go on hold"
        custom_items["% Jobs Over Rqst Disk"] = "Percent of Num Uniq Job Ids that went over their requested disk space"
        custom_items["% Jobs using S'ty"] = "Percent of Num Uniq Job Ids that requested to run inside a Singularity image"
        custom_items["% Ckpt Able"] = "Percent of Num Uniq Job Ids that may be using user-level checkpointing"
        custom_items["Total Files Xferd"] = "Total files transferred (input and output) across all transfer protocols and execution attempts"
        custom_items["OSDF Files Xferd"] = "Total files transferred using the OSDF (f.k.a. Stash) transfer plugin. A zero in an OSDF column means that no files were transferred with the OSDF for this [row]. A dash in an OSDF column means that this [row] is unable to transfer files with the OSDF"
        custom_items["% OSDF Files"] = "Percent of all transferred files that were transferred using the OSDF plugin"
        custom_items["% OSDF Bytes"] = "Percent of all transferred bytes that were transferred using the OSDF plugin"

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
        custom_items["Max Rqst/Used Disk GB"] = "Maximum requested/used disk space across all submittted jobs' last execution attempts in GB"

        custom_items["CPU Hours / Bad Exec Att"] = "Average CPU Hours used in a non-final execution attempt"
        custom_items["Num Local Univ Jobs"] = "Number of jobs that used local universe"
        custom_items["Num Sched Univ Jobs"] = "Number of jobs that used scheduler universe"
        html = super().get_legend(custom_items)
        return html


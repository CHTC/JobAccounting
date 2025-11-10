import sys
from .BaseFormatter import BaseFormatter, break_chars
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


def compact_institution(s):
    # Shorted some common nouns
    s = s.replace("University", "U.")
    s = s.replace("Laboratory", "Lab.")
    s = s.replace("College", "Coll.")
    s = s.replace("Institute", "Inst.")
    s = s.replace("Technology", "Tech.")
    s = s.replace("National", "Nat'l")
    s = s.replace("Science", "Sci.")
    s = break_chars(s)
    return s


class OsgScheddResourcesFormatter(BaseFormatter):

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
            subject_str = f"OSPool {report_period.capitalize()} Resources Report {start_date}"
        else:
            end = datetime.fromtimestamp(end_ts)
            start_date = start.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end.strftime("%Y-%m-%d %H:%M:%S")
            subject_str = f"OSPool Resources Report {start_date} to {end_date}"
        return subject_str

    def rm_cols(self, data):
        cols = {
            "Good CPU Hours",
            "Num Job Starts",
            "Num Shadow Starts",
            "Num Job Holds",
            "Num Rm'd Jobs",
            "Num Jobs w/>1 Exec Att",
            "Num Jobs w/1+ Holds",
            "Num Short Jobs",
            "Num Shadow Starts Post 24.11.1",
            "Num TransferInputError",
            "Num Jobs Post 24.11.1",
        }
        return super().rm_cols(data, cols=cols)

    def get_table_html(self, table_file, report_period, start_ts, end_ts, **kwargs):
        return super().get_table_html(table_file, report_period, start_ts, end_ts, **kwargs)

    def format_rows(self, header, rows, custom_fmts={}, default_text_fmt=None, default_numeric_fmt=None):
        memory_lt_fmt = lambda x: f"<td>{round(float(x)):,.0f}</td>" if round(float(x)) >= 1 else "<td>&lt;</td>"
        memory_zero_fmt = lambda x: f"<td>{round(float(x)):,.0f}</td>"
        memory_lt_cols = ["Req Mem", "Use Mem"]
        memory_zero_cols = ["Unuse Mem GBh", "Util% Mem", "Req Mem GBh"]
        distros = ["Min", "25%", "Med", "75%", "95%", "Max", "Mean"]
        custom_fmts = {
            "&nbsp;": lambda x: '<td style="background-color: #ddd"></td>',
            "PI Institution": lambda x: f'<td class="text">{compact_institution(x)}</td>',
            "Min Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "25% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Med Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "75% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "95% Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Max Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "Mean Hrs":   lambda x: f"<td>{hhmm(x)}</td>",
            "Stdv Hrs":    lambda x: f"<td>{hhmm(x)}</td>",
            "% Short Jobs":         lambda x: f"<td>{float(x):.1f}</td>",
            "Total Mem GBh Util%":  lambda x: f"<td>{float(x):.1f}</td>",
        }
        for memory_col in memory_lt_cols:
            for distro in distros:
                custom_fmts[f"{distro} {memory_col}"] = memory_lt_fmt
            custom_fmts[f"Stdv {memory_col}"] = memory_zero_fmt
        for memory_col in memory_zero_cols:
            for distro in distros:
                custom_fmts[f"{distro} {memory_col}"] = memory_zero_fmt
            custom_fmts[f"Stdv {memory_col}"] = memory_zero_fmt

        return super().format_rows(header, rows, custom_fmts=custom_fmts, default_text_fmt=default_text_fmt, default_numeric_fmt=default_numeric_fmt)

    def get_legend(self):
        custom_items = OrderedDict()

        custom_items["Total Unuse Mem GBh"] = "Total unused (allocated - used) memory time of all jobs, in GB-hours"
        custom_items["Total Mem GBh Util%"] = "Total memory utilization, weighted by wallclock time"
        custom_items["Total Allo Mem GBh"] = "Total allocated memory time of all jobs, in GB-hours"

        custom_items["Min/25%/Median/75%/Max/Mean/Std Util% Mem"] = r"Final execution memory utility (% used/allocated)"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Allo Mem GBh"] = "Allocated memory time (using final execution memory request) in GB-hours"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Unuse Mem GB"] = "Unused memory time (using final execution memory request and usage) in GB-hours"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Allo Mem"] = "Final execution memory allocation in GB"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Use Mem"] = "Final execution memory usage in GB"

        custom_items["&lt;"] = "Memory allocation or usage was below 1 GB"

        custom_items["% Short Jobs"] = "Percent of Num Uniq Job Ids that were short jobs (<1 minute)"
        custom_items["Min/25%/Median/75%/Max/Mean/Std Hrs"] = "Final execution wallclock hours that a non-short job (Min-Max) or jobs (Mean/Std) ran for"

        html = super().get_legend(custom_items)
        return html


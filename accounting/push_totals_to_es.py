import sys
import json
import elasticsearch
import logging
from pathlib import Path
import importlib

logger = logging.getLogger("accounting.push_totals_to_es")

def push_totals_to_es(csv_files, index_name, **kwargs):
    def rename_field(field):
        field = field.casefold()
        field = field.replace("util%", "util_pct")
        field = field.replace(" ", "_")
        field = field.replace("'d", "ed")
        field = field.replace("w/", "with_")
        field = field.replace(">", "more_than_")
        field = field.replace("<", "less_than_")
        field = field.replace("%", "pct")
        field = field.replace("/", "per")
        return field

    def read_csv(csv_file):
        with open(csv_file) as f:
            headers = f.readline().rstrip().split(",")
            totals = f.readline().rstrip().split(",")
            entries = sum([1 for line in f])
        headers = [rename_field(header) for header in headers]
        return (dict(zip(headers, totals)), entries)

    def parse_csv_name(csv_file):
        csv_file = Path(csv_file)
        parts = ["Name", "Table", "Period", "Date"]
        return dict(zip(parts, csv_file.stem.split("_")))

    def connect(es_host, es_user, es_pass, es_use_https, es_ca_certs, **kwargs):
        # Returns Elasticsearch client

        # Split off port from host if included
        if ":" in es_host and len(es_host.split(":")) == 2:
            [es_host, es_port] = es_host.split(":")
            es_port = int(es_port)
        elif ":" in es_host:
            logger.error(f"Ambiguous hostname:port in given host: {es_host}")
            sys.exit(1)
        else:
            es_port = 9200
        es_client = {
            "host": es_host,
            "port": es_port
        }

        # Include username and password if both are provided
        if es_user is None and es_pass is None:
            pass
        elif (es_user is None) != (es_pass is None):
            logger.warning("Only one of es_user and es_pass have been defined")
            logger.warning("Connecting to Elasticsearch anonymously")
        else:
            es_client["http_auth"] = (es_user, es_pass)

        if "es_url_prefix" in kwargs:
            es_client["url_prefix"] = kwargs["es_url_prefix"]

        # Only use HTTPS if CA certs are given or if certifi is available
        if es_use_https:
            if es_ca_certs is not None:
                logger.info(f"Using CA from {es_ca_certs}")
                es_client["ca_certs"] = es_ca_certs
            elif importlib.util.find_spec("certifi") is not None:
                logger.info("Using CA from certifi library")
            else:
                logger.error("Using HTTPS with Elasticsearch requires that either es_ca_certs be provided or certifi library be installed")
                sys.exit(1)
            es_client["use_ssl"] = True
            es_client["verify_certs"] = True

        return elasticsearch.Elasticsearch([es_client])

    def make_index(client, index):
        index_client = elasticsearch.client.IndicesClient(client)
        if index_client.exists(index):
            return

        properties = {
            "date": {"type": "date"},
        }
        dynamic_templates = [
            {
                "strings_as_keywords": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword", "norms": "false", "ignore_above": 256},
                }
            },
        ]
        mappings = {
            "dynamic_templates": dynamic_templates,
            "properties": properties,
            "date_detection": False,
            "numeric_detection": True,
        }

        body = json.dumps({"mappings": mappings})
        client.indices.create(index=index, body=body)

    def get_doc_id(doc):
        return f"{doc['query']}_{doc['report_period']}_{doc['date']}"

    totals = {}
    lengths = {}
    for csv_file in csv_files:
        metadata = parse_csv_name(csv_file)
        table = metadata["Table"]
        if table[-1] == "s":
            table = table[:-1]
        (totals[table], lengths[table]) = read_csv(csv_file)
        del totals[table][table.casefold()]
        if "nbsp;" in totals[table]:
            del totals[table]["nbsp;"]
        for key in totals[table]:
            if totals[table][key] == "n/a":
                totals[table][key] = None
            if totals[table][key] is not None:
                try:
                    totals[table][key] = int(totals[table][key])
                    if totals[table][key] == -999:
                        totals[table][key] = None
                except ValueError:
                    try:
                        totals[table][key] = float(totals[table][key])
                        if abs(totals[table][key] + 999) < 0.001:
                            totals[table][key] = None
                    except ValueError:
                        pass
        totals[table][f"num_{table.casefold()}s"] = lengths[table]
        totals[table]["date"] = metadata["Date"]
        totals[table]["query"] = metadata["Name"]
        totals[table]["report_period"] = metadata["Period"]

    if "Schedd" in totals:
        total = totals["Schedd"]
        del lengths["Schedd"]
    else:
        key = list(totals.keys())[0]
        if key in ["Site", "Facility", "Institution"]:
            key = list(totals.keys())[-1]
        total = totals[key]
        del lengths[key]
    if "Facility" in totals:
        total["num_sites"] = totals["Facility"]["num_sites"]
    if "Institution" in totals:
        total["num_sites"] = totals["Institution"]["num_sites"]
    for table in lengths:
        total[f"num_{table.casefold()}s"] = lengths[table]

    es = connect(**kwargs)
    make_index(es, index_name)
    es.index(index=index_name, id=get_doc_id(total), body=total)

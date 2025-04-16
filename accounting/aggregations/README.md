CHTC Elasticsearch DSL Reporting

### Install Required Packages
* `pip install elasticsearch<8 elasticsearch_dsl<8 tabulate`

### Command Line Options
* `--output` specify the output CSV file name
* `--print-table` prints the report output on the command line
* `-s / --start <YYYY-MM-DD>` Report from this date forward
* `-e / --end <YYYY-MM-DD>` Report until this date (defaults to now)
* `-h / --host <hostname>` Elasticsearch server hostname (defaults to `http://localhost:9200`)
* `-i / --index <index_name>` Elasticsearch index to search from (defaults to `chtc-schedd-*`)

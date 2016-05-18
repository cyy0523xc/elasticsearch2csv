#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# Export data from ElasticSearch to CSV file.
# 从es导出数据到csv
# @author Alex<cyy0523xc@gmail.com>
# @version 20160518

import csv
import sys
import click
from settings import ES_CONF
from pyelasticsearch import ElasticSearch

reload(sys)
sys.setdefaultencoding('utf-8')


@click.command()
@click.option('--index-name', required=True,
              help='Index name to load data from')
@click.option('--doc-type', required=True,
              help='Doc Type name to load data from')
@click.option('--file-name', required=True,
              help='CSV File name to save')
@click.option('--size', required=False, default=1000,
              help='The number of hits to return. Defaults to 1000.')
def cli(index_name, doc_type, file_name, size):
    """
    Export data from ElasticSearch to CSV file.

    \b
    Help:
        python es2csv.py --help

    \b
    Example:
        python es2csv.py --index-name=index_name --doc-type=typename
            --file-name=/tmp/save_file.csv
    """
    es = ElasticSearch(ES_CONF['host'])
    mapping = es.get_mapping(index=index_name, doc_type=doc_type)
    fieldnames = mapping[index_name]['mappings'][doc_type]['properties'].keys()
    print "Fields Total: %d" % len(fieldnames)

    writer = csv.writer(file(file_name, 'wb'), quoting=csv.QUOTE_NONNUMERIC)
    writer.writerow(fieldnames)
    print fieldnames

    data = es.search("*", index=index_name, doc_type=doc_type, size=1)
    total = data['hits']['total']
    print "Total: %d" % total

    size = 1000
    for es_from in range(0, total+1, size):
        data = es.search("*", index=index_name, doc_type=doc_type,
                         es_from=es_from, size=size)
        data = data['hits']['hits']
        format_data = []
        for row in data:
            for k in fieldnames:
                if k not in row['_source']:
                    row['_source'][k] = ''
            format_data.append([row['_source'][k] for k in fieldnames])

        writer.writerows(format_data)
        print "Saved count %d" % (es_from + size)

    print 'ok'


if __name__ == "__main__":
    cli()

#!/usr/bin/env python

"""Update the logs_streaming schema to match what the streaming job wants.

Our logexport script writes records to the bigquery
khan-academy:logs_streaming.logs_all_time table.  It hard-codes what
columns it writes, and there will be an error if the bigquery table's
schema does not include all those columns.  This script updates the
bigquery table to make sure they match.

You should run this whenever adding new elog fields to
EventLogParser.java.  To make it easy to remember, we run the script
automatically in the Makefile.

This script needs the 'bq' binary to be installed and authenticated.
"""

import difflib
import json
import pprint
import subprocess
import tempfile


_BQ = ['bq', '-q', '--format=json', '--headless']

# Where the requestlogs tables live.
_KHAN_PROJECT = ['--project_id', 'khanacademy.org:deductive-jet-827']
# Where the streaming logs live.
_KHAN_ACADEMY_PROJECT = ['--project_id', 'khan-academy']


def _bq(project, command_list):
    """`project` is one of _KHAN_PROJECT or _KHAN_ACADEMY_PROJECT."""
    data = subprocess.check_output(_BQ + project + command_list)
    return json.loads(data)


def _schema(project, table_name):
    """`project` is one of _KHAN_PROJECT or _KHAN_ACADEMY_PROJECT."""
    data = _bq(project, ['show', table_name])
    return data['schema']['fields']


def _schema_from_java():
    data = subprocess.check_output(
        ['mvn', '-q', 'compile', 'exec:java',
         '-Dexec.mainClass=org.khanacademy.logexport.ListSchema'])
    return json.loads(data)


def _merge_schemas(streaming_schema, schema_that_java_will_write):
    """Returns streaming_schema + logs_schema.

    Each entry of foo_schema looks like this:
      {
        "mode": "REQUIRED",         # may be missing
        "name": "url_map_entry",
        "type": "STRING"
      },
    or, for record fields, like this:
      {
        "fields": [...subfields...],
        "mode": "REPEATED",
        "name": "app_logs",
        "type": "RECORD"
      },
    We recurse on the latter.
    """
    streaming_schema = streaming_schema[:]    # make a local copy

    # First, let's get a more efficient representation of streaming_schema.
    streaming_map = {field['name']: field for field in streaming_schema}

    for logs_field in schema_that_java_will_write:
        if logs_field['name'] not in streaming_map:
            streaming_schema.append(logs_field)
        elif logs_field['type'] == 'RECORD':
            # We need to recursively merge the sub-fields of the record.
            streaming_field = streaming_map[logs_field['name']]
            streaming_field['fields'] = _merge_schemas(
                logs_field['fields'], streaming_field['fields'])

    return streaming_schema


def _delete_mode(schema):
    """Delete all but 'REPEATED' mode's from a schema, in place.

    It looks like the streaming schema just makes all fields NULLABLE,
    which is the default, so we ignore the "mode" entry on all logs
    fields unless it is "REPEATED".
    """
    for field in schema:
        if field.get('mode') != "REPEATED":
            field.pop('mode', None)
        if field['type'] == 'RECORD':
            _delete_mode(field['fields'])


def _update_schema(project, table_name, new_schema):
    with tempfile.NamedTemporaryFile(prefix='streaming_schema_') as f:
        json.dump(new_schema, f)
        f.flush()
        # We don't call _bq here because it doesn't return json!
        subprocess.check_call(_BQ + project +
                              ['update', '--schema=%s' % f.name, table_name])


def main(streaming_table, dry_run=False):
    print "Getting existing schema"
    streaming_schema = _schema(_KHAN_ACADEMY_PROJECT, streaming_table)

    print "Getting schema that the java streaming job is expecting"
    schema_that_java_will_write = _schema_from_java()

    print "Merging schemas"
    new_streaming_schema = _merge_schemas(streaming_schema,
                                          schema_that_java_will_write)
    _delete_mode(new_streaming_schema)

    # This diffing is taken from unittest.case.assertSequenceEqual
    print ("New schema: %s"
           % json.dumps(new_streaming_schema, sort_keys=True, indent=4))
    print "Diff:\n"
    print "\n".join(difflib.ndiff(
        pprint.pformat(streaming_schema).splitlines(),
        pprint.pformat(new_streaming_schema).splitlines()))

    if streaming_schema == new_streaming_schema:
        print "No differences, no need to update schema."
    elif dry_run:
        print "Not updating schema, dry-run specified."
    else:
        print "Updating schemas"
        _update_schema(_KHAN_ACADEMY_PROJECT, streaming_table,
                       new_streaming_schema)

    print "DONE"


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help="Show what we would do but don't do it.")
    parser.add_argument('-t', '--table',
                        default='logs_streaming.logs_all_time',
                        help="The streaming-table to update the schema for")
    args = parser.parse_args()

    main(args.table, dry_run=args.dry_run)

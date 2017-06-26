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
import logging
import pprint
import subprocess
import tempfile


_BQ = ['bq', '-q', '--format=json', '--headless']


def _bq(project, command_list):
    """project is likely khan-academy or khanacademy.org:deductive-jet-827."""
    data = subprocess.check_output(_BQ + ['--project_id', project] +
                                   command_list)
    return json.loads(data)


def schema(project, table_name):
    """project is likely khan-academy or khanacademy.org:deductive-jet-827."""
    try:
        data = _bq(project, ['show', table_name])
    except subprocess.CalledProcessError:    # probably 'table does not exist'
        return []
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
        subprocess.check_call(_BQ + ['--project_id', project] +
                              ['update', '--schema=%s' % f.name, table_name])


def _log_diff(new_schema, orig_schema):
    """Print the diff between new_schema and old_schema to stdout."""
    if orig_schema == new_schema:
        return

    logging.info("New schema: %s",
                 json.dumps(new_schema, sort_keys=True, indent=4))
    # This diffing is taken from unittest.case.assertSequenceEqual
    logging.info("Diff:")
    logging.info("\n".join(difflib.ndiff(
        pprint.pformat(orig_schema).splitlines(),
        pprint.pformat(new_schema).splitlines())))


def merge_and_update_schema(project, table, merge_with, dry_run=False):
    """Update the schema of table to include columsn in merge_with.

    After this is run, the table called `table` in project `project`
    will include all the columns it used to have, plus the columns in
    `merge_with` that it did not have before.
    """
    orig_schema = schema(project, table)
    new_schema = _merge_schemas(orig_schema, merge_with)
    _delete_mode(new_schema)

    _log_diff(new_schema, orig_schema)

    if not orig_schema:
        logging.info("Creating a new table, will create a new "
                     "schema automatically.")
    elif new_schema == orig_schema:
        logging.info("Not updating schema, no changes found.")
    elif dry_run:
        logging.info("Not updating schema, dry-run specified.")
    else:
        logging.info("Updating schemas.")
        _update_schema(project, table, new_schema)


def main(project, table, dry_run=False):
    logging.info("Getting schema that the java streaming job is expecting")
    schema_that_java_will_write = _schema_from_java()

    logging.info("Merging and updating existing schema")
    merge_and_update_schema(project, table, schema_that_java_will_write,
                            dry_run)

    logging.info("DONE")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--table',
                        default='khan-academy.logs_streaming.logs_all_time',
                        help=("The streaming-table to update the schema for, "
                              "in the form <project>.<dataset>.<table>. "
                              "Default: %(default)s"))
    parser.add_argument('-n', '--dry-run', action='store_true',
                        help="Show what we would do but don't do it.")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="More verbose output.")
    args = parser.parse_args()

    try:
        (project, dataset, table) = args.table.split('.')
    except ValueError:
        parser.error('--table must be of the form <project>.<dataset>.<table>.'
                     ' Example projects are khanacademy.org:deductive-jet-827'
                     ' and khan-academy.')

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    main(project, '%s.%s' % (dataset, table), dry_run=args.dry_run)

#!/usr/bin/env python

"""A script to update the logs.* and logs_hourly.* bigquery tables.

We have the streaming khan-academy:logs_streaming.logs_all_time
bigquery table, which holds all our logs, but is hard to work with for
analytics.  So we also provide these tables:
   khan:logs.requestlogs_YYYYMMDD
   khan:logs_hourly.requestlogs_YYYYMMDD_HH

This is the script that provides those tables.  It does this by
"copying" recent rows from the streaming logs into the appropriate
logs and logs_hourly tables.  It is expected that this is run once an
hour (a few minutes after the hour) in a cron job.

I wrote "copying" above in scare quotes because we actually have to do
some data munging first.  The reason is that, for managed-vm
("appengine flex") modules, the logs_streaming data does not collate
the request-logs (from nginx) and app logs (from our python app).  In
fact, due to windowing, the app-logs for a single request may be
broken up into multiple bigquery rows in the streaming logs.  This
script connects each request-log row with all its app-logs row in the
streaming logs, and combines them into a single record when writing to
the daily-log/hourly-log tables.
"""

import calendar
import datetime
import json
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time

import update_schema


# The project where _HOURLY_DATASET and _DAILY_DATASET live.
_PROJECT = 'khanacademy.org:deductive-jet-827'
_HOURLY_DATASET = 'logs_hourly'
_DAILY_DATASET = 'hourly'
# TODO(csilvers): remove these next two lines once we're willing to go live!
_HOURLY_DATASET = 'logs_new'
_DAILY_DATASET = 'logs_new'


# This string should be instantiated with a dict with these fields:
#    start_time: the time_t to start this hourly log, in seconds.
#       It should probably be on the hour.  We will log all messages
#       which *finished* >= start_time.
#    end_time: the time_t to end this hourly log, in seconds.
#       It is probably start_time + 3600.  We will log all messages
#       with *finished* < en_time
#    start_ms: the time_t to start reading logs from, in ms.  This should
#       be about 10 minutes before start_time, so we can collect app-logs
#       associated with a request that ended after start_time.
#    end_ms: the time_t to stop reading logs from, in ms.  This needs to be
#       >15 minutes later than end_time to get all the records, and sometimes
#       longer, since for non-recent records the streaming output is chunked
#       at 15-minute intervals.  See
#       https://groups.google.com/a/khanacademy.org/forum/#!topic/infrastructure-team/Y2qG9SH5S3o
# Note that our start_time and end_time and only kinda related to the
# bigquery columns entitled 'start_time' and 'end_time'.  So confusing!
# TODO(csilvers): figure out a way to distinguish vm loglines from non-vm
#                 loglines without hard-coding module names.
_VM_SUBTABLE_QUERY = """\
SELECT *
FROM [khan-academy:logs_streaming.logs_all_time@%(start_ms)s-%(end_ms)s]
WHERE end_time >= %(start_time)s and end_time < %(end_time)s
      AND module_id = 'vm'
"""

_NON_VM_SUBTABLE_QUERY = """\
SELECT *
FROM [khan-academy:logs_streaming.logs_all_time@%(start_ms)s-%(end_ms)s]
WHERE end_time >= %(start_time)s and end_time < %(end_time)s
      AND module_id != 'vm'
"""

# This needs a dict with all the field listed above, *plus*:
#   vm_filtered_temptable: the name given the table created by
#       _VM_SUBTABLE_QUERY.
#   reqlog_fields: all the fields in the streaming schema that come
#       just from the request-logs, as a comma-joined string: that's
#       everything except `app_logs` and fields derived from it, such
#       as `elog_*`.
#   kalog_fields: all the fields derived from `app_logs` -- in particular
#       the KA_LOG app-log line -- as a comma-joined string.
#   concatted_kalog_fields: like kalog_fields, but with every field in
#       the value converted into the string
#       "ANY_VALUE(kalog_line.<kalog_field>) as <kalog_field>"
_VM_MODULES_QUERY = """\
WITH

-- All the lines generated from the request-log.  These lack a thread-id.
request AS (
    SELECT %(reqlog_fields)s
    FROM %(vm_filtered_temptable)s
    WHERE (request_id != "null" AND request_id IS NOT NULL)
           AND (thread_id = "null" or thread_id IS NULL)
),

-- All the lines generated from the app-log.  These have a thread id.
app_log AS (
    SELECT *
    FROM %(vm_filtered_temptable)s
    WHERE thread_id != "null" AND thread_id IS NOT NULL
),

-- The special app-log line that we emit at the beginning of each request
-- that logs the request-id for that request.  This is the app-log line
-- that "links" thread-ids to request-ids.
link_line AS (
    SELECT thread_id, request_id
    FROM app_log
    WHERE request_id != "null" AND request_id IS NOT NULL
),

-- The special app-log line that we emit at the end of each request that
-- we use to generate a bunch of computed log lines like elog_browser.
kalog_line AS (
    SELECT %(kalog_fields)s, thread_id
    FROM app_log
    -- We pick an arbitrary elog-field which is set for every request.
    WHERE elog_country is not null
),

-- And similar for the bigbingo fields.  We hard-code these.
-- TODO(csilvers): get rid of all these special lines (including
-- kalog_line entirely, maybe when we can use legacy sql with
-- table decorators so it doesn't insert a bunch of (null, null) records.
bingo_participation_line AS (
    SELECT bingo_participation_events, thread_id
    FROM app_log
    WHERE bingo_participation_events[SAFE_OFFSET(0)] is not null AND
          bingo_participation_events[SAFE_OFFSET(0)].bingo_id is not null
),
bingo_conversion_line AS (
    SELECT bingo_conversion_events, thread_id
    FROM app_log
    WHERE bingo_conversion_events[SAFE_OFFSET(0)] is not null AND
          bingo_conversion_events[SAFE_OFFSET(0)].bingo_id is not null
),

-- One row for each request, but only for the app-log; we haven't
-- merged with the request-log data yet.  This merges together a bunch
-- of `app_log` lines with the same thread_id, and also all the kalog
-- data.
joined_applog_lines AS (
    SELECT ARRAY_CONCAT_AGG(app_log.app_logs) as app_logs,
           %(concatted_kalog_fields)s,
           ANY_VALUE(bingo_participation_line.bingo_participation_events)
               as bingo_participation_events,
           ANY_VALUE(bingo_conversion_line.bingo_conversion_events)
               as bingo_conversion_events,
           link_line.request_id as request_id
    FROM link_line
    LEFT OUTER JOIN app_log
    USING (thread_id)
    LEFT OUTER JOIN kalog_line
    USING (thread_id)
    LEFT OUTER JOIN bingo_participation_line
    USING (thread_id)
    LEFT OUTER JOIN bingo_conversion_line
    USING (thread_id)
    GROUP BY link_line.request_id
)

-- And the final result: the app-log data merged with the request-log data!
SELECT *
FROM request
LEFT OUTER JOIN joined_applog_lines
USING (request_id)
"""


def _now():
    """Now, as a time_t."""
    return calendar.timegm(datetime.datetime.now().timetuple())


def _hourly_table_name(start_time):
    return start_time.strftime(_HOURLY_DATASET + '.requestlogs_%Y%m%d_%H')


def _sanitize_query(sql_query):
    """Remove newlines and comments from the sql query, for the commandline."""
    return re.sub(r'--.*', '', sql_query).replace('\n', ' ')


def _call_bq(cmd_and_args, **kwargs):
    """cmd_and_args is, e.g., ['query', 'SELECT * from ...']."""
    dry_run = kwargs.pop('dry_run', False)

    cmd = (['bq', '-q', '--headless', '--format', 'none',
            '--project_id', _PROJECT] +
           cmd_and_args)

    if dry_run:
        logging.info("Would run %s", cmd)
        return

    logging.debug("Running %s", cmd)
    start_time = time.time()
    try:
        subprocess.check_call(cmd, **kwargs)
    finally:
        elapsed_time = time.time() - start_time
        logging.debug("bq command ran in %.2f seconds" % elapsed_time)


def _logs_are_up_to_date(start_ms, end_ms, end_time):
    # The second selected field is just to help with debugging.
    query = ('SELECT MAX(end_time) >= %s, INTEGER(MAX(end_time)) '
             'FROM [khan-academy:logs_streaming.logs_all_time@%s-%s]'
             % (end_time, start_ms, end_ms))
    r = subprocess.check_output(['bq', '-q', '--headless', '--format', 'csv',
                                 'query', query])
    if 'true' in r:
        return True
    logging.warning("Insufficient table decorator values: '%s' returned '%s'",
                    query, r)
    return False


def _table_decorator_end_time(end_time):
    """Return a time-in-ms so a table decorator ending then includes end_time.

    For non-recent records, the streaming output seems to be chunked
    in some way.  (Colin suspects that there are actually two queues
    for streaming data, one for very recent stuff and one for less
    recent, and sometimes the latter gets behind.)  See
        https://groups.google.com/a/khanacademy.org/forum/#!topic/infrastructure-team/Y2qG9SH5S3o

    Thus, while in theory a query over logs_streaming@x000-y000
    will return all results between time x and time y, in practice
    it often returns results between time x and about 15 minutes
    before time y.  So if you want to get all results between time
    x and time y, your table decorator (the `@x000-y000` part)
    needs to end about 15 minutes after y.

    In our experience, 15 minutes works well most of the time.
    However, around midnight and noon, even that's not long enough.
    Shrug.  This function figures out the right table decorator
    end time, even if it's in the future (it waits in that case :-) )

    Arguments:
        end_time: the time_t we want the logs to be caught up to
    """
    # We give a few seconds' slack in case there's a long period of time
    # with no queries.
    start_ms = (end_time - 10) * 1000

    # We'll start by checking 16 minutes later.
    # TODO(csilvers): check if this is overkill, maybe end_time plus
    # one minute (or plus 1 second) works well most of the time.
    end_ms = (end_time + 16 * 60) * 1000

    # Things are straightforward until we have to wait until the future...
    while end_ms < _now():
        if _logs_are_up_to_date(start_ms, end_ms, end_time):
            return end_ms
        logging.warning("Reading logs %d seconds past end-time isn't enough, "
                        "trying a later time", (end_ms / 1000 - end_time))
        end_ms += (5 * 60) * 1000

    # If we get here, even having end_ms be the present isn't enough.
    # We will just have to wait a while for more logs to come in.
    logging.info("Waiting for streaming logs to get up to date.")
    for i in xrange(10):
        end_ms = _now() * 1000
        if _logs_are_up_to_date(start_ms, end_ms, end_time):
            return end_ms
        wait = 60 * (1.5 ** i)   # friendly exponential backoff
        logging.warning("Streaming logs not up to date, waiting %ds..." % wait)
        time.sleep(wait)

    logging.fatal("Streaming logs never got up to date.")


def _hourly_logs_seem_complete(start_time):
    """Return True if the given table seems to have all the data it ought.

    We were seeing some problems, in our QA testing, where we compared
    the hourly logs created by this script with ones created by a
    separate process (the logs_to_bigquery mapreduce).  We noticed
    that this log could be missing hundreds of thousands of lines.
    They all -- almost all -- were from the last 5 minutes of the log.
    (This log had plenty of loglines in the last 5 minutes, just not
    all of them.)  We don't understand really how this could happen,
    but we can test against it and fail.  In our experience, trying
    the query again an hour later typically fixes it.
    """
    # In our experience, after a few hours the streaming logs have all
    # the data, so don't even both to check this if the logs are from
    # more than, say, 4-5 hours ago.
    if start_time + datetime.timedelta(hours=5) < datetime.datetime.utcnow():
        return True

    hourly_log_table = _hourly_table_name(start_time)
    query = """\
SELECT "old" as which, count(1) as count
FROM %(table)s
WHERE end_time_timestamp BETWEEN
      TIMESTAMP("%(YYYYMMDD_HH)s:50:00 UTC") AND
      TIMESTAMP("%(YYYYMMDD_HH)s:54:59 UTC")
UNION ALL
SELECT "new" as which, count(1) as count
FROM %(table)s
WHERE end_time_timestamp BETWEEN
      TIMESTAMP("%(YYYYMMDD_HH)s:55:00 UTC") AND
      TIMESTAMP("%(YYYYMMDD_HH)s:59:59 UTC")
""" % {
    'table': hourly_log_table,
    'YYYYMMDD_HH': start_time.strftime("%Y-%m-%d %H")
}

    results = subprocess.check_output(
        ['bq', '-q', '--headless', '--format', 'json',
         '--project_id', _PROJECT,
         'query', '--nouse_legacy_sql', query])
    results = json.loads(results)

    old_count = next(r['count'] for r in results if r['which'] == 'old')
    new_count = next(r['count'] for r in results if r['which'] == 'new')
    difference = (new_count - old_count) * 100.0 / old_count

    # We expect some difference, but if it's more than a few percent
    # that indicates a problem.
    if difference >= 5:
        logging.error("Error reading from streaming logs: The last 5 "
                      "minutes of the logs have %.2f%% fewer "
                      "loglines than the preceding 5 minutes",
                      hourly_log_table, difference)
        return False
    return True


def _next_hourly_table_time():
    """The smallest datetime we don't have a table for in logs_hourly.

    If the most recent table is more than 7 days ago, returns the
    beginning of the day 6 days ago, which is about as long back as
    you can query the streaming logs efficiently.

    If the logs_hourly dataset is empty, returns midnight today.  (We
    could have picked any value up to 7 days ago, but this seemed like
    a reasonable boostrapping value.)
    """
    today = datetime.datetime.utcnow()
    midnight = datetime.datetime(today.year, today.month, today.day)
    output = subprocess.check_output(['bq', '-q', '--headless',
                                      '--format', 'json',
                                      '--project_id', _PROJECT,
                                      'ls', '-n', '100000', _HOURLY_DATASET])
    if not output:
        return midnight

    all_hourly_tables = json.loads(output)
    table_names = [d['tableId'] for d in all_hourly_tables
                   if d['tableId'].startswith('requestlogs_')]
    latest_table = max(table_names)

    table_time = datetime.datetime.strptime(latest_table,
                                            'requestlogs_%Y%m%d_%H')
    next_table_time = table_time + datetime.timedelta(hours=1)
    six_days_ago = midnight - datetime.timedelta(days=6)
    return max(next_table_time, six_days_ago)


def _create_hourly_table(start_time, interactive=False, dry_run=False):
    """Copy an hour's worth of logs from streaming to a new table.

    This stores logs for all requests that *ended* between
    start_time (inclusive) and start_time + 1 hour (exclusive).
    Each request is a single row in the output table.  start_time
    should be a datetime at an hour boundary.  It is interpreted as
    a UTC time (everything about logs is UTC-only).

    This raises an error if the table already exists.
    """
    daily_table = start_time.strftime(_DAILY_DATASET + '.requestlogs_%Y%m%d')
    hourly_table = _hourly_table_name(start_time)
    # We use a random number to avoid collisions if we try to process the
    # same hour multiple times.
    vm_subtable = start_time.strftime(_HOURLY_DATASET + '.subquery_%Y%m%d_%H' +
                                      '__%s' % random.randint(0, 9999))

    streaming_schema = update_schema.schema('khan-academy',
                                            'logs_streaming.logs_all_time')
    # This is the field that we take from the app-log.
    applog_fields = ['app_logs']
    # These are fields that we derive from the 'KALOG' app-log logline.
    kalog_fields = sorted(f['name'] for f in streaming_schema
                           if f['name'].startswith('elog_'))
    bingo_fields = sorted(f['name'] for f in streaming_schema
                           if f['name'].startswith('bingo_'))
    # These are fields that we take from the request-log.
    reqlog_fields = sorted(f['name'] for f in streaming_schema
                           if f['name'] not in (applog_fields + kalog_fields +
                                                bingo_fields))

    start_time_t = calendar.timegm(start_time.timetuple())
    end_time_t = start_time_t + 3600
    sql_dict = {
        'start_time': start_time_t,
        'end_time': end_time_t,
        'start_ms': (start_time_t - 10 * 60) * 1000,
        'end_ms': _table_decorator_end_time(end_time_t),
        'vm_filtered_temptable': vm_subtable,
        'reqlog_fields': ', '.join(reqlog_fields),
        'kalog_fields': ', '.join(kalog_fields),
        'concatted_kalog_fields': ', '.join(
            ["ANY_VALUE(kalog_line.%s) as %s" % (f, f) for f in kalog_fields]),
    }

    # If the hourly table already exists, then we have a noop.
    try:
        with open(os.devnull, 'w') as devnull:
            _call_bq(['show', hourly_table], stdout=devnull, stderr=devnull)
        logging.warning("Skipping %s -- already exists", hourly_table)
        return
    except subprocess.CalledProcessError:    # means 'table does not exist'
        pass

    # We need to `mk` our temp-table so we can give it an expiry.
    # We do this even in dry-run mode so the hourly table doesn't
    # fail due to a missing subquery table.
    _call_bq(['mk', '--expiration', str(50 * 60), '-t', vm_subtable])

    _BQ_QUERY = ['query', '--allow_large_results', '--noflatten']
    if not interactive:
        _BQ_QUERY.append('--batch')
    if dry_run:
        _BQ_QUERY.append('--dry_run')

    # Create the temp-table that just holds the Managed VM loglines.
    _call_bq(_BQ_QUERY +
             ['--destination_table', vm_subtable,
              _sanitize_query(_VM_SUBTABLE_QUERY % sql_dict)])

    # Create the hourly table in two steps.  (Ideally we'd just do one
    # step so creating the hourly table was atomic, but sadly we can't
    # use legacy sql for the [very complicated] vm-modules query, and
    # standard sql doesn't support table decorators yet.)
    # The non-vm modules come first; they're very simple.
    logging.info("Creating a new hourly table: %s", hourly_table)
    logging.info("-- adding logs from non-vm modules")
    _call_bq(_BQ_QUERY +
             ['--destination_table', hourly_table,
              _sanitize_query(_NON_VM_SUBTABLE_QUERY % sql_dict)])

    logging.info("-- adding logs from vm modules")
    _call_bq(_BQ_QUERY +
             ['--append', '--nouse_legacy_sql',
              '--destination_table', hourly_table,
              _sanitize_query(_VM_MODULES_QUERY % sql_dict)])

    # Sanity check on the hourly logs.
    if not _hourly_logs_seem_complete(start_time):
        logging.error("Deleting %s", hourly_table)
        _remove_tables_at_time(start_time)
        raise RuntimeError("%s seems to be incomplete" % hourly_table)

    # Call update_schema to make sure that the daily table has all the
    # columns the hourly table does, in case some just got added.
    # Otherwise the `cp` command below will fail.  (This doesn't work
    # in dry_run mode, where the hourly table was never created.)
    if not dry_run:
        hourly_table_schema = update_schema.schema(_PROJECT, hourly_table)
        update_schema.merge_and_update_schema(_PROJECT, daily_table,
                                              merge_with=hourly_table_schema)

    # Update the daily table.
    logging.info("Updating daily table: %s", daily_table)
    _call_bq(['cp', '--append_table', hourly_table, daily_table],
             dry_run=dry_run)


def setup_logging(verbose):
    """Log DEBUG/INFO to stdout, WARNING/ERROR to stderr.

    The reason we care is this is run from cron, which will send stdout
    to a logfile and stderr out as mail.

    This will cause us to log WARNING/ERROR twice when run from the
    commandline (once to stdout and once to stderr), but it seems
    worth it to get the errors in the logfile as well as in email.

    This is based of an idea at
    http://stackoverflow.com/questions/2302315/how-can-info-and-debug-logging-message-be-sent-to-stdout-and-higher-level-messag
    """
    logger = logging.getLogger()
    # Have to set the root logger level, it defaults to logging.WARNING.
    logger.setLevel(logging.NOTSET)

    logs_format = '[%(asctime)s %(levelname)s] %(message)s'
    formatter = logging.Formatter(logs_format)

    logging_handler_out = logging.StreamHandler(sys.stdout)
    logging_handler_out.setLevel(logging.DEBUG if verbose else logging.INFO)
    logging_handler_out.setFormatter(formatter)
    logger.addHandler(logging_handler_out)

    logging_handler_err = logging.StreamHandler(sys.stderr)
    logging_handler_err.setLevel(logging.WARNING)
    logging_handler_err.setFormatter(formatter)
    logger.addHandler(logging_handler_err)


def _remove_tables_at_time(table_time):
    table = _hourly_table_name(table_time)
    try:
        _call_bq(['rm', '-t', '-f', table])
    except subprocess.CalledProcessError:
        # It didn't exist or we couldn't delete it.
        pass


def _signal_handler(signal_number, _stackframe):
    signals_to_names = dict((getattr(signal, n), n)
        for n in dir(signal) if n.startswith('SIG') and '_' not in n)

    # Throw an exception from the signal_handler to hit the catch clause.
    raise RuntimeError("Caught signal %s." % signals_to_names[signal_number])


def main(interactive, dry_run):
    """Populate any hourly and daily tables that still need it."""
    now = datetime.datetime.utcnow()
    start_of_this_hour = datetime.datetime(now.year, now.month, now.day,
                                           now.hour)
    # TODO(csilvers): deal properly with the case where the hourly table
    # was created but crashed before updating the daily table.
    next_hourly_table_time = _next_hourly_table_time()
    while next_hourly_table_time < start_of_this_hour:
        printable_time = next_hourly_table_time.ctime()

        logging.info("Processing logs at %s (UTC)", printable_time)
        try:
            _create_hourly_table(next_hourly_table_time, interactive, dry_run)
        except Exception as e:
            start_time = time.time()
            logging.exception("Error creating tables for "
                "%s, deleting it to be safe: %s" % (printable_time, e))
            _remove_tables_at_time(next_hourly_table_time)

            the_rest_of_four_minutes = start_time + (4 * 60) - time.time()
            if the_rest_of_four_minutes > 0:
                logging.info("Waiting %.2f minutes to try and let daily_table"
                    " finish updating.", the_rest_of_four_minutes / 60)
                time.sleep(the_rest_of_four_minutes)
            return

        logging.info("DONE processing logs at %s (UTC)", printable_time)

        next_hourly_table_time += datetime.timedelta(hours=1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--interactive', action='store_true',
                        help="Use interactive mode instead of batch (faster)")
    parser.add_argument('-n', '--dry-run', action='store_true',
                        help="Show what we would do but don't do it.")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="More verbose output.")
    args = parser.parse_args()

    setup_logging(args.verbose)

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    main(args.interactive, args.dry_run)

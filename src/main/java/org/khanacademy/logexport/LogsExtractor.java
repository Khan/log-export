package org.khanacademy.logexport;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.logging.v2.model.LogEntry;
import com.google.api.services.logging.v2.model.LogLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Top-level logic class for log transformation. The work is subdivided into four top-level
 * components:
 * - The "standard log fields" that are provided directly by GAE, except the app logs.
 * - The app logs.
 * - The event log information, which is extracted from the app logs.
 * - The bingo events, which are also extracted from the app logs.
 */
public class LogsExtractor {
    private final StandardLogFieldParser mStandardLogFieldParser;
    private final AppLogParser mAppLogParser;
    private final EventLogParser mEventLogParser;
    private final BingoEventParser mBingoEventParser;

    public LogsExtractor(StandardLogFieldParser standardLogFieldParser, AppLogParser appLogParser,
                         EventLogParser eventLogParser, BingoEventParser bingoEventParser) {
        mStandardLogFieldParser = standardLogFieldParser;
        mAppLogParser = appLogParser;
        mEventLogParser = eventLogParser;
        mBingoEventParser = bingoEventParser;
    }

    public static LogsExtractor create() {
        return new LogsExtractor(new StandardLogFieldParser(), new AppLogParser(),
                new EventLogParser(), new BingoEventParser());
    }

    public List<TableFieldSchema> getSchemaFields() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.addAll(mStandardLogFieldParser.getSchemaFields());
        fields.add(mAppLogParser.getSchemaField());
        fields.addAll(mEventLogParser.getSchemaFields());
        fields.addAll(mBingoEventParser.getSchemaFields());
        return fields;
    }

    public TableSchema getSchema() {
        return new TableSchema().setFields(getSchemaFields());
    }

    public TableRow extractLogs(LogEntry logEntry) throws IOException {
        TableRow row = new TableRow();
        mStandardLogFieldParser.populateStandardLogFields(row, logEntry);
        List<LogLine> logLines = mAppLogParser.getLogLines(logEntry);
        mAppLogParser.populateAppLogField(row, logLines);
        mEventLogParser.populateEventLogFields(row, logLines);
        mBingoEventParser.populateBingoEventFields(row, logLines);
        return row;
    }
}

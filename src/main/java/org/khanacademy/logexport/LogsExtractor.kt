package org.khanacademy.logexport

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.api.services.logging.v2beta1.model.LogEntry

import java.io.IOException
import java.util.ArrayList

/**
 * Top-level logic class for log transformation. The work is subdivided into four top-level
 * components:
 * - The "standard log fields" that are provided directly by GAE, except the app logs.
 * - The app logs.
 * - The event log information, which is extracted from the app logs.
 * - The bingo events, which are also extracted from the app logs.
 */
class LogsExtractor(private val mStandardLogFieldParser: StandardLogFieldParser, private val mAppLogParser: AppLogParser,
                    private val mEventLogParser: EventLogParser, private val mBingoEventParser: BingoEventParser) {

    val schema: TableSchema
        get() {
            val fields = ArrayList<TableFieldSchema>()
            fields.addAll(mStandardLogFieldParser.schemaFields)
            fields.add(mAppLogParser.schemaField)
            fields.addAll(mEventLogParser.schemaFields)
            fields.addAll(mBingoEventParser.schemaFields)
            return TableSchema().setFields(fields)
        }

    @Throws(IOException::class)
    fun extractLogs(logEntry: LogEntry): TableRow {
        val row = TableRow()
        mStandardLogFieldParser.populateStandardLogFields(row, logEntry)
        val logLines = mAppLogParser.getLogLines(logEntry)
        mAppLogParser.populateAppLogField(row, logLines)
        mEventLogParser.populateEventLogFields(row, logLines)
        mBingoEventParser.populateBingoEventFields(row, logLines)
        return row
    }

    companion object {

        fun create(): LogsExtractor {
            return LogsExtractor(StandardLogFieldParser(), AppLogParser(),
                    EventLogParser(), BingoEventParser())
        }
    }
}

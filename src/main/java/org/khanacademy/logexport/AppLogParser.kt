package org.khanacademy.logexport

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogEntry
import com.google.api.services.logging.v2beta1.model.LogLine
import com.google.api.services.logging.v2beta1.model.SourceLocation
import com.google.common.collect.ImmutableList
import org.khanacademy.logexport.Schemas.Type

/**
 * Parser for the app_logs (or protoPayload.line) field.
 */
class AppLogParser {
    val schemaField: TableFieldSchema
        get() = Schemas.repeatedRecord("app_logs",
                Schemas.field("time", Type.FLOAT),
                Schemas.field("time_timestamp", Type.TIMESTAMP),
                Schemas.field("level", Type.INTEGER),
                Schemas.field("message", Type.STRING))

    /**
     * Extract the app log contents in LogLine format. Unfortunately, the JSON parser doesn't seem
     * to do that for us, even though it recognizes most other Google API JSON types.
     */
    fun getLogLines(logEntry: LogEntry): List<LogLine> {
        val resultBuilder = ImmutableList.builder<LogLine>()
        // This is an unchecked cast because of type erasure.  Annoying.  But the alternative of checking each element
        // is awful, and it's not clear to me that we actually want to continue processing if we're suddenly getting
        // logs in a different format anyway...
        val rawLines = logEntry.protoPayload["line"] as? List<Map<String, Any>>
        if (rawLines != null) {
            for (rawLine in rawLines) {
                val logLine = LogLine()
                rawLine.entries.forEach { entry ->
                    // For reasons I don't understand, we've started receiving
                    // logs with a sourceLocation field with a different type
                    // than the one we require.  This code handles the type
                    // conversions.
                    // TODO(colin): figure out why there is this mismatch and if
                    // there is a better fix
                    if (entry.key == "sourceLocation") {
                        val srcLoc = SourceLocation()
                        val entryValue = entry.value as? Map<String, Any>
                        if (entryValue != null) {
                            entryValue.entries.forEach { srcLocEntry ->
                                val value = srcLocEntry.value
                                if (srcLocEntry.key == "line" && value is String) {
                                    srcLoc.set(srcLocEntry.key, java.lang.Long.parseLong(value))
                                } else {
                                    srcLoc.set(srcLocEntry.key, value)
                                }
                            }
                        }
                        logLine.set(entry.key, srcLoc)
                    } else {
                        logLine.set(entry.key, entry.value)
                    }
                }
                resultBuilder.add(logLine)
            }
        }
        return resultBuilder.build()
    }

    fun populateAppLogField(row: TableRow, logLines: List<LogLine>) {
        val appLogs = logLines.map({ logLine: LogLine ->
            mapOf(
                "time" to LogParsingUtils.dateToSeconds(logLine.getTime()),
                "time_timestamp" to logLine.getTime(),
                "level" to LogParsingUtils.parseSeverity(logLine.getSeverity()),
                "message" to logLine.getLogMessage()
            )
        })
        row.set("app_logs", appLogs)
    }
}

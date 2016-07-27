package org.khanacademy.logexport

import com.google.api.client.util.ArrayMap
import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogEntry
import com.google.api.services.logging.v2beta1.model.LogLine
import com.google.api.services.logging.v2beta1.model.SourceLocation
import com.google.common.collect.ImmutableList
import org.khanacademy.logexport.Schemas.Type
import java.util.stream.Collectors

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
        val rawLinesObject = logEntry.protoPayload["line"]
        if (rawLinesObject is List<Any>) {
            @SuppressWarnings("unchecked")
            val rawLines = rawLinesObject as List<Map<String, Any>>
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
                        // Unfortunately we need to do a few unchecked casts
                        // here, since we get values as plain objects and need
                        // them as more specifically typed things.  When setting
                        // the values, there appears to be some built-in runtime
                        // type checking, so at least we will still know quickly
                        // if types are wrong.
                        entry.value.entries.forEach { srcLocEntry ->
                            val value = srcLocEntry.value
                            if (srcLocEntry.key == "line") {
                                srcLoc.set(srcLocEntry.key, java.lang.Long.parseLong(value))
                            } else {
                                srcLoc.set(srcLocEntry.key, value)
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
        val appLogs = logLines.stream().map({ logLine ->
            val appLogMap = ArrayMap<String, Any>()
            appLogMap.put("time", LogParsingUtils.dateToSeconds(logLine.getTime()))
            appLogMap.put("time_timestamp", logLine.getTime())
            appLogMap.put("level", LogParsingUtils.parseSeverity(logLine.getSeverity()))
            appLogMap.put("message", logLine.getLogMessage())
            appLogMap
        }).collect(Collectors.toList<ArrayMap<String, Any>>())
        row.set("app_logs", appLogs)
    }
}

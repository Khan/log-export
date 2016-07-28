package org.khanacademy.logexport

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogEntry
import com.google.common.collect.ImmutableList
import org.khanacademy.logexport.Schemas.Type
import java.util.stream.Collectors

/**
 * Parser for the normal log fields, independent of any extensibility points, except for app_logs,
 * which is special.
 */
class StandardLogFieldParser {

    val schemaFields: List<TableFieldSchema>
        get() = ALL_FIELDS
                .map({ field -> TableFieldSchema().setName(field.bigQueryName).setType(field.type.toString()) })

    fun populateStandardLogFields(row: TableRow, logEntry: LogEntry) {
        for (field in ALL_FIELDS) {
            var value: Any? = logEntry.protoPayload[field.protoPayloadName]
            if (field.transformer != null) {
                value = field.transformer.invoke(value)
            }
            if (value != null) {
                row.set(field.bigQueryName, value)
            }
        }
    }

    private class StandardLogField(val bigQueryName: String, val type: Type, val protoPayloadName: String,
                                   val transformer: ((Any?) -> Any?)?)

    companion object {
        private val ALL_FIELDS = ImmutableList.of(
                logField("ip", Type.STRING, "ip"),
                logField("nickname", Type.STRING, "nickname"),
                logField("start_time", Type.FLOAT, "startTime", { LogParsingUtils.dateToSeconds(it) }),
                logField("start_time_timestamp", Type.TIMESTAMP, "startTime"),
                logField("end_time", Type.FLOAT, "endTime", { LogParsingUtils.dateToSeconds(it) }),
                logField("end_time_timestamp", Type.TIMESTAMP, "endTime"),
                logField("method", Type.STRING, "method"),
                logField("resource", Type.STRING, "resource"),
                logField("http_version", Type.STRING, "httpVersion"),
                logField("status", Type.INTEGER, "status"),
                logField("response_size", Type.INTEGER, "responseSize"),
                logField("referrer", Type.STRING, "referrer"),
                logField("user_agent", Type.STRING, "userAgent"),
                logField("host", Type.STRING, "host"),
                logField("latency", Type.FLOAT, "latency", { LogParsingUtils.parseDuration(it) }),
                logField("pending_time", Type.FLOAT, "pendingTime", { LogParsingUtils.parseDuration(it) }),
                logField("mcycles", Type.INTEGER, "megaCycles"),
                logField("cost", Type.FLOAT, "cost"),
                logField("task_queue_name", Type.STRING, "taskQueueName"),
                logField("task_name", Type.STRING, "taskName"),
                logField("instance_key", Type.STRING, "instanceId"),
                logField("module_id", Type.STRING, "moduleId"),
                logField("version_id", Type.STRING, "versionId"),
                logField("request_id", Type.STRING, "requestId"),
                logField("replica_index", Type.INTEGER, "instanceIndex"),
                logField("url_map_entry", Type.STRING, "urlMapEntry"),
                logField("was_loading_request", Type.BOOLEAN, "wasLoadingRequest"))

        private fun logField(
                bigQueryName: String, type: Type, logEntryName: String,
                transformer: ((Any?) -> Any?)?  = null): StandardLogField {
            return StandardLogField(bigQueryName, type, logEntryName, transformer)
        }
    }
}

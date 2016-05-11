package org.khanacademy.logexport;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.logging.v2beta1.model.LogEntry;
import com.google.common.collect.ImmutableList;
import org.khanacademy.logexport.Schemas.Type;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parser for the normal log fields, independent of any extensibility points, except for app_logs,
 * which is special.
 */
public class StandardLogFieldParser {
    private static List<StandardLogField> ALL_FIELDS = ImmutableList.of(
            logField("ip", Type.STRING, "ip"),
            logField("nickname", Type.STRING, "nickname"),
            logField("start_time", Type.FLOAT, "startTime", LogParsingUtils::dateToSeconds),
            logField("start_time_timestamp", Type.TIMESTAMP, "startTime"),
            logField("end_time", Type.FLOAT, "endTime", LogParsingUtils::dateToSeconds),
            logField("end_time_timestamp", Type.TIMESTAMP, "endTime"),
            logField("method", Type.STRING, "method"),
            logField("resource", Type.STRING, "resource"),
            logField("http_version", Type.STRING, "httpVersion"),
            logField("status", Type.INTEGER, "status"),
            logField("response_size", Type.INTEGER, "responseSize"),
            logField("referrer", Type.STRING, "referrer"),
            logField("user_agent", Type.STRING, "userAgent"),
            logField("host", Type.STRING, "host"),
            logField("latency", Type.FLOAT, "latency", LogParsingUtils::parseDuration),
            logField("pending_time", Type.FLOAT, "pendingTime", LogParsingUtils::parseDuration),
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
            logField("was_loading_request", Type.BOOLEAN, "wasLoadingRequest")
    );

    public List<TableFieldSchema> getSchemaFields() {
        return ALL_FIELDS.stream()
                .map(field -> new TableFieldSchema()
                        .setName(field.bigQueryName).setType(field.type.toString()))
                .collect(Collectors.toList());
    }

    public void populateStandardLogFields(TableRow row, LogEntry logEntry) {
        for (StandardLogField field : ALL_FIELDS) {
            Object value = logEntry.getProtoPayload().get(field.protoPayloadName);
            if (field.transformer != null) {
                value = field.transformer.transform(value);
            }
            if (value != null) {
                row.set(field.bigQueryName, value);
            }
        }
    }

    private static StandardLogField logField(
            String bigQueryName, Type type, String logEntryName) {
        return logField(bigQueryName, type, logEntryName, null);
    }

    private static StandardLogField logField(
            String bigQueryName, Type type, String logEntryName,
            @Nullable Transformer transformer) {
        return new StandardLogField(bigQueryName, type, logEntryName, transformer);
    }

    private static class StandardLogField {
        private final String bigQueryName;
        private final Type type;
        private final String protoPayloadName;
        private final @Nullable Transformer transformer;

        public StandardLogField(String bigQueryName, Type type, String protoPayloadName,
                                @Nullable Transformer transformer) {
            this.bigQueryName = bigQueryName;
            this.type = type;
            this.protoPayloadName = protoPayloadName;
            this.transformer = transformer;
        }
    }

    /**
     * Optional transformation to make on a field value before sending to BigQuery.
     */
    private interface Transformer {
        @Nullable Object transform(Object object);
    }
}

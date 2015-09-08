package org.khanacademy.logexport;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.logging.model.LogLine;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.khanacademy.logexport.Schemas.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Parse event log values, both categorized events and unique-occurence keys, as defined by
 * event_log.py in webapp.
 */
public class EventLogParser {
    private static final Logger LOG = LoggerFactory.getLogger(EventLogParser.class);
    private static final String EVENT_LOG_PREFIX = "KALOG";

    // TODO(alan): Make it easier to keep these values in sync with webapp. For example, we could
    // have webapp output a JSON dump of this data and populate these values from the JSON.
    private static List<EventLogField> UNIQUE_OCCURRENCE_KEYS = ImmutableList.of(
            eventLogField("KA_APP", Type.BOOLEAN),
            eventLogField("app_version", Type.STRING),
            eventLogField("browser", Type.STRING),
            eventLogField("country", Type.STRING),
            eventLogField("device_brand", Type.STRING),
            eventLogField("device_name", Type.STRING),
            eventLogField("language", Type.STRING),
            eventLogField("orig_request_id", Type.STRING),
            eventLogField("os", Type.STRING),
            eventLogField("pageload", Type.BOOLEAN),
            eventLogField("retries", Type.INTEGER),
            eventLogField("session_id", Type.STRING),
            eventLogField("session_start", Type.INTEGER),
            eventLogField("touch", Type.BOOLEAN),
            eventLogField("url_route", Type.STRING),
            eventLogField("user_bingo_id", Type.STRING),
            eventLogField("user_first_visit_date", Type.INTEGER),
            eventLogField("user_is_parent", Type.BOOLEAN),
            eventLogField("user_is_phantom", Type.BOOLEAN),
            eventLogField("user_is_registered", Type.BOOLEAN),
            eventLogField("user_is_teacher", Type.BOOLEAN),
            eventLogField("user_joined_date", Type.INTEGER),
            eventLogField("user_kaid", Type.STRING),
            eventLogField("user_phantom_creation_date", Type.INTEGER)
    );

    private static Map<String, EventLogField> UNIQUE_OCCURRENCE_KEYS_BY_NAME =
            FluentIterable.from(UNIQUE_OCCURRENCE_KEYS)
                    .uniqueIndex(field -> field.name);

    private static List<EventLogField> CATEGORIES = ImmutableList.of(
            eventLogField("bingo.", Type.STRING),
            eventLogField("content_survey.", Type.STRING),
            eventLogField("id.", Type.STRING),
            eventLogField("stats.bingo.", Type.INTEGER),
            eventLogField("stats.english_visibility.", Type.FLOAT),
            eventLogField("stats.logging.", Type.INTEGER),
            eventLogField("stats.rpc.", Type.INTEGER),
            eventLogField("stats.rpc_info.", Type.STRING),
            eventLogField("stats.rpc_ops.", Type.INTEGER),
            eventLogField("stats.search.", Type.INTEGER),
            eventLogField("stats.time.", Type.INTEGER)
    );

    public List<TableFieldSchema> getSchemaFields() {
        ImmutableList.Builder<TableFieldSchema> resultBuilder = ImmutableList.builder();
        for (EventLogField field : UNIQUE_OCCURRENCE_KEYS) {
            resultBuilder.add(Schemas.field(field.columnName, field.type));
        }
        for (EventLogField category : CATEGORIES) {
            resultBuilder.add(Schemas.repeatedRecord(category.columnName,
                    Schemas.field("key", Type.STRING),
                    Schemas.field("value", category.type)));
        }
        return resultBuilder.build();
    }

    public void populateEventLogFields(TableRow row, List<LogLine> logLines) {
        for (LogLine line : logLines) {
            String logMessage = line.getLogMessage();
            if (logMessage == null || !logMessage.startsWith(EVENT_LOG_PREFIX)) {
                continue;
            }
            for (String eventString : logMessage.split(";")) {
                if (eventString.isEmpty() || eventString.equals(EVENT_LOG_PREFIX)) {
                    continue;
                }
                String[] components = eventString.split(":");
                String key = components[0];
                // Some events don't have a value, so we consider them to be the "true" boolean.
                String value = components.length >= 2 ? components[1] : Boolean.toString(true);
                processKeyValuePair(row, key, value);
            }
        }
    }

    private void processKeyValuePair(TableRow row, String key, String value) {
        // TODO(alan): Doing a startsWith for every category for every event is kind of expensive.
        // Maybe profile this code and see if it's worth doing something more clever.
        Optional<EventLogField> foundCategory =
                CATEGORIES.stream().filter(category -> category.name.startsWith(key)).findFirst();

        if (foundCategory.isPresent()) {
            EventLogField eventLogField = foundCategory.get();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> jsonEvents =
                    (List<Map<String, Object>>) row.get(eventLogField.columnName);
            if (jsonEvents == null) {
                jsonEvents = new ArrayList<>();
                row.set(eventLogField.columnName, jsonEvents);
            }
            ArrayMap<String, Object> newEvent = new ArrayMap<>();
            newEvent.put("key", key);
            newEvent.put("value", eventLogField.parseValue(value));
            jsonEvents.add(newEvent);
        } else {
            EventLogField eventLogField = UNIQUE_OCCURRENCE_KEYS_BY_NAME.get(key);
            if (eventLogField != null) {
                Object oldValue = row.put(
                        eventLogField.columnName, eventLogField.parseValue(value));
                if (oldValue != null) {
                    LOG.error("Unexpected duplicate result for key " + eventLogField.name);
                }
            }
        }
    }

    private static EventLogField eventLogField(String name, Type type) {
        String columnName = "elog_" + name.replaceAll("\\.$", "").replaceAll("\\.", "_");
        return new EventLogField(name, type, columnName);
    }

    private static class EventLogField {
        public final String name;
        public final Type type;
        public final String columnName;

        public EventLogField(String name, Type type, String columnName) {
            this.name = name;
            this.type = type;
            this.columnName = columnName;
        }

        public Object parseValue(String value) {
            try {
                String decodedValue = URLDecoder.decode(value, "UTF-8");
                switch (type) {
                    case STRING:
                        return decodedValue;
                    case INTEGER:
                        return Long.parseLong(decodedValue);
                    case FLOAT:
                        return new BigDecimal(decodedValue);
                    case BOOLEAN:
                        return Boolean.parseBoolean(decodedValue);
                    case TIMESTAMP:
                    case RECORD:
                        // Fall through to failure case.
                }
                throw new UnsupportedOperationException("Unsupported event log type: " + type);
            } catch (UnsupportedEncodingException | NumberFormatException e) {
                return null;
            }
        }
    }
}

package org.khanacademy.logexport;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.logging.model.LogLine;
import com.google.common.collect.ImmutableList;
import org.khanacademy.logexport.Schemas.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse bingo events, as logged by bigbingo/log.py in webapp.
 */
public class BingoEventParser {
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final String PARTICIPATION_EVENT_PREFIX = "BINGO_PARTICIPATION_EVENT:";
    private static final String CONVERSION_EVENT_PREFIX = "BINGO_CONVERSION_EVENT:";

    public List<TableFieldSchema> getSchemaFields() {
        return ImmutableList.of(
                Schemas.repeatedRecord("bingo_participation_events",
                        Schemas.field("bingo_id", Type.STRING),
                        Schemas.field("experiment", Type.STRING),
                        Schemas.field("alternative", Type.STRING)),
                Schemas.repeatedRecord("bingo_conversion_events",
                        Schemas.field("bingo_id", Type.STRING),
                        Schemas.field("conversion", Type.STRING),
                        Schemas.field("extra", Type.STRING))
        );
    }

    public void populateBingoEventFields(TableRow row, List<LogLine> logLines) {
        List<Object> participationEvents = new ArrayList<>();
        List<Object> conversionEvents = new ArrayList<>();

        for (LogLine logLine : logLines) {
            String logMessage = logLine.getLogMessage();
            if (logMessage == null) {
                continue;
            }
            if (logMessage.startsWith(PARTICIPATION_EVENT_PREFIX)) {
                String eventJson = logMessage.substring(PARTICIPATION_EVENT_PREFIX.length());
                participationEvents.add(parseEventJson(eventJson));
            } else if (logMessage.startsWith(CONVERSION_EVENT_PREFIX)) {
                String eventJson = logMessage.substring(CONVERSION_EVENT_PREFIX.length());
                conversionEvents.add(parseEventJson(eventJson));
            }
        }

        row.set("bingo_participation_events", participationEvents);
        row.set("bingo_conversion_events", conversionEvents);
    }

    /**
     * Turn the JSON string from a bingo event log line into a JSON value ready for BigQuery import.
     * Conveniently, the two JSON formats are the same, so we can just do regular JSON parsing and
     * get exactly the value we want.
     */
    private Object parseEventJson(String eventJson) {
        try {
            return JSON_FACTORY.fromString(eventJson, Object.class);
        } catch (IOException e) {
            return null;
        }
    }
}

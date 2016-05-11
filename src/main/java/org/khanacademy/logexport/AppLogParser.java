package org.khanacademy.logexport;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.logging.v2beta1.model.LogEntry;
import com.google.api.services.logging.v2beta1.model.LogLine;
import com.google.api.services.logging.v2beta1.model.SourceLocation;
import com.google.common.collect.ImmutableList;
import org.khanacademy.logexport.Schemas.Type;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parser for the app_logs (or protoPayload.line) field.
 */
public class AppLogParser {
    public TableFieldSchema getSchemaField() {
        return Schemas.repeatedRecord("app_logs",
                Schemas.field("time", Type.FLOAT),
                Schemas.field("time_timestamp", Type.TIMESTAMP),
                Schemas.field("level", Type.INTEGER),
                Schemas.field("message", Type.STRING));
    }

    /**
     * Extract the app log contents in LogLine format. Unfortunately, the JSON parser doesn't seem
     * to do that for us, even though it recognizes most other Google API JSON types.
     */
    public List<LogLine> getLogLines(LogEntry logEntry) {
        ImmutableList.Builder<LogLine> resultBuilder = ImmutableList.builder();
        Object rawLinesObject = logEntry.getProtoPayload().get("line");
        if (rawLinesObject instanceof List) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> rawLines = (List<Map<String, Object>>) rawLinesObject;
            for (Map<String, Object> rawLine : rawLines) {
                LogLine logLine = new LogLine();
                rawLine.entrySet().forEach(entry -> {
                    // For reasons I don't understand, we've started receiving
                    // logs with a sourceLocation field with a different type
                    // than the one we require.  This code handles the type
                    // conversions.
                    // TODO(colin): figure out why there is this mismatch and if
                    // there is a better fix
                    if (entry.getKey().equals("sourceLocation")) {
                        SourceLocation srcLoc = new SourceLocation();
                        // Unfortunately we need to do a few unchecked casts
                        // here, since we get values as plain objects and need
                        // them as more specifically typed things.  When setting
                        // the values, there appears to be some built-in runtime
                        // type checking, so at least we will still know quickly
                        // if types are wrong.
                        @SuppressWarnings("unchecked")
                        Map<String, Object> rawLoc = (Map<String, Object>) entry.getValue();
                        rawLoc.entrySet().forEach(srcLocEntry -> {
                            Object value = srcLocEntry.getValue();
                            if (srcLocEntry.getKey().equals("line")) {
                                @SuppressWarnings("unchecked")
                                String lineString = (String) value;
                                srcLoc.set(srcLocEntry.getKey(), Long.parseLong(lineString));
                            } else {
                                srcLoc.set(srcLocEntry.getKey(), value);
                            }
                        });
                        logLine.set(entry.getKey(), srcLoc);
                    } else {
                        logLine.set(entry.getKey(), entry.getValue());
                    }
                });
                resultBuilder.add(logLine);
            }
        }
        return resultBuilder.build();
    }

    public void populateAppLogField(TableRow row, List<LogLine> logLines) {
        List<ArrayMap<String, Object>> appLogs = logLines.stream().map(logLine -> {
            ArrayMap<String, Object> appLogMap = new ArrayMap<>();
            appLogMap.put("time", LogParsingUtils.dateToSeconds(logLine.getTime()));
            appLogMap.put("time_timestamp", logLine.getTime());
            appLogMap.put("level", LogParsingUtils.parseSeverity(logLine.getSeverity()));
            appLogMap.put("message", logLine.getLogMessage());
            return appLogMap;
        }).collect(Collectors.toList());
        row.set("app_logs", appLogs);
    }
}

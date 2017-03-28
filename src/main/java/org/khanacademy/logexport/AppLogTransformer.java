package org.khanacademy.logexport;

import com.google.api.services.logging.v2.model.LogEntry;
import com.google.api.services.logging.v2.model.LogLine;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * Helper class for converting app_logs to request_logs so that they can be
 * ingested into bigquery. Note: this depends very heavily on the exact layout
 * of LogEntries. There are various hacks, dragons, and other spooky things.
 *
 * Realistically, all this means is adding a `threadId` field to protopayload,
 * moving the module_id from `metadata.labels` into protopayload, moving the
 * toplevel textpayload field into an entry in `protopayload.line` with severity
 * and time from `metadata`, and finally taking the time from `metadata` and
 * placing it in protopayload.
 */
public class AppLogTransformer implements Function<LogEntry, LogEntry> {

    private static String getPayloadText(LogEntry log) {
        String textPayload = "";
        if (log.getTextPayload() != null) {
            textPayload = log.getTextPayload();
        } else {
            Object payload = log.get("payload");
            try {
                textPayload = (String) ((Map<String, Object>) payload).get("logMessage");
            } catch (java.lang.ClassCastException e) {
                textPayload = (String) payload;
            }
        }
        return textPayload;
    }

    // Fill out the applogs (`protopayload.line`) as reasonably as we can
    private static ArrayList<LogLine> generateAppLogs(LogEntry log) {
        /* `protopayload.line` is a field that has an array of entries that look
         * like:
         * {
         *     "time": string,
         *     "severity": enum(LogSeverity),
         *     "logMessage": string,
         *     "sourceLocation": {
         *         object(SourceLocation)
         *     },
         * }
         * We choose to fill out time, severity, and logMessage using the fields
         * we have available.
         */
        ArrayList<LogLine> newAppLogs = new ArrayList<LogLine>();
        //Map<String, Object> line = new HashMap<String, Object>();
        LogLine line = new LogLine();

        String severity = "";
        String timestamp = "";
        if (LogAPIVersion.apiVersion(log) == LogAPIVersion.V1) {
            Map<String, Object> metadata = (Map<String, Object>) log.get("metadata");
            Map<String, Object> labels = (Map<String, Object>) metadata.get("labels");
            severity = (String) metadata.get("severity");
            timestamp = (String) metadata.get("timestamp");
        } else {
            severity = log.getSeverity();
            timestamp = log.getTimestamp();
        }

        // Set the various fields of the line that we need
        line.put("logMessage", getPayloadText(log));
        line.put("severity", severity);
        line.put("time", timestamp);

        // Add it to the array
        newAppLogs.add(line);

        return newAppLogs;
    }

    // Fill out all the fields in protopayload as reasonably as we can.
    private static Map<String, Object> generateProtoPayload(LogEntry log) {
        Map<String, Object> newProtoPayload = new HashMap<String, Object>();

        String thread_id = "";
        String timestamp = "";
        String module_id = "";
        String version_id = "";
        if (LogAPIVersion.apiVersion(log) == LogAPIVersion.V1) {
            Map<String, Object> metadata = (Map<String, Object>) log.get("metadata");
            Map<String, Object> labels = (Map<String, Object>) metadata.get("labels");
            thread_id = (String) labels.get("appengine.googleapis.com/thread_id");
            timestamp = (String) metadata.get("timestamp");
            module_id = (String) labels.get("appengine.googleapis.com/module_id");
        } else {
            thread_id = (String) log.getLabels().get("appengine.googleapis.com/thread_id");
            module_id = (String) log.getLabels().get("appengine.googleapis.com/module_id");
            version_id = (String) log.getLabels().get("appengine.googleapis.com/version_id");
            Map<String, String> resourceLabels = (Map<String, String>) ((Map<String, Object>) log.get("resource")).get("labels");
            if (module_id == null) {
                module_id = resourceLabels.get("module_id");
            }
            if (version_id == null) {
                version_id = resourceLabels.get("version_id");
            }

            timestamp = log.getTimestamp();
        }

        // Allow grouping on thread_id to gather app_logs.
        newProtoPayload.put("threadId", thread_id);

        // Almost reasonable times to fill our protopayload
        newProtoPayload.put("startTime", timestamp);
        newProtoPayload.put("endTime", timestamp);

        // Fill out the module_id so we know where this came from.
        newProtoPayload.put("moduleId", module_id);

        newProtoPayload.put("versionId", version_id);

        String payloadText = getPayloadText(log);
        // Do we have a request_id in here?
        if (payloadText != null && payloadText.startsWith("REQUEST_ID: ")) {
            // If we do, it's from the anchor message emitted in
            // `webapp/middleware.LogRequestIdMiddleware` if you change the
            // format there, make sure to change it here!
            int beginIndex = "REQUEST_ID: ".length();
            newProtoPayload.put("requestId", payloadText.substring(beginIndex));
        }

        // Finally, generate the app_logs and put them in place.
        newProtoPayload.put("line", generateAppLogs(log));
        return newProtoPayload;
    }

    /**
     * Main entry point for transforming app_logs to request_logs.
     */
    @Override
    public LogEntry apply(LogEntry log) {
        LogEntry workingLog = new LogEntry();
        workingLog.setTextPayload(null);
        Map<String, Object> protoPayload = generateProtoPayload(log);
        workingLog.setProtoPayload(protoPayload);
        workingLog.set("payload", protoPayload);
        return workingLog;
    }
}

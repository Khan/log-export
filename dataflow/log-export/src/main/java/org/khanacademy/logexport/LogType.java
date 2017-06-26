package org.khanacademy.logexport;

import com.google.api.services.logging.v2.model.LogEntry;
import com.google.common.collect.ImmutableMap;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

/**
 * Enum for determining the type of a log based on its log field.
 */
public enum LogType {
    ACCESS_LOG, ACTIVITY_LOG, APP_LOG, CRASH_LOG,
    REQUEST_LOG, SHUTDOWN_LOG, STDERR_LOG, STDOUT_LOG,
    SYSLOG_LOG, UNKNOWN_LOG;

    // A current canonical list of what types of logs we have now.
    private static Map<String, LogType> logtypes = ImmutableMap.<String, LogType>builder()
            .put("access", ACCESS_LOG)
            .put("activity", ACTIVITY_LOG)
            .put("app", APP_LOG)
            .put("crash.log", CRASH_LOG)
            .put("request_log", REQUEST_LOG)
            .put("shutdown.log", SHUTDOWN_LOG)
            .put("stderr", STDERR_LOG)
            .put("stdout", STDOUT_LOG)
            .put("syslog", SYSLOG_LOG)
            .build();

    @Override
    public String toString() {
        switch (this) {
            case ACCESS_LOG: return "access";
            case ACTIVITY_LOG: return "activity";
            case APP_LOG: return "app";
            case CRASH_LOG: return "crash.log";
            case REQUEST_LOG: return "request_log";
            case SHUTDOWN_LOG: return "shutdown.log";
            case STDERR_LOG: return "stderr";
            case STDOUT_LOG: return "stdout";
            case SYSLOG_LOG: return "syslog";
            case UNKNOWN_LOG: return "unknown log";
            default: return "unknown log";
        }
    }

    public static LogType getLogType(LogEntry log) {
        String logName = "";
        if (LogAPIVersion.apiVersion(log) == LogAPIVersion.V1) {
            logName = (String) log.get("log");
        } else {
            logName = log.getLogName();
        }
        // Log names look like "appengine.googleapis.com/request_log", get the
        // last field.  Names may be urlencoded, so we try to decode first.

        String decoded = logName;
        try {
            decoded = URLDecoder.decode(logName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.err.println("Caught unexpected error while URL decoding log name; ignoring.");
        }
        String[] parts = decoded.split("/");
        String key = parts[parts.length - 1];

        // We only have these type of logs defined, but safety first.
        if (logtypes.containsKey(key)) {
            return logtypes.get(key);
        } else {
            return LogType.UNKNOWN_LOG;
        }
    }
}

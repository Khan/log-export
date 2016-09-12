package org.khanacademy.logexport;

import com.google.api.services.logging.v2beta1.model.LogEntry;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
/**
 * Helper class for determining the type of a log based on its log field.
 */
public class ClassifyLog {
    // A current canonical list of what types of logs we have now.
    private static Map<String, LogType> logtypes = ImmutableMap.<String, LogType>builder()
        .put("access", LogType.ACCESS_LOG)
        .put("activity", LogType.ACTIVITY_LOG)
        .put("app", LogType.APP_LOG)
        .put("crash.log", LogType.CRASH_LOG)
        .put("request_log", LogType.REQUEST_LOG)
        .put("shutdown.log", LogType.SHUTDOWN_LOG)
        .put("stderr", LogType.STDERR_LOG)
        .put("stdout", LogType.STDOUT_LOG)
        .put("syslog", LogType.SYSLOG_LOG)
        .build();

    public static LogType getLogType(LogEntry m) {
        // TODO(samantha): com.google.api.services.logging.v2beta1.model.LogEntry.getLogName()
        // exists, but seems to return null, even though required? For now, just
        // get the log field from the map. Maybe due to using v1 logs with the
        // v2 library? This is spooky and could break in the future.
        String logName = (String) m.get("log");
        //This shouldn't ever be null, but just to be sure...
        if (logName == null) {
            return LogType.UNKNOWN_LOG;
        }

        // Log names look like "appengine.googleapis.com/request_log", get the
        // last field.
        String[] parts = logName.split("/");
        String key = parts[parts.length - 1];

        // We only have these type of logs defined, but safety first.
        if (logtypes.containsKey(key)) {
            return logtypes.get(key);
        }
        else {
            return LogType.UNKNOWN_LOG;
        }
    }
}

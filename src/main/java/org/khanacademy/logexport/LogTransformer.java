package org.khanacademy.logexport;

import com.google.api.services.logging.v2beta1.model.LogEntry;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;
import java.lang.IllegalArgumentException;

/**
 * Helper class for converting log of type A to request_log.
 * For instance, converting from app to request so that the data might be
 * ingested into BigQuery.
 */
public class LogTransformer {
    private static Map<LogType, Function> logTransformers =
            ImmutableMap.<LogType, Function>builder()
                    .put(LogType.REQUEST_LOG, Function.<LogEntry>identity())
                    .put(LogType.APP_LOG, new AppLogTransformer())
                    .build();
    /**
     * Main entry point for transforming logs.
     */
    public static LogEntry transform(LogEntry log) throws IllegalArgumentException{
        LogType type = LogType.getLogType(log);
        Function<LogEntry, LogEntry> transformer = logTransformers.get(type);
        if (transformer != null) {
            return transformer.apply(log);
        } else {
            throw new IllegalArgumentException(
                    String.format("Don't know how to transform log of type: %s!", type));
        }
    }
}

package org.khanacademy.logexport;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class LogParsingUtils {
    private static final int MICROSECONDS_TO_SECONDS_SHIFT = 6;

    /**
     * The proto payload gives dates in ISO 8601 format with microsecond precision. Change that to
     * UNIX time in seconds as a BigDecimal.
     */
    public static @Nullable BigDecimal dateToSeconds(Object object) {
        if (object instanceof String) {
            String string = (String) object;
            Instant instant = Instant.parse(string);
            long seconds = instant.getEpochSecond();
            int microseconds = instant.getNano() / 1000;
            return BigDecimal.valueOf(seconds)
                    .add(BigDecimal.valueOf(microseconds, MICROSECONDS_TO_SECONDS_SHIFT));
        }
        return null;
    }

    /**
     * The proto payload has some strings like "0.282637s". Change them to a float of the number of
     * seconds.
     */
    public static @Nullable BigDecimal parseDuration(Object object) {
        if (object instanceof String) {
            String string = (String) object;
            if (string.endsWith("s")) {
                try {
                    return new BigDecimal(string.substring(0, string.length() - 2));
                } catch (NumberFormatException e) {
                    // Fall through
                }

            }
        }
        return null;
    }

    // The ordering of this list corresponds to the log level numbers in the app log format.
    private static final List<String> SEVERITIES =
            ImmutableList.of("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL");

    /**
     * Convert a severity string like "WARNING" into the log level number, like 2.
     */
    public static @Nullable Integer parseSeverity(String severityName) {
        int index = SEVERITIES.indexOf(severityName);
        if (index == -1) {
            return null;
        } else {
            return index;
        }
    }


    /**
     * The proto payload has a `moduleId` field that isn't set for the default service, set it to
     * "default" so as to match `logs_hourly`.
     */
    public static @Nullable String parseModuleId(Object module_id) {
        return (module_id != null) ? (String) module_id : "default";
    }
}

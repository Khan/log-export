package org.khanacademy.logexport

import com.google.common.collect.ImmutableList
import java.math.BigDecimal
import java.time.Instant

object LogParsingUtils {
    private val MICROSECONDS_TO_SECONDS_SHIFT = 6

    /**
     * The proto payload gives dates in ISO 8601 format with microsecond precision. Change that to
     * UNIX time in seconds as a BigDecimal.
     */
    fun dateToSeconds(`object`: Any): BigDecimal? {
        if (`object` is String) {
            val instant = Instant.parse(`object`)
            val seconds = instant.epochSecond
            val microseconds = instant.nano / 1000
            return BigDecimal.valueOf(seconds).add(BigDecimal.valueOf(microseconds.toLong(), MICROSECONDS_TO_SECONDS_SHIFT))
        }
        return null
    }

    /**
     * The proto payload has some strings like "0.282637s". Change them to a float of the number of
     * seconds.
     */
    fun parseDuration(`object`: Any): BigDecimal? {
        if (`object` is String) {
            if (`object`.endsWith("s")) {
                try {
                    return BigDecimal(`object`.substring(0, `object`.length - 2))
                } catch (e: NumberFormatException) {
                    // Fall through
                }

            }
        }
        return null
    }

    // The ordering of this list corresponds to the log level numbers in the app log format.
    private val SEVERITIES = ImmutableList.of("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    /**
     * Convert a severity string like "WARNING" into the log level number, like 2.
     */
    fun parseSeverity(severityName: String): Int? {
        val index = SEVERITIES.indexOf(severityName)
        if (index == -1) {
            return null
        } else {
            return index
        }
    }
}

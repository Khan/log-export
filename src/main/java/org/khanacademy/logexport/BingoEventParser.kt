package org.khanacademy.logexport

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogLine
import org.khanacademy.logexport.Schemas.Type

import java.io.IOException
import java.util.ArrayList

/**
 * Parse bingo events, as logged by bigbingo/log.py in webapp.
 */
class BingoEventParser {

    val schemaFields: List<TableFieldSchema>
        get() = listOf(
                Schemas.repeatedRecord("bingo_participation_events",
                        Schemas.field("bingo_id", Type.STRING),
                        Schemas.field("experiment", Type.STRING),
                        Schemas.field("alternative", Type.STRING)),
                Schemas.repeatedRecord("bingo_conversion_events",
                        Schemas.field("bingo_id", Type.STRING),
                        Schemas.field("conversion", Type.STRING),
                        Schemas.field("extra", Type.STRING)))

    fun populateBingoEventFields(row: TableRow, logLines: List<LogLine>) {
        val participationEvents = ArrayList<Any?>()
        val conversionEvents = ArrayList<Any?>()

        for (logLine in logLines) {
            val logMessage = logLine.logMessage ?: continue
            if (logMessage.startsWith(PARTICIPATION_EVENT_PREFIX)) {
                val eventJson = logMessage.substring(PARTICIPATION_EVENT_PREFIX.length)
                participationEvents.add(parseEventJson(eventJson))
            } else if (logMessage.startsWith(CONVERSION_EVENT_PREFIX)) {
                val eventJson = logMessage.substring(CONVERSION_EVENT_PREFIX.length)
                conversionEvents.add(parseEventJson(eventJson))
            }
        }

        row.set("bingo_participation_events", participationEvents)
        row.set("bingo_conversion_events", conversionEvents)
    }

    /**
     * Turn the JSON string from a bingo event log line into a JSON value ready for BigQuery import.
     * Conveniently, the two JSON formats are the same, so we can just do regular JSON parsing and
     * get exactly the value we want.
     */
    private fun parseEventJson(eventJson: String): Any? {
        try {
            return JSON_FACTORY.fromString<Any>(eventJson, Any::class.java)
        } catch (e: IOException) {
            return null
        }

    }

    companion object {
        private val JSON_FACTORY = JacksonFactory()
        private val PARTICIPATION_EVENT_PREFIX = "BINGO_PARTICIPATION_EVENT:"
        private val CONVERSION_EVENT_PREFIX = "BINGO_CONVERSION_EVENT:"
    }
}

package org.khanacademy.logexport

import com.google.api.client.util.ArrayMap
import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogLine
import com.google.common.collect.FluentIterable
import com.google.common.collect.ImmutableList
import org.khanacademy.logexport.Schemas.Type
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.UnsupportedEncodingException
import java.math.BigDecimal
import java.net.URLDecoder
import java.util.ArrayList
import java.util.Optional

/**
 * Parse event log values, both categorized events and unique-occurence keys, as defined by
 * event_log.py in webapp.
 */
class EventLogParser {

    val schemaFields: List<TableFieldSchema>
        get() {
            val resultBuilder = ImmutableList.builder<TableFieldSchema>()
            for (field in UNIQUE_OCCURRENCE_KEYS) {
                resultBuilder.add(Schemas.field(field.columnName, field.type))
            }
            for (category in CATEGORIES) {
                resultBuilder.add(Schemas.repeatedRecord(category.columnName,
                        Schemas.field("key", Type.STRING),
                        Schemas.field("value", category.type)))
            }
            return resultBuilder.build()
        }

    fun populateEventLogFields(row: TableRow, logLines: List<LogLine>) {
        for (line in logLines) {
            val logMessage = line.logMessage
            if (logMessage == null || !logMessage.startsWith(EVENT_LOG_PREFIX)) {
                continue
            }
            for (eventString in logMessage.split(";".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()) {
                if (eventString.isEmpty() || eventString == EVENT_LOG_PREFIX) {
                    continue
                }
                val components = eventString.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
                val key = components[0]
                // Some events don't have a value, so we consider them to be the "true" boolean.
                val value = if (components.size >= 2) components[1] else java.lang.Boolean.toString(true)
                processKeyValuePair(row, key, value)
            }
        }
    }

    private fun processKeyValuePair(row: TableRow, key: String, value: String) {
        // TODO(alan): Doing a startsWith for every category for every event is kind of expensive.
        // Maybe profile this code and see if it's worth doing something more clever.
        val foundCategory = CATEGORIES.stream().filter({ category -> category.name.startsWith(key) }).findFirst()

        if (foundCategory.isPresent()) {
            val eventLogField = foundCategory.get()
            @SuppressWarnings("unchecked")
            var jsonEvents: MutableList<Map<String, Any>>? = row[eventLogField.columnName] as List<Map<String, Any>>
            if (jsonEvents == null) {
                jsonEvents = ArrayList<Map<String, Any>>()
                row.set(eventLogField.columnName, jsonEvents)
            }
            val newEvent = ArrayMap<String, Any>()
            newEvent.put("key", key)
            newEvent.put("value", eventLogField.parseValue(value))
            jsonEvents.add(newEvent)
        } else {
            val eventLogField = UNIQUE_OCCURRENCE_KEYS_BY_NAME[key]
            if (eventLogField != null) {
                val oldValue = row.put(
                        eventLogField.columnName, eventLogField.parseValue(value))
                if (oldValue != null) {
                    LOG.error("Unexpected duplicate result for key " + eventLogField.name)
                }
            }
        }
    }

    private class EventLogField(val name: String, val type: Type, val columnName: String) {

        fun parseValue(value: String): Any? {
            try {
                val decodedValue = URLDecoder.decode(value, "UTF-8")
                when (type) {
                    Schemas.Type.STRING -> return decodedValue
                    Schemas.Type.INTEGER -> return java.lang.Long.parseLong(decodedValue)
                    Schemas.Type.FLOAT -> return BigDecimal(decodedValue)
                    Schemas.Type.BOOLEAN -> return java.lang.Boolean.parseBoolean(decodedValue)
                }// Fall through to failure case.
                throw UnsupportedOperationException("Unsupported event log type: " + type)
            } catch (e: UnsupportedEncodingException) {
                return null
            } catch (e: NumberFormatException) {
                return null
            }

        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(EventLogParser::class.java!!)
        private val EVENT_LOG_PREFIX = "KALOG"

        // TODO(alan): Make it easier to keep these values in sync with webapp. For example, we could
        // have webapp output a JSON dump of this data and populate these values from the JSON.
        private val UNIQUE_OCCURRENCE_KEYS = ImmutableList.of(
                eventLogField("KA_APP", Type.BOOLEAN),
                eventLogField("app_version", Type.STRING),
                eventLogField("browser", Type.STRING),
                eventLogField("client_ip", Type.STRING),
                eventLogField("country", Type.STRING),
                eventLogField("device_brand", Type.STRING),
                eventLogField("device_name", Type.STRING),
                eventLogField("device_type", Type.STRING),
                eventLogField("ka_locale", Type.STRING),
                eventLogField("language", Type.STRING),
                eventLogField("orig_request_id", Type.STRING),
                eventLogField("os", Type.STRING),
                eventLogField("pageload", Type.BOOLEAN),
                eventLogField("retries", Type.INTEGER),
                eventLogField("session_id", Type.STRING),
                eventLogField("session_start", Type.INTEGER),
                eventLogField("test_prep_client_logging", Type.STRING),
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
                eventLogField("user_phantom_creation_date", Type.INTEGER))

        private val UNIQUE_OCCURRENCE_KEYS_BY_NAME = FluentIterable.from(UNIQUE_OCCURRENCE_KEYS).uniqueIndex<String>({ field -> field.name })

        private val CATEGORIES = ImmutableList.of(
                eventLogField("auth.", Type.STRING),
                eventLogField("bingo.", Type.STRING),
                eventLogField("content_survey.", Type.STRING),
                eventLogField("id.", Type.STRING),
                eventLogField("learn_storm.final.", Type.STRING),
                eventLogField("stats.bingo.", Type.INTEGER),
                eventLogField("stats.english_visibility.", Type.FLOAT),
                eventLogField("stats.logging.", Type.INTEGER),
                eventLogField("stats.react_render_server.", Type.INTEGER),
                eventLogField("stats.rpc.", Type.INTEGER),
                eventLogField("stats.rpc_info.", Type.STRING),
                eventLogField("stats.rpc_ops.", Type.INTEGER),
                eventLogField("stats.search.", Type.INTEGER),
                eventLogField("stats.time.", Type.INTEGER),
                eventLogField("stats.untranslated_text_seen.", Type.STRING))

        private fun eventLogField(name: String, type: Type): EventLogField {
            val columnName = "elog_" + name.replace("\\.$".toRegex(), "").replace("\\.".toRegex(), "_")
            return EventLogField(name, type, columnName)
        }
    }
}

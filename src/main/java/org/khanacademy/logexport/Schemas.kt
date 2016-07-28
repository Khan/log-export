package org.khanacademy.logexport

import com.google.api.services.bigquery.model.TableFieldSchema

import java.util.Arrays

/**
 * Convenience utilities for declaring components of a BigQuery schema.

 * Note that, unlike in LogToBigQuery, we never use the REQUIRED mode, only NULLABLE.
 */
object Schemas {
    fun repeatedRecord(name: String, vararg fields: TableFieldSchema): TableFieldSchema {
        return TableFieldSchema()
                .setName(name)
                .setType(Type.RECORD.toString())
                .setMode("REPEATED")
                .setFields(Arrays.asList(*fields))
    }

    fun field(name: String, type: Type): TableFieldSchema {
        return TableFieldSchema()
                .setName(name)
                .setType(type.toString())
    }

    enum class Type {
        STRING,
        INTEGER,
        FLOAT,
        BOOLEAN,
        TIMESTAMP,
        RECORD
    }
}

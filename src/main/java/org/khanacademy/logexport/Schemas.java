package org.khanacademy.logexport;

import com.google.api.services.bigquery.model.TableFieldSchema;

import java.util.Arrays;

/**
 * Convenience utilities for declaring components of a BigQuery schema.
 *
 * Note that, unlike in LogToBigQuery, we never use the REQUIRED mode, only NULLABLE.
 */
public class Schemas {
    public static TableFieldSchema repeatedRecord(String name, TableFieldSchema... fields) {
        return new TableFieldSchema().setName(name).setType(Type.RECORD.toString())
                .setMode("REPEATED").setFields(Arrays.asList(fields));
    }

    public static TableFieldSchema field(String name, Type type) {
        return new TableFieldSchema().setName(name).setType(type.toString());
    }

    public enum Type {
        STRING,
        INTEGER,
        FLOAT,
        BOOLEAN,
        TIMESTAMP,
        RECORD
    }
}

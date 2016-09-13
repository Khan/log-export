package org.khanacademy.logexport;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;

import java.io.IOException;
import java.util.List;


/**
 * The entrypoint for a binary that just lists the schema fields.
 */
public class ListSchema {
    public static void main(String[] arg) {
        LogsExtractor extractor = LogsExtractor.create();
        List<TableFieldSchema> fields = extractor.getSchemaFields();
        try {
           JsonFactory JSON_FACTORY = new JacksonFactory();
           System.out.println(JSON_FACTORY.toPrettyString(fields));
        } catch (IOException i) {
           i.printStackTrace();
        }
   }
}

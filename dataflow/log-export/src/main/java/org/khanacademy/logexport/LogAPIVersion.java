package org.khanacademy.logexport;

import com.google.api.services.logging.v2.model.LogEntry;

/**
 * Enum for determining the API version of a log based on various fields.
 */
public enum LogAPIVersion {
    V1, V2;

    public static LogAPIVersion apiVersion(LogEntry log) {
        if(log.getLogName() == null && log.getLabels() == null) {
            return V1;
        } else {
            return V2;
        }
    }
}

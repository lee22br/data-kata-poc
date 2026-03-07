package com.poc.model;

import java.sql.Timestamp;

/**
 * Output record written to the sink DB.
 * Shared by both Stream A (CITY) and Stream B (COUNTRY).
 */
public class SalesRank {

    /** "CITY" or "COUNTRY" */
    public String rankType;

    /** City name (CITY) or salesman name (COUNTRY) */
    public String groupKey;

    /** Salesman ID -- populated for COUNTRY stream, null for CITY */
    public String entityId;

    public double totalSales;

    public Timestamp windowStart;
    public Timestamp windowEnd;

    public SalesRank() {}

    @Override
    public String toString() {
        return String.format("SalesRank{type=%s, key=%s, total=%.2f, window=[%s --> %s]}",
            rankType, groupKey, totalSales, windowStart, windowEnd);
    }
}

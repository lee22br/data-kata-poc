package com.poc.model;

/**
 * Unified sale event flowing through all Flink streams.
 */
public class SaleEvent {

    public String saleId;
    public String salesmanId;
    public String salesmanName;
    public String city;
    public String region;
    public String productId;
    public double amount;
    public long   eventTime;   // epoch ms
    public String source;      // "db"

    public SaleEvent() {}

    @Override
    public String toString() {
        return String.format("SaleEvent{id=%s, salesman=%s, city=%s, amount=%.2f, source=%s}",
            saleId, salesmanName, city, amount, source);
    }
}

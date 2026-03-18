package com.poc;

import com.poc.model.SaleEvent;
import com.poc.model.SalesRank;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Sales Rankings -- Flink Batch Job (DB --> DB)
 *
 * Source:
 *   PostgreSQL  source_sales table
 *
 * Streams:
 *   A. Total Sales per City    (keyBy city       --> reduce)
 *   B. Total Sales per Salesman (keyBy salesmanId --> reduce)
 *
 * Sink:
 *   PostgreSQL  sales_ranks table  (upsert on conflict)
 *
 * Configuration via environment variables (set in docker-compose.yml):
 *   SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS,
 *   SINK_DB_URL,   SINK_DB_USER,   SINK_DB_PASS
 *
 * Optional CLI arguments:
 *   --from  yyyy-MM-dd   inclusive lower bound on event_time (default: no lower bound)
 *   --to    yyyy-MM-dd   inclusive upper bound on event_time (default: no upper bound)
 *   e.g.  flink run ... flink-job.jar --from 2024-02-01 --to 2024-02-29
 */
public class BatchJob {

    private static final Logger log = LoggerFactory.getLogger(BatchJob.class);

    public static void main(String[] args) throws Exception {

        // Config from environment
        String sourceDbUrl  = env("SOURCE_DB_URL",  "jdbc:postgresql://postgres:5432/salesdb");
        String sourceDbUser = env("SOURCE_DB_USER", "poc");
        String sourceDbPass = env("SOURCE_DB_PASS", "poc123");
        String sinkDbUrl    = env("SINK_DB_URL",    "jdbc:postgresql://postgres:5432/salesdb");
        String sinkDbUser   = env("SINK_DB_USER",   "poc");
        String sinkDbPass   = env("SINK_DB_PASS",   "poc123");

        // Parse optional --from / --to CLI arguments (yyyy-MM-dd or epoch ms)
        Long fromEpochMs = null;
        Long toEpochMs   = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--from".equals(args[i])) fromEpochMs = parseDate(args[i + 1]);
            if ("--to".equals(args[i]))   toEpochMs   = parseDate(args[i + 1]);
        }
        log.info("[BatchJob] Date filter -- from: {}, to: {}",
            fromEpochMs == null ? "unbounded" : args[indexOf(args, "--from") + 1],
            toEpochMs   == null ? "unbounded" : args[indexOf(args, "--to")   + 1]);

        // SOURCE -- JdbcSource streams rows directly from DB into Flink (no pre-load into memory).
        StringBuilder sql = new StringBuilder(
            "SELECT sale_id, salesman_id, salesman_name, city, region, " +
            "       product_id, amount, event_time " +
            "FROM   source_sales");
        if (fromEpochMs != null && toEpochMs != null) {
            sql.append(" WHERE event_time >= ").append(fromEpochMs)
               .append(" AND event_time <= ").append(toEpochMs);
        } else if (fromEpochMs != null) {
            sql.append(" WHERE event_time >= ").append(fromEpochMs);
        } else if (toEpochMs != null) {
            sql.append(" WHERE event_time <= ").append(toEpochMs);
        }
        sql.append(" ORDER BY event_time ASC");

        ResultExtractor<SaleEvent> extractor = rs -> {
            SaleEvent e = new SaleEvent();
            e.saleId       = rs.getString("sale_id");
            e.salesmanId   = rs.getString("salesman_id");
            e.salesmanName = rs.getString("salesman_name");
            e.city         = rs.getString("city");
            e.region       = rs.getString("region");
            e.productId    = rs.getString("product_id");
            e.amount       = rs.getDouble("amount");
            e.eventTime    = rs.getLong("event_time");
            e.source       = "db";
            return e;
        };

        JdbcSource<SaleEvent> jdbcSource = JdbcSource.<SaleEvent>builder()
            .setDriverName("org.postgresql.Driver")
            .setDBUrl(sourceDbUrl)
            .setUsername(sourceDbUser)
            .setPassword(sourceDbPass)
            .setSql(sql.toString())
            .setResultExtractor(extractor)
            .setTypeInformation(TypeInformation.of(SaleEvent.class))
            .build();

        // BATCH mode: keyBy + reduce gives group-by semantics on bounded (finite) input,
        // emitting one final result per key rather than intermediate running totals.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<SaleEvent> allSales = env
            .fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "Source: JDBC bounded");

        // STREAM A -- Total Sales per City
        // keyBy city --> reduce --> map to SalesRank(CITY)
        // window_start = min event_time for that city, window_end = max event_time
        DataStream<SalesRank> topSalesPerCity = allSales
            .map(e -> new CityAcc(e.city, e.amount, e.eventTime, e.eventTime))
            .returns(TypeInformation.of(CityAcc.class)) // to enforce type info
            .keyBy(a -> a.city)
            .reduce((a, b) -> new CityAcc(
                a.city,
                a.total + b.total,
                Math.min(a.minTime, b.minTime),
                Math.max(a.maxTime, b.maxTime)))
            .map(a -> {
                SalesRank r = new SalesRank();
                r.rankType    = "CITY";
                r.groupKey    = a.city;
                r.entityId    = null;
                r.totalSales  = a.total;
                r.windowStart = new Timestamp(a.minTime);
                r.windowEnd   = new Timestamp(a.maxTime);
                return r;
            })
            .returns(TypeInformation.of(SalesRank.class)) // to enforce type info
            .name("Stream A: Total Sales per City");

        // STREAM B -- Total Sales per Salesman (country-wide)
        // keyBy salesmanId --> reduce --> map to SalesRank(COUNTRY)
        DataStream<SalesRank> topSalesmanCountry = allSales
            .map(e -> new SalesmanAcc(e.salesmanId, e.salesmanName, e.amount, e.eventTime, e.eventTime))
            .returns(TypeInformation.of(SalesmanAcc.class)) // to enforce type info
            .keyBy(a -> a.salesmanId)
            .reduce((a, b) -> new SalesmanAcc(
                a.salesmanId,
                a.salesmanName != null ? a.salesmanName : b.salesmanName,
                a.total + b.total,
                Math.min(a.minTime, b.minTime),
                Math.max(a.maxTime, b.maxTime)))
            .map(a -> {
                SalesRank r = new SalesRank();
                r.rankType    = "COUNTRY";
                r.groupKey    = a.salesmanName;
                r.entityId    = a.salesmanId;
                r.totalSales  = a.total;
                r.windowStart = new Timestamp(a.minTime);
                r.windowEnd   = new Timestamp(a.maxTime);
                return r;
            })
            .returns(TypeInformation.of(SalesRank.class)) // to enforce type info
            .name("Stream B: Total Sales per Salesman");

        // SINK -- PostgreSQL (both streams --> same table, different rank_type)
        // Using Sink V2 API (OpenLineage compatible)
        JdbcConnectionOptions jdbcConnOpts = new JdbcConnectionOptions
            .JdbcConnectionOptionsBuilder()
            .withUrl(sinkDbUrl)
            .withDriverName("org.postgresql.Driver")
            .withUsername(sinkDbUser)
            .withPassword(sinkDbPass)
            .build();

        JdbcExecutionOptions jdbcExecOpts = JdbcExecutionOptions.builder()
            .withBatchSize(50)
            .withBatchIntervalMs(0)  // flush immediately in batch mode
            .withMaxRetries(3)
            .build();

        String upsertSql =
            "INSERT INTO sales_ranks (rank_type, group_key, entity_id, total_sales, window_start, window_end) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (rank_type, group_key, window_end) " +
            "DO UPDATE SET total_sales = EXCLUDED.total_sales";

        topSalesPerCity.sinkTo(
            JdbcSink.<SalesRank>builder()
                .withQueryStatement(upsertSql, (stmt, r) -> {
                    stmt.setString(1, r.rankType);
                    stmt.setString(2, r.groupKey);
                    stmt.setString(3, r.entityId);   // null for CITY
                    stmt.setDouble(4, r.totalSales);
                    stmt.setTimestamp(5, r.windowStart);
                    stmt.setTimestamp(6, r.windowEnd);
                })
                .withExecutionOptions(jdbcExecOpts)
                .buildAtLeastOnce(jdbcConnOpts)
        ).name("Sink: City Totals --> PostgreSQL");

        topSalesmanCountry.sinkTo(
            JdbcSink.<SalesRank>builder()
                .withQueryStatement(upsertSql, (stmt, r) -> {
                    stmt.setString(1, r.rankType);
                    stmt.setString(2, r.groupKey);
                    stmt.setString(3, r.entityId);
                    stmt.setDouble(4, r.totalSales);
                    stmt.setTimestamp(5, r.windowStart);
                    stmt.setTimestamp(6, r.windowEnd);
                })
                .withExecutionOptions(jdbcExecOpts)
                .buildAtLeastOnce(jdbcConnOpts)
        ).name("Sink: Salesman Totals --> PostgreSQL");

        System.out.println("DEBUG: Iniciando execução do Job com OpenLineage...");
        env.execute("KATA_DATA_JOB");
        System.out.println("Aguardando envio de metadados (7s)...");
        Thread.sleep(7000);
        System.out.println("Done");
    }

    private static long parseDate(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
    }

    private static int indexOf(String[] arr, String val) {
        for (int i = 0; i < arr.length; i++) if (val.equals(arr[i])) return i;
        return -1;
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }

    // -- Accumulators (static inner POJOs for keyBy+reduce)

    public static class CityAcc {
        public String city;
        public double total;
        public long   minTime;
        public long   maxTime;

        public CityAcc() {}

        public CityAcc(String city, double total, long minTime, long maxTime) {
            this.city    = city;
            this.total   = total;
            this.minTime = minTime;
            this.maxTime = maxTime;
        }
    }

    public static class SalesmanAcc {
        public String salesmanId;
        public String salesmanName;
        public double total;
        public long   minTime;
        public long   maxTime;

        public SalesmanAcc() {}

        public SalesmanAcc(String id, String name, double total, long minTime, long maxTime) {
            this.salesmanId   = id;
            this.salesmanName = name;
            this.total        = total;
            this.minTime      = minTime;
            this.maxTime      = maxTime;
        }
    }
}

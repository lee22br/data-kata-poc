CREATE DATABASE marquez;
CREATE USER marquez WITH PASSWORD 'marquez';
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;

-- 2. Conectar no banco do Job (salesdb) para criar as tabelas de negócio
\c salesdb;
-- Source table
CREATE TABLE IF NOT EXISTS source_sales (
    sale_id       VARCHAR(50)    PRIMARY KEY,
    salesman_id   VARCHAR(50)    NOT NULL,
    salesman_name VARCHAR(100)   NOT NULL,
    city          VARCHAR(100)   NOT NULL,
    region        VARCHAR(100)   NOT NULL,
    product_id    VARCHAR(50)    NOT NULL,
    amount        DECIMAL(12,2)  NOT NULL,
    event_time    BIGINT         NOT NULL   -- epoch ms
);

-- Sink table
CREATE TABLE IF NOT EXISTS sales_ranks (
    id            SERIAL         PRIMARY KEY,
    rank_type     VARCHAR(10)    NOT NULL,  -- 'CITY' | 'COUNTRY'
    group_key     VARCHAR(200)   NOT NULL,  -- city name or salesman name
    entity_id     VARCHAR(50),              -- salesmanId (COUNTRY stream only)
    total_sales   DECIMAL(12,2)  NOT NULL,
    window_start  TIMESTAMP      NOT NULL,  -- earliest event_time in this group
    window_end    TIMESTAMP      NOT NULL,  -- latest  event_time in this group
    created_at    TIMESTAMP      NOT NULL DEFAULT NOW(),
    UNIQUE (rank_type, group_key, window_end)
);

-- Convenience views for querying results
CREATE OR REPLACE VIEW top_cities_latest AS
    SELECT
        group_key AS city,
        total_sales,
        RANK() OVER (PARTITION BY window_end ORDER BY total_sales DESC)   AS rank,
        window_end
    FROM sales_ranks
    WHERE rank_type = 'CITY'
    ORDER BY window_end DESC, total_sales DESC;

CREATE OR REPLACE VIEW top_salesmen_latest AS
    SELECT
        group_key AS salesman_name,
        entity_id AS salesman_id,
        total_sales,
        RANK() OVER (PARTITION BY window_end ORDER BY total_sales DESC)   AS rank,
        window_end
    FROM sales_ranks
    WHERE rank_type = 'COUNTRY'
    ORDER BY window_end DESC, total_sales DESC;

-- Seed data for the source DB  (Sales spread across Jan / Feb / Mar 2024)
-- event_time values are Unix epoch ms (UTC start of day + hourly offsets)
--
--   2024-01-10  =>  1704844800000
--   2024-01-20  =>  1705708800000
--   2024-02-05  =>  1707091200000
--   2024-02-20  =>  1708387200000
--   2024-03-01  =>  1709251200000
INSERT INTO source_sales VALUES

  -- January 2024 (DB001–DB010)
  ('DB001','SM001','Alice Johnson',  'New York',      'Northeast', 'P001', 3200.00, 1704844800000),
  ('DB002','SM002','Bob Williams',   'Los Angeles',   'West',      'P002', 1750.00, 1704848400000),
  ('DB003','SM003','Carol Davis',    'Chicago',       'Midwest',   'P001', 2100.00, 1704852000000),
  ('DB004','SM001','Alice Johnson',  'New York',      'Northeast', 'P003', 4500.00, 1704855600000),
  ('DB005','SM004','David Brown',    'Houston',       'South',     'P002', 1200.00, 1704859200000),
  ('DB006','SM005','Eva Martinez',   'Phoenix',       'Southwest', 'P004', 2800.00, 1705708800000),
  ('DB007','SM002','Bob Williams',   'Los Angeles',   'West',      'P005', 3100.00, 1705712400000),
  ('DB008','SM006','Frank Wilson',   'Philadelphia',  'Northeast', 'P001', 1900.00, 1705716000000),
  ('DB009','SM003','Carol Davis',    'Chicago',       'Midwest',   'P003', 2400.00, 1705719600000),
  ('DB010','SM007','Grace Taylor',   'San Antonio',   'South',     'P002', 3300.00, 1705723200000),

  -- February 2024 (DB011–DB020)
  ('DB011','SM004','David Brown',    'Houston',       'South',     'P004', 2600.00, 1707091200000),
  ('DB012','SM008','Henry Anderson', 'San Diego',     'West',      'P005', 1400.00, 1707094800000),
  ('DB013','SM005','Eva Martinez',   'Phoenix',       'Southwest', 'P001', 3700.00, 1707098400000),
  ('DB014','SM001','Alice Johnson',  'New York',      'Northeast', 'P002', 2900.00, 1707102000000),
  ('DB015','SM006','Frank Wilson',   'Philadelphia',  'Northeast', 'P003', 1600.00, 1707105600000),
  ('DB016','SM007','Grace Taylor',   'San Antonio',   'South',     'P004', 4100.00, 1708387200000),
  ('DB017','SM008','Henry Anderson', 'San Diego',     'West',      'P001', 2200.00, 1708390800000),
  ('DB018','SM002','Bob Williams',   'Los Angeles',   'West',      'P003', 3500.00, 1708394400000),
  ('DB019','SM003','Carol Davis',    'Chicago',       'Midwest',   'P005', 1800.00, 1708398000000),
  ('DB020','SM005','Eva Martinez',   'Phoenix',       'Southwest', 'P002', 2700.00, 1708401600000),

  -- March 2024 (DB021–DB030)
  ('DB021','SM001','Alice Johnson',  'New York',      'Northeast', 'P001', 5100.00, 1709251200000),
  ('DB022','SM002','Bob Williams',   'Los Angeles',   'West',      'P004', 2300.00, 1709254800000),
  ('DB023','SM003','Carol Davis',    'Chicago',       'Midwest',   'P002', 3800.00, 1709258400000),
  ('DB024','SM004','David Brown',    'Houston',       'South',     'P005', 1500.00, 1709262000000),
  ('DB025','SM005','Eva Martinez',   'Phoenix',       'Southwest', 'P003', 4200.00, 1709265600000),
  ('DB026','SM006','Frank Wilson',   'Philadelphia',  'Northeast', 'P001', 2100.00, 1709269200000),
  ('DB027','SM007','Grace Taylor',   'San Antonio',   'South',     'P002', 3600.00, 1709272800000),
  ('DB028','SM008','Henry Anderson', 'San Diego',     'West',      'P003', 1700.00, 1709276400000),
  ('DB029','SM001','Alice Johnson',  'New York',      'Northeast', 'P004', 4800.00, 1709280000000),
  ('DB030','SM002','Bob Williams',   'Los Angeles',   'West',      'P005', 2900.00, 1709283600000)

ON CONFLICT (sale_id) DO NOTHING;

# db-flink-batch-poc

Batch version of [db-flink-poc](../db-flink-poc). Same PostgreSQL → Flink → PostgreSQL
pipeline, but runs **once** against a bounded dataset instead of continuously polling for new records.

## Services & ports

| Service | Port |
|---|---|
| Flink UI | [localhost:8084](http://localhost:8084) |
| PostgreSQL | localhost:5434 |

## Start

### Option A — docker compose (all-in-one)

Starts postgres + Flink cluster + submits the job in one command.

```bash
cd db-flink-batch-poc
docker compose up --build          # default: Feb 2024
```

Override dates at runtime:

```bash
JOB_FROM_DATE=2024-01-01 JOB_TO_DATE=2024-01-31 docker compose up
```

`flink-job-submit` blocks until the batch job finishes, then exits with code 0.

### Option B — build.sh + submit.sh (cluster already running)

Use these scripts when the cluster is already up (`docker compose up` without the job)
and you want to submit jobs manually with different date ranges.

**1. Start the cluster (skip the job-submit service):**

```bash
docker compose up postgres flink-jobmanager flink-taskmanager -d
```

**2. Build the fat JAR:**

```bash
./build.sh
# Output: flink-job/target/flink-job-1.0.jar
```

**3. Truncate the sink table (Optional):**

```bash
docker exec db-flink-batch-poc-postgres-1 psql -U poc -d salesdb -c "TRUNCATE sales_ranks;"
```

**4. Submit with any date range:**

```bash
./submit.sh --from 2024-01-01 --to 2024-01-31
./submit.sh --from 2024-02-01 --to 2024-02-29
./submit.sh --from 2024-01-01 --to 2024-03-31
```

`submit.sh` connects to `flink-jobmanager:8081` via the compose network,
mounts the local JAR, and blocks until the job completes.

**To submit a new JAR version after a code change:**

```bash
# 1. Edit BatchJob.java (or any source file)
# 2. Rebuild — fast on subsequent runs due to ~/.m2 cache
./build.sh
# 3. Submit as usual — picks up the new JAR automatically
./submit.sh --from 2024-02-01 --to 2024-02-29
```

No cluster restart needed. The cluster stays up; only the JAR is replaced.

## Monitoring — Flink UI

Open **[http://localhost:8084](http://localhost:8084)** while the cluster is running.

| Section | Path | What to look for |
|---|---|---|
| Running jobs | **Jobs → Running Jobs** | Job appears here while `submit.sh` / `flink-job-submit` is blocking |
| Completed jobs | **Jobs → Completed Jobs** | Job moves here on success (green) or failure (red) |
| Job graph | Click the job → **Overview** tab | Shows the two parallel pipelines (Stream A: City, Stream B: Salesman) |
| Task metrics | Click a task node → **Metrics** tab | Records in/out, throughput |
| Logs | Click a task node → **TaskManagers** tab → select TM → **Logs** | Full Flink execution logs including `[BatchJob]` lines |
| Exceptions | Click the job → **Exceptions** tab | Full stack trace if the job failed |

> **Tip:** Because this is a batch job it runs fast (a few seconds). If you miss it in
> Running Jobs, check Completed Jobs immediately after `submit.sh` returns.

## Query results

```bash
# City rankings (ranked by total sales)
docker exec db-flink-batch-poc-postgres-1 psql -U poc -d salesdb \
  -c "SELECT rank, city, total_sales, window_end FROM top_cities_latest;"

# Salesman rankings
docker exec db-flink-batch-poc-postgres-1 psql -U poc -d salesdb \
  -c "SELECT rank, salesman_name, salesman_id, total_sales FROM top_salesmen_latest;"

# Raw output -- all data
docker exec db-flink-batch-poc-postgres-1 psql -U poc -d salesdb \
  -c "SELECT rank_type, group_key, total_sales, window_start, window_end FROM sales_ranks ORDER BY rank_type, total_sales DESC;"
```

## Stop

```bash
docker compose down -v
```

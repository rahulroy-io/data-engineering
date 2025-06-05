# Comprehensive Enumerations for Event Log Schema (v2: Custom Columns)

## Table Columns

```
record_id           -- Unique UUID per record (string, PK)
step                -- Auto-incremented integer for event order in job (int)
task                -- Task identifier (string, e.g. sub-step or component)
job_name            -- Name of the Glue job or workflow (string)
job_run_id          -- Unique ID per execution (string/uuid)
job_type            -- High-level job type (parsed from job_name; e.g., 'nbjob', 'gluejob')
event_type          -- High-level event category (string)
event_name          -- Specific event or step name (string)
event_payload       -- JSON string with event details (string/JSON)
event_state         -- Outcome of the event ('success', 'fail', 'running', etc.)
metric_name         -- Key for numeric/boolean metric (string, nullable)
metric_value        -- Value of metric (double/float, nullable)
event_ts_epoch      -- Event timestamp (epoch seconds, float)
event_ts_isoformat  -- Event timestamp (ISO-8601 string)
event_date          -- Partition/date column (yyyy-mm-dd)
env                 -- Environment (dev/uat/prod)
```

---

## 1. `event_type` – Canonical Categories

| Value          | Description                                         |
| -------------- | --------------------------------------------------- |
| JOB\_STATUS    | Job status events (start, end, etc.)                |
| STEP\_STATUS   | Status of a specific processing step                |
| VALIDATION     | Data quality/validation check                       |
| METADATA       | Emitting table schema, partition, or system info    |
| ERROR          | Exception, failure, or warning                      |
| INFO           | Informational, not an error                         |
| METRIC         | Numeric/boolean metric (row count, file size, etc.) |
| COMPACTION     | Hudi compaction lifecycle events                    |
| NOTIFICATION   | Notification events (SNS, email)                    |
| RETRY          | Retry attempt of an action                          |
| EXTERNAL\_CALL | External system/API/Secrets Manager call            |

---

## 2. `event_name` – Specific Actions or Steps

| Value                          | Description                       |
| ------------------------------ | --------------------------------- |
| JOB\_STARTED                   | Job execution has started         |
| JOB\_COMPLETED                 | Job finished (success or failure) |
| JOB\_FAILED                    | Job execution failed              |
| STEP\_STARTED                  | Step has started                  |
| STEP\_COMPLETED                | Step completed                    |
| STEP\_FAILED                   | Step failed                       |
| VALIDATION\_SCHEMA             | Schema conformity check           |
| VALIDATION\_NOT\_NULL          | Null check on critical columns    |
| VALIDATION\_UNIQUE\_KEY        | Uniqueness check                  |
| WRITE\_S3\_STARTED/COMPLETED   | Writing to S3 start/completion    |
| WRITE\_HUDI\_STARTED/COMPLETED | Writing to Hudi start/completion  |
| READ\_JDBC\_STARTED/COMPLETED  | Read JDBC start/completion        |
| METRIC\_ROW\_COUNT             | Row count metric emitted          |
| METRIC\_FILE\_SIZE\_MB         | File size metric emitted          |
| COMPACTION\_STARTED            | Hudi compaction started           |
| COMPACTION\_COMPLETED          | Hudi compaction finished          |
| ERROR\_EXCEPTION               | Error/exception event             |
| NOTIFICATION\_SENT             | Notification sent (SNS, email)    |
| EXTERNAL\_CALL\_START/END      | External API call event           |

---

## 3. `event_payload` – Example JSONs

* **Minimal string (for debugging/test):**

  ```json
  "abcd"
  ```
* **Source Read**

  ```json
  {"jdbc_url":"...","table":"CustomerOrders","filter":"order_date >= '2024-01-01'","source_row_count":15423,"elapsed_seconds":12.4}
  ```
* **Validation**

  ```json
  {"rule_name":"NOT_NULL","columns_checked":["customer_id"],"failed_column":"order_id","failed_count":5}
  ```
* **Write/Target**

  ```json
  {"target_path":"s3://bucket/...","file_format":"parquet","partition_cols":["dt"],"target_row_count":15400}
  ```
* **Error**

  ```json
  {"exception_type":"AnalysisException","error_message":"Table not found","stack_trace":"..."}
  ```
* **Notification**

  ```json
  {"notification_type":"SNS","topic_arn":"...","message_id":"...","status":"SENT"}
  ```
* **Custom Info**

  ```json
  {"notes":"Starting incremental load","spark_ui_job_id":"job_20240605_001","executor_memory_mb":4096}
  ```

---

## 4. `event_state` – Canonical States

| Value   | Description                            |
| ------- | -------------------------------------- |
| success | Event/task/step completed successfully |
| fail    | Event/task/step failed                 |
| running | Task/step/event in progress            |
| warning | Completed but with warnings            |
| skipped | Task/step was skipped                  |

---

## 5. `metric_name` – Canonical Metrics

| Value                | Description                           |
| -------------------- | ------------------------------------- |
| duration\_seconds    | Duration of the event/step in seconds |
| source\_row\_cnt     | Rows read from source                 |
| target\_row\_cnt     | Rows written to target                |
| dq\_rule\_passed     | DQ rule passed (1/0)                  |
| dq\_rule\_failed     | DQ rule failed (1/0)                  |
| file\_size\_mb       | Size of written file in MB            |
| elapsed\_seconds     | Duration of step/operation in seconds |
| num\_partitions      | Number of partitions written/read     |
| avg\_record\_size    | Average record size in bytes          |
| num\_executor\_cores | CPU cores used by Spark executors     |
| executor\_memory\_mb | Memory used by Spark executors (MB)   |
| retry\_count         | Retry attempts for a given action     |
| validation\_passed   | Validation result (1/0)               |
| num\_errors          | Number of errors in a step            |
| api\_latency\_ms     | Latency of external API call (ms)     |
| cost\_estimate\_usd  | Estimated operation cost              |

---

## 6. `metric_value` – Example Values

* For row counts: `15423.0`
* For boolean results: `1.0` (true), `0.0` (false)
* For durations: `12.4`, `0.0`
* For sizes: `32.8`
* For retries/errors: `2.0`, `0.0`, etc.
* For cost: `0.031`

---

## 7. Example Usage (Python)

```python
import uuid
from datetime import datetime

class CustomRange:
    def __init__(self, start, prefix=''):
        self.current = start
        self.prefix = prefix
    def __iter__(self):
        return self
    def __next__(self):
        self.current += 1
        return f"{self.prefix}{self.current}"

# Usage:
auto_increment = CustomRange(start=-1)

sample_record = {
    "record_id": str(uuid.uuid4()),
    "step": next(auto_increment),
    "task": "task_id",
    "job_name": JOB_NAME,
    "job_run_id": JOB_RUN_ID,
    "job_type": JOB_NAME.split('_')[0],
    "event_type": "JOB_STATUS",
    "event_name": "JOB_STARTED",
    "event_payload": "abcd",
    "event_state": "success",
    "metric_name": "duration_seconds",
    "metric_value": '0.0',
    "event_ts_epoch": datetime.now().timestamp(),
    "event_ts_isoformat": datetime.now().isoformat(),
    "event_date": datetime.today().strftime('%Y-%m-%d'),
    "env": JOB_NAME.split('_')[1]
}

# ... in your ingestion loop, collect records as list of dicts ...

# Then write using your existing write_job_metadata_to_hudi()

write_job_metadata_to_hudi(
    spark,
    hudi_table_path='s3://bucket/tmp/metadata_tbl/',
    table_name='metadata_tbl',
    records=[sample_record],
    hudi_database='test_db',
    partition_keys=["job_run_id"],
    record_key="job_run_id"
)
```

---

## 8. Sample Hudi Insert Function (with comments)

```python
from pyspark.sql import functions as F

def write_job_metadata_to_hudi(
    spark,
    hudi_table_path: str,
    table_name: str,
    records: list,
    hudi_database: str = "default",
    partition_keys: list = ["job_run_id"],
    record_key: str = "record_id"
):
    """
    Insert job metadata records into Hudi table (MOR, insert-only).
    """
    # Convert records list of dicts to DataFrame
    df = spark.createDataFrame(records).withColumn("timestamp", F.current_timestamp())

    # Hudi options for insert-only, MOR
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.hive_style_partitioning": 'true',
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.partitionpath.field": ",".join(partition_keys),
        "hoodie.datasource.write.precombine.field": "timestamp",  # Not used but required
        "hoodie.upsert.shuffle.parallelism": 1,
        "hoodie.insert.shuffle.parallelism": 1,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": hudi_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": ",".join(partition_keys),
        "hoodie.datasource.hive_sync.use_jdbc": "false",  # For Glue/Athena
        "hoodie.cleaner.policy.failed.writes": "EAGER"
    }

    # Write DataFrame to Hudi table
    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(hudi_table_path)
    )
```

---

*This structure supports detailed job tracking, step-wise tracing, Hudi/Athena compatibility, and strong partitioning for fast analytics in Glue/Athena.*

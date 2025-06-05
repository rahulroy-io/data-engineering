# Comprehensive Enumerations for Event Log Schema

## Table Columns

```
job_name        -- Name of the Glue job or workflow (string)
job_run_id      -- Unique ID per execution (string/uuid)
event_type      -- High-level category for event (string)
event_name      -- Specific event or step (string)
event_payload   -- JSON string with event details (string/JSON)
metric_name     -- Key for numeric/boolean metric (string, nullable)
metric_value    -- Value of metric (double, nullable)
event_ts        -- Timestamp of event (timestamp)
event_date      -- Partition column (date, derived from event_ts)
env             -- Environment (dev/uat/prod)
```

---

## 1. `event_type` – Canonical Categories

| Value             | Description                                         |
| ----------------- | --------------------------------------------------- |
| JOB\_START        | Start of a job run                                  |
| JOB\_END          | End of a job run (success/failure)                  |
| STEP\_START       | Beginning of a processing step                      |
| STEP\_END         | End of a processing step                            |
| VALIDATION        | Data quality/validation check                       |
| METADATA          | Emitting table schema, partition, or system info    |
| ERROR             | Exception, failure, or warning                      |
| INFO              | Informational, not an error                         |
| METRIC            | Numeric/boolean metric (row count, file size, etc.) |
| COMPACTION\_START | Start of Hudi compaction                            |
| COMPACTION\_END   | End of Hudi compaction                              |
| NOTIFICATION      | Notification events (SNS, email)                    |
| RETRY             | Retry attempt of an action                          |
| EXTERNAL\_CALL    | External system/API/Secrets Manager call            |

---

## 2. `event_name` – Specific Actions or Steps

| Value                        | Description                            |
| ---------------------------- | -------------------------------------- |
| JOB\_START / JOB\_END        | Start/end of the job execution         |
| READ\_JDBC\_START / END      | Read from JDBC source (start/end)      |
| READ\_PARQUET\_START / END   | Read Parquet files from S3 (start/end) |
| TRANSFORM\_START / END       | Data transformation (start/end)        |
| JOIN\_DATA\_START / END      | Join operation (start/end)             |
| AGGREGATE\_START / END       | Aggregation operation (start/end)      |
| VALIDATION\_SCHEMA           | Schema conformity check                |
| VALIDATION\_NOT\_NULL        | Null check on critical columns         |
| VALIDATION\_UNIQUE\_KEY      | Uniqueness check                       |
| WRITE\_S3\_START / END       | Write results to S3                    |
| WRITE\_HUDI\_START / END     | Write to Hudi table                    |
| WRITE\_REDSHIFT\_START / END | Write to Redshift                      |
| COMPACTION\_START / END      | Hudi compaction operation              |
| CLEANUP\_START / END         | Maintenance/cleanup                    |
| EMIT\_TABLE\_SCHEMA          | Emitting target table schema           |
| UPDATE\_CATALOG\_START / END | Update Glue Data Catalog               |
| FETCH\_SECRET\_START / END   | Retrieve AWS Secrets                   |
| API\_CALL\_START / END       | Call to external API                   |
| RETRY\_READ\_JDBC            | Retried JDBC read                      |
| SNS\_NOTIFICATION\_SENT      | Notification sent (SNS)                |
| INFO\_BROADCAST\_COMPLETED   | Informational events                   |
| HEARTBEAT                    | Liveness check                         |

---

## 3. `event_payload` – Example JSONs

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
* **Compaction**

  ```json
  {"hudi_table_name":"job_metadata_hudi","commit_time":"20240605123000","num_files_before":12,"num_files_after":1,"elapsed_seconds":8.7}
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

## 4. `metric_name` – Canonical Metrics

| Value                | Description                           |
| -------------------- | ------------------------------------- |
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

## 5. `metric_value` – Example Values

* For row counts: `15423.0`
* For boolean results: `1.0` (true), `0.0` (false)
* For durations: `12.4`
* For sizes: `32.8`
* For retries/errors: `2.0`, `0.0`, etc.
* For cost: `0.031`

---

## 6. Example Event Log Rows

| job\_name              | job\_run\_id   | event\_type       | event\_name        | event\_payload                                    | metric\_name     | metric\_value | event\_ts            | event\_date | env  |
| ---------------------- | -------------- | ----------------- | ------------------ | ------------------------------------------------- | ---------------- | ------------- | -------------------- | ----------- | ---- |
| minda\_marketingdbo... | 20240605120000 | JOB\_START        | JOB\_START         | ""                                                | NULL             | NULL          | 2024-06-05T12:00:00Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | STEP\_START       | READ\_JDBC\_START  | {"jdbc\_url":"..."}                               | NULL             | NULL          | 2024-06-05T12:00:10Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | METRIC            | READ\_JDBC\_END    | {"source\_row\_count":15423}                      | source\_row\_cnt | 15423.0       | 2024-06-05T12:00:22Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | VALIDATION        | VALIDATION\_SCHEMA | {"error\_message":"Column 'order\_date' missing"} | NULL             | NULL          | 2024-06-05T12:01:00Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | STEP\_START       | WRITE\_S3\_START   | {"target\_path":"s3://..."}                       | NULL             | NULL          | 2024-06-05T12:05:00Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | METRIC            | WRITE\_S3\_END     | {"target\_row\_count":15400}                      | target\_row\_cnt | 15400.0       | 2024-06-05T12:05:15Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | COMPACTION\_START | COMPACTION\_START  | {"hudi\_table":"job\_metadata\_hudi"}             | NULL             | NULL          | 2024-06-05T02:00:00Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | COMPACTION\_END   | COMPACTION\_END    | {"num\_files\_before":5,"num\_files\_after":1}    | NULL             | NULL          | 2024-06-05T02:00:04Z | 2024-06-05  | prod |
| minda\_marketingdbo... | 20240605120000 | JOB\_END          | JOB\_END           | ""                                                | NULL             | NULL          | 2024-06-05T12:10:00Z | 2024-06-05  | prod |

---

*You can adapt/expand these lists as your system evolves, but this table covers 99% of common job event and metric tracking needs for Glue/Athena/Hudi metadata systems.*

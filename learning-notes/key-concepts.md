# Key Concepts

## Project Architecture Summary

This project connects several AWS services into a dataflow pipeline.

The high-level flow is:

```text
GitHub repository
      ↓
AWS CodePipeline
      ↓
Amazon S3 bucket for Airflow DAG code
      ↓
Amazon MWAA / Apache Airflow
      ↓
Amazon Kinesis Data Streams
      ↓
Amazon Data Firehose
      ↓
Amazon S3 data storage
      ↓
Amazon Athena
```

The project has two major ideas:

1. **Code deployment path**
   ```text
   GitHub → CodePipeline → S3
   ```

2. **Data movement/query path**
   ```text
   Airflow → Kinesis Data Stream → Firehose → S3 → Athena
   ```

The first path is about getting DAG code from GitHub into AWS.

The second path is about moving data through AWS streaming/storage services and querying it with Athena.

---

## Airflow / Amazon MWAA

### What Airflow Is

Apache Airflow is a workflow orchestration tool.

It is used to define, schedule, run, monitor, and troubleshoot multi-step workflows.

A workflow might include steps like:

```text
call an API
download data
upload data to S3
send records to Kinesis
run SQL
wait for another task
validate output
```

Airflow is not a database.

Airflow is not a storage system.

Airflow is not the service that permanently holds the business data.

Airflow is the system that controls the order of work.

A simple mental model:

```text
Airflow = conductor
DAG = workflow map
Task = one step in the workflow
Operator = the kind of work a task performs
```

### What Amazon MWAA Is

Amazon MWAA stands for Amazon Managed Workflows for Apache Airflow.

MWAA is AWS-managed Airflow.

Instead of installing and maintaining Airflow servers manually, AWS provides the Airflow environment.

MWAA manages infrastructure pieces such as:

```text
Airflow scheduler
Airflow workers
Airflow web UI
Airflow metadata database
Airflow logs
Airflow environment configuration
```

The project uses MWAA so that Airflow can run in AWS and interact with AWS services like S3 and Kinesis.

### Why Airflow Is Used in This Project

Airflow is used to run the project workflow.

In this project, Airflow is expected to run DAG code that can:

```text
call an API
retrieve records
send data into Kinesis
track task success/failure
show logs in the Airflow UI
```

Airflow is useful because it provides:

```text
workflow visibility
task dependencies
manual triggering
scheduled triggering
logs
failure status
retry behavior
repeatable execution
```

Without Airflow, the project would rely on manual scripts or one-off commands.

### What a DAG Is

DAG stands for Directed Acyclic Graph.

Plain English:

```text
A DAG is Airflow's workflow definition.
```

A DAG tells Airflow:

```text
which tasks exist
what order tasks run in
which tasks depend on other tasks
which tasks can run independently
what schedule the workflow uses
```

Example:

```text
start
  ↓
extract data
  ↓
send records to Kinesis
  ↓
finish
```

### What Operators Are

Operators are Airflow templates for task types.

Examples:

```text
PythonOperator = run Python code
BashOperator = run a shell command
SQL operator = run SQL
EmptyOperator = create a checkpoint or placeholder
```

If a DAG has a Python task that calls an API and sends records to Kinesis, that task will likely use a `PythonOperator`.

### What Tasks Are

A task is one execution step inside a DAG.

Examples:

```text
load_sample_json_to_s3
load_api_to_aws_kinesis
send_records_to_stream
```

A task can succeed, fail, retry, or be skipped.

### What Scheduling Means

Airflow DAGs can run manually or on a schedule.

Examples:

```text
run manually only
run every hour
run every day
run every Monday
```

For learning projects, manual runs are usually safer because they avoid repeated AWS usage and unexpected cost.

### What Airflow Does Not Do

Airflow does not magically process or store all data by itself.

It coordinates other systems.

In this project:

```text
Airflow runs the DAG
Kinesis receives streaming records
Firehose delivers records
S3 stores files
Athena queries files
```

Airflow connects the steps together.

---

## CodePipeline

### What CodePipeline Is

AWS CodePipeline is a continuous delivery / CI/CD service.

It automates movement of code from a source location to a destination or deployment target.

In this project, CodePipeline watches a GitHub repository and deploys the repository contents into an S3 bucket.

The flow is:

```text
GitHub main branch
      ↓
CodePipeline source stage
      ↓
CodePipeline deploy stage
      ↓
S3 bucket
```

### Why CodePipeline Is Used

Airflow DAGs are code.

A cleaner way to manage DAG code is to keep it in GitHub and deploy it automatically instead of manually uploading DAG files to S3.

This creates a repeatable deployment process:

```text
update DAG code locally
commit changes
push to GitHub
CodePipeline detects the change
CodePipeline copies the repo contents to S3
MWAA reads the DAG files from S3
```

### Source Stage

The source stage is where CodePipeline gets the input code.

In this project:

```text
Source provider = GitHub via GitHub App
Repository = jenniferarias414/airflow-dags
Branch = main
```

When code is pushed to `main`, CodePipeline can run again.

### Build Stage

A build stage is used when code needs to be compiled, packaged, tested, or transformed before deployment.

This project skips the build stage because Airflow DAGs are Python files and do not need a compiled application build.

The goal is simply:

```text
copy repository files to S3
```

### Test Stage

A test stage can run automated tests.

This project skips the test stage because the course flow is focused on syncing DAG code to S3 and validating behavior later through AWS/Airflow.

A more advanced version could add tests for:

```text
Python syntax
DAG import errors
linting
unit tests
security checks
```

### Deploy Stage

The deploy stage sends the source artifact to S3.

In this project:

```text
Deploy provider = Amazon S3
Bucket = jennyarias-airflow-server-setup-v1
Extract file before deploy = enabled
```

The extraction setting matters.

Without extraction, S3 might receive one compressed artifact file.

With extraction, S3 receives the actual repo structure:

```text
dags/
docs/
learning-notes/
README.md
```

### What CodePipeline Is Not

CodePipeline is not Airflow.

CodePipeline deploys code.

Airflow runs workflows.

In this project:

```text
CodePipeline gets DAG files into S3.
Airflow later reads and runs DAG files from S3.
```

---

## Amazon S3

### What S3 Is

Amazon S3 is object storage.

It stores objects in buckets.

An object is usually a file plus metadata.

Examples:

```text
README.md
dags/load_api_to_aws_kinesis.py
raw/data.json
athena-query-results/result.csv
```

S3 is not a traditional database.

S3 does not automatically understand rows, columns, schemas, or SQL.

S3 stores files.

Other tools, like Athena, Glue, Spark, Redshift Spectrum, or Snowflake external stages, interpret those files.

### Bucket

A bucket is the top-level container in S3.

Examples from this project:

```text
jennyarias-airflow-server-setup-v1
jennyarias-athena-query-output-v1
```

### Prefix

S3 does not have real folders in the same way a local computer does.

What looks like a folder is usually a prefix in the object key.

Example:

```text
dags/load_api_to_aws_kinesis.py
```

The bucket is:

```text
jennyarias-airflow-server-setup-v1
```

The object key is:

```text
dags/load_api_to_aws_kinesis.py
```

The `dags/` part is a prefix.

### How S3 Is Used in This Project

This project uses S3 in multiple ways.

#### 1. S3 bucket for synced Airflow DAG code

CodePipeline copies GitHub repository contents into this bucket.

```text
GitHub → CodePipeline → S3 DAG/code bucket
```

MWAA can later read DAGs from this location.

#### 2. S3 bucket for Athena query output

Athena writes query results to S3.

Athena needs a query results location.

This project created a bucket for query output:

```text
jennyarias-athena-query-output-v1
```

#### 3. S3 storage for delivered data

Later in the project, Firehose is expected to deliver streamed records into S3.

Athena can then query those delivered files.

### Why S3 Matters in Data Engineering

S3 is commonly used as a data lake storage layer.

Companies use S3 to store:

```text
raw files
JSON data
CSV data
Parquet data
logs
streaming output
staged data
archive data
machine learning datasets
backup files
```

S3 is cheap and scalable, but it needs metadata and query engines to become useful for analytics.

---

## Athena

### What Athena Is

Amazon Athena is a serverless query service that lets you run SQL against data stored in S3.

Athena does not move the data into a traditional database before querying it.

Instead, Athena reads files directly from S3 using table definitions.

A better explanation:

```text
S3 stores the files.
Glue Data Catalog stores metadata about the files.
Athena uses that metadata to understand how to query the files with SQL.
```

### Can Athena Query Any S3 Bucket?

Not automatically.

Athena can query data in S3 when the data is accessible and described correctly.

For Athena to query S3 data successfully, several things usually need to be true:

```text
Athena has permission to read the S3 location.
The S3 files are in a supported format.
A table definition exists in the Data Catalog or is created with DDL.
The table definition points to the correct S3 LOCATION.
The schema matches the actual file structure.
Partitions are registered or projected if the data is partitioned.
The query results location is configured and writable.
```

So the simple answer is:

```text
Athena queries data in S3, but it needs metadata, permissions, compatible data layout, and a correct S3 location.
```

### Why the Data Catalog Matters

Athena needs to know what the files represent.

S3 only knows:

```text
bucket name
object key
object bytes
metadata
```

S3 does not know:

```text
table name
column names
data types
file delimiter
partition columns
whether a path represents a dataset
```

That information usually lives in the AWS Glue Data Catalog.

The Data Catalog acts like a metadata registry.

It stores information such as:

```text
database name
table name
column names
column data types
S3 LOCATION
file format
partition information
SerDe settings
```

If a Glue/Athena table exists but the table has no correct S3 location, Athena may not know where to read the data from.

If the S3 URI is missing, wrong, stale, or points to an empty path, the table can be visible but queries may fail or return no data.

### Database vs Table vs S3 Location

In Athena/Glue:

```text
Database = logical grouping of tables
Table = metadata definition for a dataset
LOCATION = S3 path where the dataset files live
```

A database by itself is not enough.

A table by itself is not enough if it does not correctly point to data.

Athena needs the table metadata to connect SQL to files.

Example:

```sql
CREATE EXTERNAL TABLE example_table (
  id string,
  event_time string,
  event_type string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://example-bucket/example-prefix/';
```

The `LOCATION` tells Athena where the files are.

Without the correct location, Athena does not know which S3 objects belong to the table.

### Supported File Formats

Athena can query common data formats such as:

```text
CSV
JSON
Parquet
ORC
Avro
text files
```

But the table definition must match the format.

For example, JSON data needs JSON-compatible table settings.

CSV data needs delimiter/header handling.

Parquet and ORC already include schema-like structure, but Athena still needs table metadata.

### Why File Format Matters

Athena reads files based on the table definition.

If the table says the data is CSV but the files are JSON, the query may fail or return incorrect results.

If the delimiter is wrong, columns may parse incorrectly.

If the schema says a field is an integer but the file contains non-numeric text, queries may fail or return nulls depending on the format and query.

### Why Partitions Matter

Many S3 datasets are partitioned.

Example:

```text
s3://company-data/events/year=2026/month=07/day=05/
```

Partitioning helps reduce the amount of data Athena scans.

Instead of scanning every file, Athena can scan only the matching partition.

Example:

```sql
SELECT *
FROM events
WHERE year = '2026'
  AND month = '07'
  AND day = '05';
```

However, Athena needs to know about the partitions.

Partitions can be added by:

```text
Glue crawler
MSCK REPAIR TABLE
ALTER TABLE ADD PARTITION
partition projection
```

If partitions exist in S3 but are not registered or projected, Athena may not find the data as expected.

### Why Permissions Matter

Athena needs permission to read S3 data and write query results.

Common permission needs:

```text
Athena/role/user can read the data bucket
Athena/role/user can list the data prefix
Athena/role/user can write to the query results bucket
Glue Data Catalog permissions allow reading table metadata
KMS permissions exist if the data is encrypted
Lake Formation permissions exist if governed by Lake Formation
```

If permissions are missing, the table may exist but queries can still fail.

### Why Query Results Location Matters

Athena writes query output to S3.

Even a simple `SELECT` query creates result files.

That is why Athena requires a query result location.

This project created:

```text
jennyarias-athena-query-output-v1
```

as the bucket for Athena query results.

### Common Athena/S3 Failure Patterns

#### Table exists but query returns no rows

Possible causes:

```text
table LOCATION points to the wrong S3 path
S3 prefix is empty
partitions are not registered
query filters exclude all data
files are not in expected format
```

#### Query says path does not exist or cannot be accessed

Possible causes:

```text
wrong S3 URI
missing S3 permissions
bucket/prefix deleted
Lake Formation restrictions
KMS encryption permissions missing
```

#### Query fails with parsing errors

Possible causes:

```text
table schema does not match file structure
wrong SerDe
wrong delimiter
bad JSON records
headers being read as data
unexpected data type values
```

#### Query works on some data but not other data

Possible causes:

```text
mixed file formats in same prefix
schema drift
partition folders have inconsistent files
corrupt files
new source data changed shape
```

### Athena Mental Model

A useful way to think about Athena:

```text
Athena is the SQL engine.
S3 is the file storage.
Glue Data Catalog is the map.
IAM/Lake Formation controls access.
The table LOCATION points the map to the files.
```

If the map is wrong, Athena cannot reliably query the data.

If access is missing, Athena cannot read the data.

If the file format and schema do not match, Athena cannot interpret the data correctly.

### Athena vs Snowflake

Athena queries files in S3.

Snowflake stores data in Snowflake-managed tables unless using external tables/stages.

Simple comparison:

```text
Athena = query files in S3
Snowflake = cloud data warehouse
S3 = object storage
Glue Data Catalog = metadata catalog for Athena
```

Athena is often good for:

```text
ad hoc querying of S3 data
data lake exploration
log analysis
querying partitioned files
serverless SQL over object storage
```

Snowflake is often better for:

```text
warehouse modeling
complex analytics workloads
managed performance
data marts
BI-ready curated tables
```

---

## Kinesis Data Streams

### What Kinesis Data Streams Is

Amazon Kinesis Data Streams is a streaming data service.

It receives records in near real time.

A record might be:

```json
{"userId": 1, "title": "example post"}
```

or:

```json
{"event_time": "2026-07-05T12:00:00Z", "event_type": "click"}
```

### Stream

A stream is the main Kinesis Data Streams resource.

Applications write records to a stream.

Other applications can read records from the stream.

### Record

A record is one unit of data sent to Kinesis.

A record has:

```text
data payload
partition key
sequence number
arrival timestamp
```

### Partition Key

The partition key determines which shard receives the record.

The partition key helps distribute records across shards.

Example:

```text
user_id
account_id
device_id
event_type
```

### Shard

A shard is a capacity unit within a Kinesis data stream.

A stream can have one or more shards.

Each shard holds an ordered sequence of records.

More shards can provide more throughput.

### Why Kinesis Data Streams Is Used

Kinesis Data Streams is useful when data arrives continuously or frequently.

Examples:

```text
clickstream events
application logs
IoT readings
transaction events
API events
real-time user activity
```

In this project, Airflow is expected to send API data into a Kinesis stream.

### What Kinesis Data Streams Does Not Do

Kinesis Data Streams receives and stores streaming records temporarily.

It does not automatically deliver records to S3 forever unless another service reads from it and writes them somewhere.

That is where Firehose comes in.

---

## Amazon Data Firehose

### What Firehose Is

Amazon Data Firehose is a managed delivery service for streaming data.

It can deliver records to destinations such as:

```text
Amazon S3
Amazon Redshift
Amazon OpenSearch
Splunk
HTTP endpoints
```

In this project, Firehose delivers records to S3.

### Kinesis Data Streams vs Firehose

These services are related but not the same.

```text
Kinesis Data Streams = receives and holds streaming records for consumers
Firehose = delivers streaming records to a destination
```

A simple mental model:

```text
Data Streams = stream pipe
Firehose = delivery truck
S3 = storage destination
```

### Why Firehose Is Used

Without Firehose, something else would need to read records from the Kinesis stream and write them to S3.

Firehose provides managed delivery.

It can buffer records and write them to S3 in batches.

### Buffering

Firehose does not always write every single record immediately as its own file.

It can buffer records by:

```text
time
size
```

Then it writes batches to S3.

This means there can be a delay between sending data to Kinesis and seeing files in S3.

### Firehose Output in S3

Firehose often writes objects using generated folder structures and filenames.

The output might look like:

```text
s3://bucket-name/prefix/2026/07/05/09/file-name
```

This output can later be queried by Athena if a table is defined correctly.

### Firehose and Transformations

Firehose can optionally transform records before delivery, often using AWS Lambda.

This project may not use that feature, but it is common in real systems.

Example:

```text
Kinesis records
→ Firehose
→ Lambda transform
→ S3
```

---

## Kinesis Data Streams and Firehose Together

### Why Use Both?

A project may use both when it wants:

```text
a real-time stream for producers/consumers
plus a managed delivery path into S3
```

The flow can be:

```text
Producer sends records
      ↓
Kinesis Data Stream receives records
      ↓
Firehose reads/delivers records
      ↓
S3 stores delivered files
      ↓
Athena queries the files
```

### What Could Produce Records?

In this project, Airflow can act as the producer.

The Airflow DAG can run Python code that:

```text
calls an API
gets JSON records
sends each record to Kinesis
```

In other projects, producers could be:

```text
web apps
mobile apps
backend services
IoT devices
Lambda functions
containers
servers
```

### What Could Consume Records?

Consumers could include:

```text
Firehose
Lambda
Kinesis Client Library applications
analytics applications
custom services
```

This project likely uses Firehose as the path from streaming records to S3.

---

## How These Services Work Together in This Project

### Code Path

```text
GitHub repository
      ↓
CodePipeline
      ↓
S3 DAG/code bucket
      ↓
MWAA reads DAG code
```

This part is about deploying workflow code.

### Data Path

```text
Airflow DAG
      ↓
API request / sample data
      ↓
Kinesis Data Stream
      ↓
Firehose
      ↓
S3 delivered data bucket
      ↓
Athena table
      ↓
SQL query
```

This part is about moving and querying data.

### Key Separation

Code and data are different.

```text
CodePipeline moves code.
Airflow runs code.
Kinesis receives records.
Firehose delivers records.
S3 stores files.
Athena queries files.
```

---

## Common Confusions

### Does Athena store the data?

No.

Athena queries data stored in S3.

Athena stores query results in S3, but the source data remains in S3.

### Does S3 know my table schema?

No.

S3 stores files.

The schema is stored in Glue Data Catalog / Athena table metadata.

### Can Athena query S3 without a table?

Usually, Athena queries through table definitions.

You need metadata that tells Athena how to interpret the files.

### If a Glue database exists, is that enough?

No.

A Glue database is just a container.

You need a table with a correct schema and S3 location.

### If a Glue table exists, is that enough?

Not always.

The table also needs:

```text
correct S3 LOCATION
correct schema
correct file format
registered/projected partitions if partitioned
permissions to read the data
```

### Why can I see a table but not query it successfully?

Because metadata visibility and data access are separate things.

You might be able to see the table definition but not read the underlying S3 files.

### Why can Athena query some S3 buckets but not others?

Possible reasons:

```text
permissions
missing table metadata
wrong S3 LOCATION
unsupported or mismatched file format
partition registration issues
KMS encryption
Lake Formation restrictions
cross-account access
```

### Is CodePipeline doing data processing?

No.

CodePipeline deploys code.

It is not the data processing engine.

### Is Airflow doing streaming?

Airflow can produce or trigger streaming activity, but Kinesis is the streaming service.

### Is Firehose the same as Kinesis Data Streams?

No.

Data Streams receives streaming records.

Firehose delivers streaming records to destinations like S3.

### Is S3 a database?

No.

S3 is object storage.

Athena makes S3 data queryable by using metadata and SQL execution.

---

## Practical Recognition Patterns

### If I see GitHub plus CodePipeline

The project probably has CI/CD deployment.

Ask:

```text
What source repo?
What branch?
What deploy target?
Is there a build stage?
What artifact is deployed?
```

### If I see MWAA

The project uses managed Airflow.

Ask:

```text
Where are DAGs stored?
What S3 bucket does MWAA read?
What execution role does MWAA use?
What permissions does the role need?
```

### If I see Kinesis Data Streams

The project is handling streaming records.

Ask:

```text
Who is the producer?
What is the partition key?
How many shards?
Who consumes the stream?
What is the retention period?
```

### If I see Firehose

The project is delivering streaming data somewhere.

Ask:

```text
What is the source?
What is the destination?
What is the buffering interval?
What is the error output location?
Is transformation enabled?
```

### If I see Athena

The project is querying files in S3.

Ask:

```text
Where is the S3 LOCATION?
What table definition exists?
What file format is used?
Are partitions registered?
Where do query results go?
Do permissions allow reading the source and writing results?
```

---

## Simple One-Minute Explanation

This project uses CodePipeline to deploy Airflow DAG code from GitHub into S3. MWAA reads that DAG code and runs the Airflow workflow. The Airflow DAG sends data into Kinesis Data Streams, Firehose delivers the data into S3, and Athena queries the S3 files using SQL. S3 stores the files, but Athena needs table metadata, correct S3 locations, compatible file formats, and permissions before it can query the data successfully.

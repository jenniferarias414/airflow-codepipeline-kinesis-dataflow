# Key Concepts

## Airflow

Airflow is the orchestration layer. It runs DAGs, tracks task status, and provides logs.

In this project, Airflow is expected to run Python tasks that move data into AWS services.

## CodePipeline

AWS CodePipeline is a CI/CD service.

In this project, CodePipeline is expected to copy Airflow DAG code from GitHub into the S3 bucket that MWAA watches.

This means DAG deployment can happen through GitHub instead of manual S3 uploads.

## Kinesis Data Stream

Kinesis Data Streams can receive streaming records.

In this project, a DAG may send API data into a Kinesis stream.

## Kinesis Firehose

Kinesis Firehose delivers streaming data to a destination such as S3.

In this project, Firehose is expected to receive or read streaming records and write them into S3.

## Amazon S3

S3 is object storage.

This project likely uses S3 for two purposes:

```text
1. Store Airflow DAG files for MWAA.
2. Store delivered data files for Athena queries.
```

## Athena

Athena queries data stored in S3 using SQL.

Athena does not move the data into a traditional database. It reads files from S3 through table definitions.

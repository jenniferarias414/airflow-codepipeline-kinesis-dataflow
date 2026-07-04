# Airflow and AWS CodePipeline Dataflow Project

This project builds a cloud dataflow pipeline using GitHub, AWS CodePipeline, Amazon MWAA / Apache Airflow, Amazon Kinesis, Amazon S3, and Amazon Athena.

The project demonstrates how Airflow DAG code can be deployed from GitHub to S3 using CodePipeline, then executed by MWAA to move API data through AWS streaming and storage services.

## Planned Architecture

```text
GitHub DAG Repository
        ↓
AWS CodePipeline
        ↓
S3 Airflow DAG Bucket
        ↓
Amazon MWAA / Apache Airflow
        ↓
Amazon Kinesis Data Stream
        ↓
Amazon Kinesis Firehose
        ↓
Amazon S3
        ↓
Amazon Athena
```

## Project Goals

- Use GitHub as the source location for Airflow DAG code.
- Use AWS CodePipeline to deploy DAG code into S3.
- Use MWAA / Airflow to orchestrate data movement.
- Use Kinesis services for streaming-style data ingestion.
- Store delivered data in S3.
- Query stored data using Athena.
- Document setup, validation, troubleshooting, and cleanup.

## Status

Project scaffold created. AWS build steps pending.

# Project Overview

## Purpose

This project demonstrates how multiple AWS services can work together in a dataflow pipeline.

The focus is not just moving data, but understanding how orchestration, deployment, streaming, storage, and querying fit together.

## Expected Flow

```text
GitHub
  ↓
CodePipeline
  ↓
S3 DAG Bucket
  ↓
MWAA / Airflow
  ↓
Kinesis
  ↓
Firehose
  ↓
S3
  ↓
Athena
```

## Main Concepts

| Concept | Meaning in This Project |
|---|---|
| GitHub | Stores Airflow DAG source code |
| CodePipeline | Deploys DAG code from GitHub to S3 |
| S3 DAG bucket | Stores DAG files for MWAA |
| MWAA / Airflow | Runs and monitors DAG workflows |
| Kinesis Data Stream | Receives streaming records |
| Kinesis Firehose | Delivers records to S3 |
| S3 storage bucket | Stores delivered JSON/data files |
| Athena | Queries files stored in S3 |

## Project Difference From Prior Snowflake Pipeline

The prior project used Airflow to orchestrate an ELT pipeline into Snowflake.

This project uses Airflow with AWS-native streaming and query services.

Simple comparison:

```text
Previous project:
Airflow → S3 → Snowflake Bronze/Silver/Gold

This project:
GitHub → CodePipeline → Airflow → Kinesis → Firehose → S3 → Athena
```

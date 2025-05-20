# client-datalake-pipelines

This repository contains ingestion pipelines for client.

## Structure
- common/: Shared modules (config, logger, utils)
- libs/: Zipped shared code (for Glue extra-py-files)
- scripts/: Pipeline scripts for Glue and other services
- scripts/athena/: Athena query SQLs
- workflows/: Glue Workflow definitions
- infrastructure/: IaC placeholders
- tests/: Test stubs/notebooks

## Naming Convention
All resource and folder names are generated from runtime args:
- client, env, domain, entity, source, connect, target, action

## How to scaffold a new pipeline job
Run:  
python scaffold.py --client acme --env dev --domain sales --entity order --source sap --target s3 --action ingest --connect appflow [--repo-prefix code/python-spark-sql] [--repo-root setup-guide]

import os

# Define the folder structure
folder_structure = {
    "sql-mastery": {
        "00_reference": [
            "glossary.md",
            "syntax_cheatsheet.md",
            "comparison_postgres_spark_delta.md"
        ],
        "01_ddl": {
            "partitioning_examples": [
                "postgres_partitioning.sql",
                "spark_partitioning.py",
                "delta_partitioning.sql"
            ],
            "schema_evolution": [
                "delta_schema_evolution.py",
                "iceberg_schema_evolution.sql",
                "hudi_schema_evolution.py"
            ],
            "__files__": [
                "create_table_postgres.sql",
                "create_table_spark.sql"
            ]
        },
        "02_dml": {
            "insert_update_delete": [
                "postgres_dml.sql",
                "spark_dml.sql",
                "delta_upsert_merge.py"
            ],
            "merge_examples": [
                "delta_merge.py",
                "iceberg_merge.sql",
                "hudi_upsert.py"
            ]
        },
        "03_dql": {
            "joins": [
                "inner_outer_joins.sql",
                "join_in_pyspark.py"
            ],
            "aggregations": [
                "groupby_having.sql",
                "groupby_pyspark.py"
            ],
            "window_functions": [
                "postgres_window.sql",
                "spark_window.sql",
                "pyspark_window.py"
            ],
            "__files__": [
                "select_basics.sql"
            ]
        },
        "04_tcl_dcl": [
            "tcl_postgres.sql",
            "delta_transactions.py",
            "grant_revoke_postgres.sql"
        ],
	"05_tcl_dcl": [
            "tcl_postgres.sql",
            "delta_transactions.py",
            "grant_revoke_postgres.sql"
        ],
        "06_io_bulk": [
            "postgres_copy.sql",
            "spark_read_write.py",
            "delta_bulk_load.py"
        ],
        "07_optimization": [
            "postgres_indexing.sql",
            "delta_optimize_vacuum.py",
            "iceberg_sorting.sql",
            "hudi_compaction.py"
        ],
        "08_metadata": [
            "postgres_metadata.sql",
            "spark_catalog_queries.sql",
            "delta_versioning.py"
        ],
        "09_case_studies": {
            "etl_queries": [
                "staging_transformation.sql",
                "pyspark_etl_pipeline.py"
            ],
            "scd_type_2": [
                "postgres_scd2.sql",
                "delta_scd2.py",
                "iceberg_scd2.sql"
            ],
            "cdc_tracking": [
                "hudi_cdc_tracking.py",
                "delta_incremental.py"
            ]
        }    }
}

def create_structure(base_path, structure):
    for name, content in structure.items():
        path = os.path.join(base_path, name)
        os.makedirs(path, exist_ok=True)
        if isinstance(content, list):
            for file in content:
                open(os.path.join(path, file), 'a').close()
        elif isinstance(content, dict):
            if "__files__" in content:
                for file in content["__files__"]:
                    open(os.path.join(path, file), 'a').close()
            subfolders = {k: v for k, v in content.items() if k != "__files__"}
            create_structure(path, subfolders)

# Create the folder structure
create_structure(".", folder_structure)

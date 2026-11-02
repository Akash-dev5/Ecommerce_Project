import logging

import pendulum

from airflow.sdk import dag, Asset, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig


logger = logging.getLogger(__name__)

# Paths inside the Airflow container
DBT_PROJECT_PATH = "/home/airflow/ecommerce_dbt"
DBT_PROFILES_PATH = "/home/airflow/ecommerce_dbt/.dbt/profiles.yml"
DBT_EXECUTABLE_PATH = "/home/airflow/dbt_venv/bin/dbt"



SNOWFLAKE_CONN_ID = "snowflake_conn"

RAW_BATCH_LOADED = Asset("x-snowflake://ecommerce/raw/monthly-batch-loaded")


# dbt project configuration
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=False,
)

profile_config = ProfileConfig(
    profile_name="ecommerce_dbt",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    dag_id="ecommerce_dbt_pipeline",
    description="Run e-commerce dbt transformations and tests.",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=[RAW_BATCH_LOADED],
    catchup=False,
    tags=["ecommerce", "dbt"],
)

def ecommerce_dbt_pipeline():

    dbt_transformations = DbtTaskGroup(
            group_id="dbt_transformations",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
    )


    @task
    def mark_dbt_completed():
        
        snowflake = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        
        pending_batches = snowflake.get_records(
            """
            SELECT BATCH_FOLDER
            FROM ECOMMERCE.RAW.BATCH_INGESTION_LOG
            WHERE DBT_COMPLETED_AT IS NULL
            ORDER BY PROCESSED_AT
            """
        )

        if len(pending_batches) == 0:
            raise ValueError(
                "No pending batch was found in BATCH_INGESTION_LOG. "
                "Cannot mark dbt completion."
            )

        if len(pending_batches) > 1:
            raise ValueError(
                "More than one pending batch was found. "
                "Expected exactly one pending batch."
            )


        batch_folder_name = pending_batches[0][0]

        logger.info(
            "Marking dbt completion for batch: %s",
            batch_folder_name,
        )

        # Update the completion timestamp for this batch only.
        snowflake.run(
            sql="""
                UPDATE ECOMMERCE.RAW.BATCH_INGESTION_LOG

                SET DBT_COMPLETED_AT =
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ

                WHERE BATCH_FOLDER = %s
                  AND DBT_COMPLETED_AT IS NULL
            """,
            parameters=(batch_folder_name,),
        )

        # Verify that the completion timestamp was updated.
        updated_batch = snowflake.get_records(
            """
            SELECT BATCH_FOLDER
            FROM ECOMMERCE.RAW.BATCH_INGESTION_LOG
            WHERE BATCH_FOLDER = %s
              AND DBT_COMPLETED_AT IS NOT NULL
            """,
            parameters=(batch_folder_name,),
        )

        if not updated_batch:
            raise RuntimeError(
                f"Could not confirm dbt completion for "
                f"{batch_folder_name}"
            )

        logger.info(
            "Successfully marked dbt completion for %s.",
            batch_folder_name,
        )


    dbt_completed = mark_dbt_completed()


    dbt_transformations >> dbt_completed


ecommerce_dbt_pipeline()
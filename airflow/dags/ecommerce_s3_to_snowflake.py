import logging
import re
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import Asset, dag, task

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)



#PROJECT CONFIGURATION

logger = logging.getLogger(__name__)

# Local directory mounted into the Airflow container through Docker Compose.
LOCAL_BATCH_ROOT = Path("/opt/airflow/Data/Batches")


# Amazon S3 configuration.
S3_BUCKET = "ecommerce-project11"
S3_BATCH_PREFIX = "batches/"


# AWS credentials will be supplied to the Airflow container
# through its environment variables.
# None tells S3Hook to use the default boto3 credential chain.
AWS_CONN_ID = None


# Snowflake connection configured in the Airflow UI.
SNOWFLAKE_CONN_ID = "snowflake_conn"

SNOWFLAKE_DATABASE = "ECOMMERCE"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_STAGE = "ECOMMERCE.RAW.ECOMMERCE_STAGE"
SNOWFLAKE_FILE_FORMAT = (
    "(FORMAT_NAME = 'ECOMMERCE.RAW.ECOMMERCE_CSV_FORMAT')"
)


# Monthly batch folders must follow this naming convention.
BATCH_PATTERN = re.compile(r"batch_\d{4}-(0[1-9]|1[0-2])")


# Five required transactional CSV files in every monthly batch.
REQUIRED_FILES = (
    "orders.csv",
    "customers.csv",
    "order_items.csv",
    "payments.csv",
    "reviews.csv",
)

# Maximum wait after dbt successfully completes before
# the next monthly ingestion batch can begin.
DBT_COOLDOWN_SECONDS = 300


# Python's ** (double asterisk) unpacking.
SNOWFLAKE_COPY_CONFIG = {
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
    "stage": SNOWFLAKE_STAGE,
    "file_format": SNOWFLAKE_FILE_FORMAT,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA,
}


# Shared Airflow Asset consumed by the dbt DAG.
RAW_BATCH_LOADED = Asset(
    "x-snowflake://ecommerce/raw/monthly-batch-loaded"
)



#DAG DEFINITION

@dag(
    dag_id="ecommerce_s3_to_snowflake",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ecommerce", "s3", "snowflake", "ingestion"],
)


def ecommerce_s3_to_snowflake():

    #DISCOVER THE NEXT ELIGIBLE MONTHLY BATCH
    @task
    def discover_next_batch() -> str | None:
        """Find the oldest complete monthly batch that has not
        already been ingested."""

        if not LOCAL_BATCH_ROOT.is_dir():
            raise FileNotFoundError(
                f"Batch directory does not exist: {LOCAL_BATCH_ROOT}"
            )

        available_folders = []

        # Inspect immediate entries inside the Batches directory.
        for folder_path in LOCAL_BATCH_ROOT.iterdir():

            # Ignore files and keep only directories.
            if not folder_path.is_dir():
                continue

            # Ignore folders that do not follow batch_YYYY-MM.
            if not BATCH_PATTERN.fullmatch(folder_path.name):
                continue

            # Check whether all five required CSV files exist.
            missing_files = [
                filename
                for filename in REQUIRED_FILES
                if not (folder_path / filename).is_file()
            ]

            if missing_files:
                logger.warning(
                    "Skipping %s because files are missing: %s",
                    folder_path.name,
                    missing_files,
                )
                continue

            available_folders.append(folder_path.name)

        # Connect to Snowflake and read the ingestion log.
        snowflake = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        batch_records = snowflake.get_records(
            """
            SELECT
                BATCH_FOLDER,
                DBT_COMPLETED_AT
            FROM ECOMMERCE.RAW.BATCH_INGESTION_LOG
            """
        )

        # Find batches whose ingestion finished but whose
        # dbt pipeline has not completed successfully yet.
        pending_batches = [
            row[0]
            for row in batch_records
            if row[1] is None
        ]

        if pending_batches:
            logger.info(
                "Waiting for dbt completion. Pending batches: %s",
                pending_batches,
            )
            return None

        # Find the most recent successful dbt completion time.
        cooldown_records = snowflake.get_records(
            """
            SELECT DATEDIFF(
                'SECOND',
                MAX(DBT_COMPLETED_AT),
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            )
            FROM ECOMMERCE.RAW.BATCH_INGESTION_LOG
            """
        )

        seconds_since_last_dbt = cooldown_records[0][0]

        # Enforce the minimum five-minute cooldown.
        if (
            seconds_since_last_dbt is not None
            and seconds_since_last_dbt < DBT_COOLDOWN_SECONDS
        ):
            logger.info(
                "Waiting for dbt cooldown. "
                "Elapsed seconds: %s",
                seconds_since_last_dbt,
            )
            return None

        # Collect every batch already recorded in Snowflake.
        processed_batches = {
            row[0]
            for row in batch_records
        }

        # Sort to process the oldest monthly batch first.
        for folder in sorted(available_folders):

            if folder in processed_batches:
                continue

            logger.info(
                "Selected next eligible batch: %s",
                folder,
            )

            # Return only one batch per DAG run.
            return folder

        logger.info("No eligible monthly batch is available.")

        return None



    #CHOOSE WHICH BRANCH SHOULD RUN
    @task.branch
    def choose_batch_path(batch_folder: str | None) -> str:
        """
        Choose between uploading a batch and doing nothing.
        """

        if batch_folder is None:
            return "no_batch_available"

        return "upload_batch_to_s3"



    #UPLOAD THE SELECTED BATCH TO AMAZON S3
    @task
    def upload_batch_to_s3(batch_folder: str) -> None:
        """
        Upload the five CSV files belonging to the selected batch.
        Existing S3 objects are left unchanged.
        """

        local_batch_dir = LOCAL_BATCH_ROOT / batch_folder

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        existing_keys = set(
            s3.list_keys(
                bucket_name=S3_BUCKET,
                prefix=f"{S3_BATCH_PREFIX}{batch_folder}/",
            ) or []
        )

        for filename in REQUIRED_FILES:

            # Construct the full local file path.
            local_file = local_batch_dir / filename

            if not local_file.is_file():
                raise FileNotFoundError(
                    f"Required file disappeared: {local_file}"
                )

            # Construct the destination key in S3.
            s3_key = (
                f"{S3_BATCH_PREFIX}{batch_folder}/{filename}"
            )

            # Avoid uploading an object that already exists.
            if s3_key in existing_keys:
                logger.info(
                    "S3 object already exists. Skipping: %s",
                    s3_key,
                )
                continue

            # Upload the local CSV file to S3.
            s3.load_file(
                filename=str(local_file),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,
            )

            logger.info(
                "Uploaded %s to s3://%s/%s",
                local_file,
                S3_BUCKET,
                s3_key,
            )

        logger.info(
            "S3 upload task completed for %s",
            batch_folder,
        )



    #NO-BATCH BRANCH
    @task
    def no_batch_available() -> None:
        """Log that there is no eligible batch to process."""

        logger.info(
            "Nothing to ingest. "
            "The next scheduled poll will check again."
        )



    #BUILD THE FIVE S3 FILE PATHS
    @task(multiple_outputs=True)
    def build_s3_file_paths(
        batch_folder: str,
    ) -> dict[str, str]:
        """Construct the S3 object paths for the five CSV files."""

        return {
            "orders": (f"{S3_BATCH_PREFIX}{batch_folder}/orders.csv"),
            "customers": (f"{S3_BATCH_PREFIX}{batch_folder}/customers.csv"),
            "order_items": (f"{S3_BATCH_PREFIX}{batch_folder}/order_items.csv"),
            "payments": (f"{S3_BATCH_PREFIX}{batch_folder}/payments.csv"),
            "reviews": (f"{S3_BATCH_PREFIX}{batch_folder}/reviews.csv"),
        }



    @task(outlets=[RAW_BATCH_LOADED])
    def record_batch_processed(batch_folder: str) -> None:
        """Record the successfully loaded batch in Snowflake."""

        snowflake = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        snowflake.run(
            sql="""
                MERGE INTO
                    ECOMMERCE.RAW.BATCH_INGESTION_LOG AS target

                USING (
                    SELECT %s AS BATCH_FOLDER
                ) AS source

                ON target.BATCH_FOLDER = source.BATCH_FOLDER

                WHEN NOT MATCHED THEN
                    INSERT (BATCH_FOLDER)
                    VALUES (source.BATCH_FOLDER)
            """,
            parameters=(batch_folder,),
        )

        logger.info(
            "Successfully recorded batch %s in Snowflake.",
            batch_folder,
        )


    #TASKFLOW TASK INVOCATIONS
    batch_folder = discover_next_batch()
    branch = choose_batch_path(batch_folder)
    upload = upload_batch_to_s3(batch_folder)
    no_batch = no_batch_available()
    batch_files = build_s3_file_paths(batch_folder)



    # These operators receive TaskFlow outputs directly.
    # No manual ti.xcom_pull() or Jinja XCom expressions.

    copy_orders = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_orders",
        files=[batch_files["orders"]],
        table="RAW_ORDERS",
        **SNOWFLAKE_COPY_CONFIG,
    )

    copy_customers = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_customers",
        files=[batch_files["customers"]],
        table="RAW_CUSTOMERS",
        **SNOWFLAKE_COPY_CONFIG,
    )

    copy_order_items = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_order_items",
        files=[batch_files["order_items"]],
        table="RAW_ORDER_ITEMS",
        **SNOWFLAKE_COPY_CONFIG,
    )

    copy_payments = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_payments",
        files=[batch_files["payments"]],
        table="RAW_PAYMENTS",
        **SNOWFLAKE_COPY_CONFIG,
    )

    copy_reviews = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_reviews",
        files=[batch_files["reviews"]],
        table="RAW_REVIEWS",
        **SNOWFLAKE_COPY_CONFIG,
    )



    record_batch = record_batch_processed(batch_folder)



    branch >> [upload, no_batch]
    upload >> batch_files
    batch_files >> [
        copy_orders,
        copy_customers,
        copy_order_items,
        copy_payments,
        copy_reviews,
    ]
    [
        copy_orders,
        copy_customers,
        copy_order_items,
        copy_payments,
        copy_reviews,
    ] >> record_batch



ecommerce_s3_to_snowflake()
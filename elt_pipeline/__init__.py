from dotenv import load_dotenv

import dagster as dg

from .assets import bronze, silver, gold
from .io_manager import MinioIOManager
from .resources import MinioResource, SparkResource


load_dotenv()


minio_resource = MinioResource(
    endpoint=dg.EnvVar("MINIO_ENDPOINT"),
    access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
    secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
    secure=False,
)

spark_resource = SparkResource(
    # "local[*]" while Dagster runs outside Docker.
    # Switch to "spark://spark-master:7077" once Dagster is containerised.
    spark_master=dg.EnvVar("SPARK_MASTER_URL"),
    minio_endpoint=dg.EnvVar("SPARK_MINIO_ENDPOINT"),
    access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
    secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
)


defs = dg.Definitions(
    assets=dg.load_assets_from_modules([bronze, silver, gold]),
    resources={
        "minio_io": MinioIOManager(
            minio=minio_resource,
            bucket_name="lakehouse",
            partition_by=["ingested_at"],
        ),
        "minio_client": minio_resource,
        "spark": spark_resource,
    },
)
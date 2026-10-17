from pyspark.sql import DataFrame
from src.config import S3_BUCKET

def build_silver_path(dataset: str) -> str:
    if not dataset.strip():
        raise ValueError("O nome do DataSet nao pode ser vazio")
    return f"s3a://{S3_BUCKET}/silver/{dataset}"

def write_parquet(df: DataFrame, path: str) -> None:
    (
        df.write.
        mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(path)
    )


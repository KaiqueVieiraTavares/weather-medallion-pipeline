from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import DataFrame
SILVER_COLUMNS = [
    # Identificação da localização
    "location_id",
    "city",
    "latitude",
    "longitude",

    # Horário da medição
    "event_timestamp",

    # Medições meteorológicas
    "temperature",
    "apparent_temperature",
    "relative_humidity",
    "precipitation",
    "pressure_msl",
    "cloud_cover",
    "wind_speed",
    "wind_direction",
    "weather_code",

    # Rastreabilidade
    "source",
    "ingested_at",
    "processed_at",

    # Particionamento
    "year",
    "month",
    "day",
]


def normalize_hourly_data(df: DataFrame) -> DataFrame:
    df_zipped = df.withColumn("hourly_per_register", f.explode(f.arrays_zip("hourly.time", "hourly.temperature_2m",
                                                                        "hourly.relative_humidity_2m", "hourly.apparent_temperature",
                                                                        "hourly.precipitation","hourly.pressure_msl",
                                                                        "hourly.cloud_cover", "hourly.wind_speed_10m",
                                                                        "hourly.wind_direction_10m", "hourly.weather_code")))
    return df_zipped

def flatten_hourly_data(df: DataFrame) -> DataFrame:
    df_desaninhado = df.select("*", "hourly_per_register.*").drop("hourly_per_register", "hourly")

    df_renomeado = (
        df_desaninhado.withColumnRenamed("temperature_2m", "temperature")
        .withColumnRenamed("relative_humidity_2m", "relative_humidity")
        .withColumnRenamed("wind_speed_10m", "wind_speed")
        .withColumnRenamed("wind_direction_10m", "wind_direction")
        .withColumnRenamed("time", "event_timestamp")
    )
    return df_renomeado

def cast_columns(df: DataFrame) -> DataFrame:
    TYPE_MAP = {
        # Double
        "latitude": t.DoubleType(),
        "longitude": t.DoubleType(),
        "temperature": t.DoubleType(),
        "apparent_temperature": t.DoubleType(),
        "precipitation": t.DoubleType(),
        "pressure_msl": t.DoubleType(),
        "wind_speed": t.DoubleType(),
        # Integer
        "relative_humidity": t.IntegerType(),
        "cloud_cover": t.IntegerType(),
        "wind_direction": t.IntegerType(),
        "weather_code": t.IntegerType(),
        # Timestamp
        "event_timestamp": t.TimestampType(),
    }
    exprs = []
    for c in df.columns:
        if c in TYPE_MAP:
            exprs.append(f.col(c).cast(TYPE_MAP[c]).alias(c))
        else:
            exprs.append(f.col(c))
    return df.select(*exprs)

def add_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", f.current_timestamp)


def add_partition_columns(df: DataFrame) -> DataFrame:
    return (df.withColumn("year", f.year(f.col("event_timestamp")))
            .withColumn("month", f.month(f.col("event_timestamp")))
            .withColumn("day", f.dayofmonth(f.col("event_timestamp")))
            )


def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["location_id", "event_timestamp", "source"])

def select_silver_columns(df: DataFrame) -> DataFrame:
    return df.select(*SILVER_COLUMNS)

def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    df = normalize_hourly_data(df)
    df = flatten_hourly_data(df)
    df = cast_columns(df)
    df = add_metadata(df)
    df = remove_duplicates(df)
    df = add_partition_columns(df)
    df = select_silver_columns(df)

    return df
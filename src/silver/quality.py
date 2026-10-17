from pyspark.sql import DataFrame
from pyspark.sql import functions as f 

REQUIRED_COLUMNS = [
    "location_id",
    "city",
    "latitude",
    "longitude",
    "event_timestamp",
    "temperature",
    "apparent_temperature",
    "relative_humidity",
    "precipitation",
    "surface_pressure",
    "cloud_cover",
    "wind_speed",
    "wind_direction",
    "weather_code",
    "source",
    "processed_at",
    "year",
    "month",
    "day"
]
#colunas que nao podem possuir valores nulos
REQUIRED_NOT_NULL_COLUMNS = [
    "location_id",
    "event_timestamp",
    "temperature",
    "relative_humidity",
    "source"
]
DEDUPLICATION_KEYS = [
    "location_id",
    "event_timestamp",
    "source",
]
def validate_required_columns(df: DataFrame) -> bool:
    
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            return False
    return True

def validate_values_not_null(df: DataFrame) -> bool:
    for col_name in REQUIRED_NOT_NULL_COLUMNS:
        hasNull = df.filter(f.col(col_name).isNull()).take(1)
        if hasNull:
            return False
    return True

def validate_numeric_ranges(df: DataFrame) -> bool:
    humidity_invalid = df.filter((f.col("relative_humidity") < 0) 
                                 | (f.col("relative_humidity") > 100)).take(1)
    precipitation_invalid = df.filter(f.col("precipitation") <0).take(1)
    wind_speed_invalid = df.filter(f.col("wind_speed") <0).take(1)
    if humidity_invalid or precipitation_invalid or wind_speed_invalid:
        return False
    return True

def validate_not_duplicates(df: DataFrame) -> bool:
    return df.count() == df.dropDuplicates(DEDUPLICATION_KEYS).count()

def validate_silver_quality(df: DataFrame) -> bool:
    return (
        validate_required_columns(df)
        and validate_values_not_null(df)
        and validate_numeric_ranges(df)
        and validate_not_duplicates(df)
    )
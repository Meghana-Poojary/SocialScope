from pyspark.sql.types import (ArrayType, BooleanType, LongType, StringType,
                               StructField, StructType)

POST_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("platform", StringType()),
    StructField("brand", StringType()),
    StructField("product", StringType()),
    StructField("author", StringType()),
    StructField("author_followers", LongType()),
    StructField("is_brand_account", BooleanType()),
    StructField("text", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("likes", LongType()),
    StructField("comments", LongType()),
    StructField("shares", LongType()),
    StructField("views", LongType()),
    StructField("created_at", StringType()),
    StructField("source", StringType()),
])

METRIC_SCHEMA = StructType([
    StructField("brand", StringType()),
    StructField("platform", StringType()),
    StructField("date", StringType()),
    StructField("followers", LongType()),
])

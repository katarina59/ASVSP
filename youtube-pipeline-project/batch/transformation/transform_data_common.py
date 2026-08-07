from pyspark.sql.functions import (
    col, lit, to_date, when, month, year, array, concat_ws, concat, size,
    expr, regexp_replace, trim, length, split as spark_split
) # type: ignore

region_map = {
    "CA": 1, "DE": 2, "FR": 3, "GB": 4, "IN": 5,
    "JP": 6, "KR": 7, "MX": 8, "RU": 9, "US": 10
}


def clean_and_enrich(df, region=None):
    """Zajednička transformaciona/enrichment logika za golden dataset.

    Ulazni df mora imati kolone: video_id, video_title, channel_title, publish_time,
    views, likes, dislikes, comment_count, category_id, category_title, assignable,
    tags_list (array<string>), thumbnail_link, comments_disabled, ratings_disabled,
    video_error_or_removed, trending_date, description, region.

    Ovu funkciju poziva i transform_data.py --mode initial (DAG1 task transform_data)
    i transform_data.py --mode incremental (DAG3 task trigger_transform_data), čime
    se izbegava dupliranje logike na dva mesta.
    """

    split_date = spark_split(col("trending_date"), "\\.")

    enriched = df.withColumn(
        "trending_date_fixed",
        concat_ws(".", split_date.getItem(1), split_date.getItem(2), split_date.getItem(0))
    ).withColumn(
        "trending_full_date",
        to_date(col("trending_date_fixed"), "dd.MM.yy")
    ).withColumn(
        "trending_month",
            when(month(col("trending_full_date")) == 1, "Januar")
            .when(month(col("trending_full_date")) == 2, "Februar")
            .when(month(col("trending_full_date")) == 3, "Mart")
            .when(month(col("trending_full_date")) == 4, "April")
            .when(month(col("trending_full_date")) == 5, "Maj")
            .when(month(col("trending_full_date")) == 6, "Jun")
            .when(month(col("trending_full_date")) == 7, "Jul")
            .when(month(col("trending_full_date")) == 8, "Avgust")
            .when(month(col("trending_full_date")) == 9, "Septembar")
            .when(month(col("trending_full_date")) == 10, "Oktobar")
            .when(month(col("trending_full_date")) == 11, "Novembar")
            .when(month(col("trending_full_date")) == 12, "Decembar")
    ).withColumn(
        "trending_year", year(col("trending_full_date"))
    ).withColumn(
        "publish_date", to_date(col("publish_time"))
    ).withColumn(
        "publish_month", month(col("publish_time"))
    ).withColumn(
        "publish_year", year(col("publish_time"))
    ).withColumn(
        "region_id",
        when(col("region") == "CA", region_map["CA"])
        .when(col("region") == "DE", region_map["DE"])
        .when(col("region") == "FR", region_map["FR"])
        .when(col("region") == "GB", region_map["GB"])
        .when(col("region") == "IN", region_map["IN"])
        .when(col("region") == "JP", region_map["JP"])
        .when(col("region") == "KR", region_map["KR"])
        .when(col("region") == "MX", region_map["MX"])
        .when(col("region") == "RU", region_map["RU"])
        .when(col("region") == "US", region_map["US"])
    ).withColumn(
        "video_title",
        regexp_replace(col("video_title"), "[^\\x20-\\x7E]", "")
    ).withColumn(
        "video_title", trim(col("video_title"))
    ).withColumn(
        "video_title",
        when(length(col("video_title")) > 100,
            concat(col("video_title").substr(1, 97), lit("...")))
        .otherwise(col("video_title"))
    ).filter(
        (col("video_title").isNotNull()) &
        (col("views") > 0)
    ).withColumn(
        "video_title", regexp_replace(col("video_title"), "\\s+", " ")
    ).withColumn(
        "tags_list",
        when(size(col("tags_list")) == 0, array(lit("no-tags")))
        .otherwise(
            expr("filter(tags_list, x -> trim(x) != '' AND x IS NOT NULL)")
        )
    ).withColumn(
        "tags_list",
        when(size(col("tags_list")) == 0, array(lit("unknown")))
        .otherwise(col("tags_list"))
    )

    return enriched


GOLDEN_DATASET_COLUMNS = [
    "video_id", "video_title", "channel_title", "publish_time", "views", "likes",
    "dislikes", "comment_count", "category_id", "category_title", "assignable",
    "tags_list", "thumbnail_link", "comments_disabled", "ratings_disabled",
    "video_error_or_removed", "trending_date", "description", "trending_date_fixed",
    "trending_full_date", "trending_month", "trending_year", "publish_date",
    "publish_month", "publish_year", "region", "region_id"
]

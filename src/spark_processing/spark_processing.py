import os
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import from_unixtime, to_timestamp, to_date, col, trim, date_format, lower, regexp_replace, length, size, split, dayofweek, udf, when

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

POST_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", LongType(), True),
    StructField("selftext", StringType(), True),
    StructField("fetch_type", StringType(), True),
    StructField("author_comment_karma", IntegerType(), True),
    StructField("author_link_karma", IntegerType(), True),
])

COMMENT_SCHEMA = StructType([
    StructField("comment_id", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", LongType(), True),
    StructField("author_comment_karma", IntegerType(), True),
    StructField("author_link_karma", IntegerType(), True),
])

class SparkProcessor:
    def __init__(
        self,
        data_path: str = "data",
        master: str = "local[*]",
        app_name: str = "RedditPostsReader",
        spark: Optional[SparkSession] = None,
        shuffle_partitions: int = 8,
        hadoop_home: Optional[str] = None
    ):
        self.data_path = Path(data_path)
        self.raw_dir = self.data_path / "raw"
        self.raw_posts_dir = self.raw_dir / "posts"
        self.raw_comments_dir = self.raw_dir / "comments"
        self.output_dir = self.data_path / "processed"
        self.output_dir_posts = self.output_dir / "posts"
        self.output_dir_comments = self.output_dir / "comments"

        if hadoop_home:
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] += os.pathsep + str(Path(hadoop_home) / "bin")
            logger.info("HADOOP_HOME impostato a: %s", hadoop_home)

        if spark is None:
            self.spark = (
                SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
                .config("spark.sql.files.ignoreMissingFiles", "true")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .getOrCreate()
            )
        else:
            self.spark = spark

    @staticmethod
    def count_caps_words(text: str) -> int:
        if not text:
            return 0
        words = text.split()
        return sum(1 for w in words if w.isupper() and len(w) > 1)

    def read_posts(self) -> DataFrame:
        csv_files = list(self.raw_posts_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Nessun CSV trovato in {self.raw_posts_dir}")
        logger.info("CSV files found: %s", [str(f) for f in csv_files])
        df = (
            self.spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .option("ignoreLeadingWhiteSpace", "true")
            .schema(POST_SCHEMA)
            .csv([str(f) for f in csv_files])
        )
        logger.info("Read posts with explicit schema. Rows: %s", df.count())
        return df

    def read_comments(self) -> DataFrame:
        csv_files = list(self.raw_comments_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Nessun CSV trovato in {self.raw_comments_dir}")
        logger.info("CSV files found: %s", [str(f) for f in csv_files])
        df = (
            self.spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .option("ignoreLeadingWhiteSpace", "true")
            .schema(COMMENT_SCHEMA)
            .csv([str(f) for f in csv_files])
        )
        logger.info("Read comments with explicit schema. Rows: %s", df.count())
        return df

    def _generic_normalize(self, df: DataFrame) -> DataFrame:
        if df is None:
            raise ValueError("df is None")
        id_columns = [c for c in df.columns if c.endswith("_id")]
        for c in id_columns:
            df = df.withColumn(c, col(c).cast("string"))
            df = df.withColumn(c, trim(col(c)))
        if "author_comment_karma" in df.columns:
            df = df.withColumn("author_comment_karma", col("author_comment_karma").cast("long"))
        if "author_link_karma" in df.columns:
            df = df.withColumn("author_link_karma", col("author_link_karma").cast("long"))
        if "created_utc" in df.columns:
            df = df.withColumn("created_utc", col("created_utc").cast("long"))
            df = df.withColumn("created_ts", to_timestamp(from_unixtime(col("created_utc"))))
            df = df.withColumn("created_date", to_date(col("created_ts")))
            df = df.withColumn("created_time", date_format(col("created_ts"), "HH:mm"))
        if "author" in df.columns:
            df = df.withColumn("author", trim(col("author")))
        if "score" in df.columns:
            df = df.withColumn("score", col("score").cast("long"))
        return df

    def _normalize_comments(self, df: DataFrame) -> DataFrame:
        if df is None:
            raise ValueError("df is None")
        if "comment_id" in df.columns:
            df = df.dropna(subset=["comment_id"]).dropDuplicates(["comment_id"])
        if "body" in df.columns:
            df = df.withColumn("body", col("body").cast("string"))
            df = df.withColumn("body", regexp_replace(col("body"), r"[\r\n\t]+", " "))
            df = df.withColumn("body", regexp_replace(col("body"), r"\s+", " "))
            df = df.withColumn("body", trim(col("body")))
            df = df.withColumn("body", regexp_replace(col("body"), r"\"", "'"))
            df = df.withColumn("body", regexp_replace(col("body"), r"[^\x00-\x7F]+", ""))
            df = df.withColumn("body", lower(col("body")))
        df = df.withColumn("body_len_chars", length(col("body")))
        df = df.withColumn("body_len_words", size(split(col("body"), " ")))
        df = df.withColumn("num_urls", size(split(col("body"), r"https?://\S+")))
        df = df.withColumn("created_dayofweek", dayofweek(col("created_ts")))
        return df

    def _normalize_posts(self, df: DataFrame) -> DataFrame:
        if df is None:
            raise ValueError("df is None")
        if "num_comments" in df.columns:
            df = df.withColumn("num_comments", col("num_comments").cast("long"))
        if "subreddit" in df.columns:
            df = df.withColumn("subreddit", trim(col("subreddit")))
            df = df.withColumn("subreddit", lower(col("subreddit")))
        for text_col in ["title", "selftext"]:
            if text_col in df.columns:
                df = df.withColumn(text_col, col(text_col).cast("string"))
                df = df.withColumn(text_col, regexp_replace(col(text_col), r"[\r\n\t]+", " "))
                df = df.withColumn(text_col, regexp_replace(col(text_col), r"\s+", " "))
                df = df.withColumn(text_col, trim(col(text_col)))
                df = df.withColumn(text_col, regexp_replace(col(text_col), r"\"", "'"))
                df = df.withColumn(text_col, regexp_replace(col(text_col), r"[^\x00-\x7F]+", ""))
                df = df.withColumn(text_col, lower(col(text_col)))
        if "post_id" in df.columns:
            df = df.dropna(subset=["post_id"]).dropDuplicates(["post_id"])
        return df

    def process_and_save_comments(self) -> Path:
        df_raw = self.read_comments()
        df_norm = self._generic_normalize(df_raw)
        df_norm = self._normalize_comments(df_norm)
        self.output_dir_comments.mkdir(parents=True, exist_ok=True)
        df_norm.coalesce(1).write.option("header", "true").mode("overwrite").csv(str(self.output_dir_comments))
        logger.info("Comments normalizzati salvati in: %s", self.output_dir_comments)
        return self.output_dir_comments

    def process_and_save_posts(self) -> Path:
        df_raw = self.read_posts()
        df_norm = self._generic_normalize(df_raw)
        df_norm = self._normalize_posts(df_norm)
        self.output_dir_posts.mkdir(parents=True, exist_ok=True)
        df_norm.coalesce(1).write.option("header", "true").mode("overwrite").csv(str(self.output_dir_posts))
        logger.info("Posts normalizzati salvati in: %s", self.output_dir_posts)
        return self.output_dir_posts

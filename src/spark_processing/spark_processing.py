import os
import argparse
import logging
from pathlib import Path
from typing import Optional



from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)
from pyspark.sql.functions import (
    from_unixtime, to_timestamp, to_date, col, unix_timestamp,
    count as _count, avg as _avg, min as _min, max as _max, trim
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

POST_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", LongType(), True), 
    StructField("selftext", StringType(), True),
])

COMMENT_SCHEMA = StructType([
    StructField("comment_id", StringType(), True),
    StructField("post_id", StringType(), True),  
    StructField("parent_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", LongType(), True),
])

class SparkProcessor:
    def __init__(
        self,
        raw_dir: str = "data",
        posts_subdir: str = "posts",
        master: str = "local[*]",
        app_name: str = "RedditPostsReader",
        spark: Optional[SparkSession] = None,
        shuffle_partitions: int = 8,
    ):
        self.raw_dir = Path(raw_dir)
        self.posts_subdir = Path(posts_subdir)
        self.posts_dir = self.raw_dir / self.posts_subdir
        logger.info("Posts directory: %s", self.posts_dir)

        if spark is None:
            self.spark = (
                SparkSession.builder
                .appName(app_name)
                .master(master)
                .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
                .getOrCreate()
            )
        else:
            self.spark = spark

        logger.info("Initialized SparkProcessor raw=%s posts=%s", self.raw_dir, self.posts_dir)

    def read_posts(self) -> DataFrame:
        pattern = str(self.posts_dir).replace("\\", "/") + "/*.csv"
        logger.info("Reading posts from pattern: %s", pattern)

        #prova
        from pathlib import Path
        csv_files = list(self.posts_dir.glob("*.csv"))
        print("CSV files:", csv_files)

        try:
            df = (
                self.spark.read
                .option("header", "true")
                .option("multiLine", "true")   # se testi multilinea nel CSV
                .option("escape", "\"")
                .option("ignoreLeadingWhiteSpace", "true")
                .schema(POST_SCHEMA)
                .csv([str(f) for f in csv_files])  # legge solo i file esistenti
            )
            logger.info("Read posts with explicit schema. Rows: %s", df.count())
            return df
        except Exception as e:
            logger.warning("Failed to read with explicit schema: %s. Falling back to permissive read.", e)

        # fallback: permissive read (inferSchema=False -> everything string), poi cast
        df = (
            self.spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .csv([str(f) for f in csv_files])  # schema inferred or as strings depending on Spark; we cast below
        )
        logger.info("Read posts permissively. Applying casts.")

        # pulizia e cast espliciti per colonne numeric / epoch
        # colonna created_utc potrebbe essere float/string -> cast to long
        df = df.withColumn("post_id", trim(col("post_id")))
        for int_col in ["score", "num_comments", "created_utc"]:
            if int_col in df.columns:
                df = df.withColumn(int_col, col(int_col).cast("long"))

        # Se manca created_utc, lascialo null; se manca post_id drop
        if "post_id" in df.columns:
            df = df.dropna(subset=["post_id"])
        logger.info("After casts, columns: %s", df.columns)
        return df

    def _normalize_posts(self, df: DataFrame) -> DataFrame:
        """
        Normalizza i tipi e le colonne utili:
        - created_utc (epoch) -> created_ts (timestamp) e created_date (date)
        - drop di post_id null e dedup su post_id
        - cast di numeric columns a int/long corretti
        """
        if df is None:
            raise ValueError("df is None")

        # assicuriamoci che created_utc sia long
        if "created_utc" in df.columns:
            df = df.withColumn("created_utc", col("created_utc").cast("long"))
            df = df.withColumn("created_ts", to_timestamp(from_unixtime(col("created_utc"))))
            df = df.withColumn("created_date", to_date(col("created_ts")))
        else:
            # evita crash se manca la colonna
            df = df.withColumn("created_ts", col("created_utc"))  # sarà null
            df = df.withColumn("created_date", col("created_utc"))

        # clean up basic string columns
        for s in ["post_id", "title", "author", "selftext"]:
            if s in df.columns:
                df = df.withColumn(s, trim(col(s)))

        # cast numeric
        for n in ["score", "num_comments"]:
            if n in df.columns:
                df = df.withColumn(n, col(n).cast("long"))

        # rimuovi righe senza post_id e dedup globali per post_id
        if "post_id" in df.columns:
            df = df.dropna(subset=["post_id"]).dropDuplicates(["post_id"])

        return df
    
    


# Esempio d'uso rapido (per test)
if __name__ == "__main__":
    posts_path = r"C:\Users\Gabri\Documents\reddit_project\data\politics\raw"
    #posts_path = "C:/Users/Gabri/Documents/reddit_project/data/politics/raw/posts"
    print("Posts:", posts_path, "=>", os.path.exists(posts_path))
    sp = SparkProcessor(raw_dir=posts_path, posts_subdir="posts")
    df_raw = sp.read_posts()
    print("RAW SCHEMA:")
    df_raw.printSchema()
    print("RAW SAMPLE:")
    df_raw.show(5, truncate=200)

    df_norm = sp._normalize_posts(df_raw)
    print("NORMALIZED SCHEMA:")
    df_norm.printSchema()
    print("NORMALIZED SAMPLE:")
    df_norm.show(5, truncate=200)
    sp.spark.stop()

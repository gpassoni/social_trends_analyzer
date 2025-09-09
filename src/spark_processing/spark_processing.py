import os
import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)
from pyspark.sql.functions import (
    from_unixtime, to_timestamp, to_date, col, trim, date_format, lower, regexp_replace, length, size, split, dayofweek, udf, when
)

# ------------------- LOGGING -------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------- SCHEMA -------------------
POST_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("author_comment_karma", IntegerType(), True),
    StructField("author_link_karma", IntegerType(), True),
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
    StructField("author_comment_karma", IntegerType(), True),
    StructField("author_link_karma", IntegerType(), True),
    StructField("body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", LongType(), True),
])


# ------------------- PROCESSOR -------------------
class SparkProcessor:
    def __init__(
        self,
        raw_dir: str = "data",
        posts_subdir: str = "posts",
        comments_subdir: str = "comments",
        master: str = "local[*]",
        app_name: str = "RedditPostsReader",
        spark: Optional[SparkSession] = None,
        shuffle_partitions: int = 8,
        hadoop_home: Optional[str] = None
    ):
        self.raw_dir = Path(raw_dir)
        self.posts_subdir = Path(posts_subdir)
        self.comments_subdir = Path(comments_subdir)
        self.posts_dir = self.raw_dir / self.posts_subdir
        self.comments_dir = self.raw_dir / self.comments_subdir
        logger.info("Posts directory: %s", self.posts_dir)

        #self.count_caps_words_udf = udf(self.count_caps_words, IntegerType())

        # Imposta HADOOP_HOME se necessario
        if hadoop_home:
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] += os.pathsep + str(Path(hadoop_home) / "bin")
            logger.info("HADOOP_HOME impostato a: %s", hadoop_home)

        # Inizializza Spark
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

        logger.info("Initialized SparkProcessor raw=%s posts=%s", self.raw_dir, self.posts_dir)

    @staticmethod
    def count_caps_words(text: str) -> int:
        if not text:
            return 0
        words = text.split()
        return sum(1 for w in words if w.isupper() and len(w) > 1)

    # ------------------- READ -------------------
    def read_posts(self) -> DataFrame:
        csv_files = list(self.posts_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Nessun CSV trovato in {self.posts_dir}")
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
        csv_files = list(self.comments_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"Nessun CSV trovato in {self.comments_dir}")

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
        # normalize columns that are equal between posts and comments:
        # created_utc, score, author, all the id columns
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
            #df = df.withColumn("caps_words_count", self.count_caps_words_udf(col("body")))
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

    def process_and_save_comments(self, output_dir: str = "processed_comments") -> Path:
        """
        Funzione finale: legge, normalizza e salva i commenti in CSV.
        Restituisce il path della cartella dei CSV salvati.
        """
        df_raw = self.read_comments()
        df_norm = self._generic_normalize(df_raw)
        df_norm = self._normalize_comments(df_norm)

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Salva in CSV (coalesce a 1 file)
        df_norm.coalesce(1).write.option("header", "true").mode("overwrite").csv(str(output_path))

        logger.info("Comments normalizzati salvati in: %s", output_path)
        return output_path

    # ------------------- PROCESS & SAVE -------------------
    def process_and_save_posts(self, output_dir: str = "processed_posts") -> Path:
        """
        Funzione finale: legge, normalizza e salva i post in CSV.
        Restituisce il path della cartella dei CSV salvati.
        """
        df_raw = self.read_posts()
        df_norm = self._generic_normalize(df_raw)
        df_norm = self._normalize_posts(df_norm)

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Salva in CSV (coalesce a 1 file)
        df_norm.coalesce(1).write.option("header", "true").mode("overwrite").csv(str(output_path))

        logger.info("Posts normalizzati salvati in: %s", output_path)
        return output_path

# ------------------- TEST COMPLETO -------------------
if __name__ == "__main__":
    # Directory dei dati raw
    posts_path = r"C:\Users\Gabri\Documents\reddit_project\data\politics\raw"
    print("Posts:", posts_path, "=>", os.path.exists(posts_path))

    hadoop_home = r"C:\hadoop" 

    sp = SparkProcessor(raw_dir=posts_path, posts_subdir="posts", hadoop_home=hadoop_home)
    saved_folder = sp.process_and_save_posts(output_dir=r"C:\Users\Gabri\Documents\reddit_project\data\politics\processed\posts")
    saved_folder_comments = sp.process_and_save_comments(output_dir=r"C:\Users\Gabri\Documents\reddit_project\data\politics\processed\comments")

    print("Processing completato! Controlla i file in:", saved_folder)

    sp.spark.stop()

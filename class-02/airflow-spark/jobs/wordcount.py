from __future__ import annotations
import os
import re
import shutil
import sys
from pyspark.sql import SparkSession


def main(input_path: str, output_path: str) -> None:
    # Clean previous output if exists (Spark fails if path exists)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    spark = (
        SparkSession.builder.appName("WordCount")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext

    text_rdd = sc.textFile(input_path)

    words = text_rdd.flatMap(lambda line: re.findall(r"\w+", line.lower()))
    counts = (
        words.map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda kv: (-kv[1], kv[0]))
    )

    # Save as text; coalesce to 1 for easier inspection (small dataset)
    counts.coalesce(1).saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount.py <input_path> <output_path>")
        sys.exit(1)
    in_path, out_path = sys.argv[1], sys.argv[2]
    main(in_path, out_path)

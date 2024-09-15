from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, concat_ws, length, monotonically_increasing_id
import pyspark.sql.functions as F
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

###### Create SparkSession ####
def create_spark_session():
    """Create a Spark session."""
    return SparkSession.builder.appName("PetitionWordCount").getOrCreate()

###### Read json file #####
def load_data(spark, file_path):
    """Load the JSON file into a DataFrame."""
    return spark.read.json(file_path)

###### Added columns petitionid(monotonically increasing) and text(concatenation of label and abstract) #####
def add_petition_id_and_text(df):
    """Add a petition_id column and concatenate 'label' and 'abstract' into a single text column."""
    return df.withColumn("petition_id", monotonically_increasing_id() + 1) \
             .withColumn("text", lower(concat_ws(" ", col("label._value"), col("abstract._value"))))


###### Coversion to lowercase and removal of punctuations and then splitting each words  #####
def clean_text(df):
    """Clean the text by removing punctuation and splitting into words."""
    return df.withColumn("words", split(regexp_replace(col("text"), "[^a-zA-Z\\s]", ""), "\\s+"))

###### exploding each words  #####
def explode_words(df):
    """Explode the 'words' column and filter words by length >= 5."""
    return df.withColumn("word", explode(col("words"))) \
             .filter(length(col("word")) >= 5)

###### Gouping and ordering  #####
def count_word_occurrences(df):
    """Count the occurrences of each word."""
    return df.groupBy("word").count().orderBy(col("count").desc())

###### Get common words  #####
def get_common_words(df, limit=20):
    """Get the most common words from the word counts."""
    return df.limit(limit).select("word").rdd.flatMap(lambda x: x).collect()

###### Add words count  #####
def add_word_columns(df, common_words):
    """Add a column for each of the 20 most common words with their occurrence count."""
    for word in common_words:
        df = df.withColumn(word, F.expr(f"size(filter(words, x -> x = '{word}'))"))
    return df

###### drop columns not required #####
def select_columns(df, columns_to_drop):
    columns_to_drop = ['abstract', 'label','numberOfSignatures','text','words']
    return df.drop(*columns_to_drop)

###### write to csv #####
def write_to_csv(df, output_path):
    """Write the DataFrame to a CSV file."""
    df.coalesce(1).write.option("header", "true").csv(output_path)

###### stopping spark session #####
def stop_spark_session(spark):
    """Stop the Spark session."""
    spark.stop()


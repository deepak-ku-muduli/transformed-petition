import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from transform_petitions import (create_spark_session, load_data, add_petition_id_and_text, clean_text, 
                         explode_words, count_word_occurrences, get_common_words, 
                         add_word_columns, select_columns)

@pytest.fixture(scope="module")
def spark_session():
    """Fixture for creating a Spark session."""
    return create_spark_session()

def test_load_data(spark_session):
    df = load_data(spark_session, "../input_data/input_data.json")
    
    assert df.count() > 0  # Check if the data is loaded

def test_add_petition_id_and_text(spark_session):
    df = spark_session.createDataFrame([Row(label={"_value": "label1"}, abstract={"_value": "abstract1"})])
    df_with_id = add_petition_id_and_text(df)
    assert "petition_id" in df_with_id.columns
    assert "text" in df_with_id.columns

def test_clean_text(spark_session):
    df = spark_session.createDataFrame([Row(text="MPs should attend all debates")])
    df_clean = clean_text(df)
    assert "words" in df_clean.columns
    assert df_clean.select("words").filter(col("words").isNull()).count() == 0
    words = df_clean.select("words").collect()[0][0]
    assert words == ["MPs", "should", "attend", "all", "debates"]

def test_explode_words(spark_session):
    # Create a sample DataFrame
    df = spark_session.createDataFrame([Row(words=["Mps", "should", "attend", "all", "debates"])])

    # Apply the explode_words function
    df_exploded = explode_words(df)

    # Assert that the 'word' column exists
    assert "word" in df_exploded.columns

    # Collect the exploded words into a list
    exploded_words = df_exploded.select("word").rdd.flatMap(lambda x: x).collect()

    # Assert that words with length < 5 are filtered out
    assert "Mps" not in exploded_words
    assert "all" not in exploded_words

    # Assert that words with length >= 5 are present
    assert "should" in exploded_words
    assert "attend" in exploded_words
    assert "debates" in exploded_words

    # Assert the number of rows is correct (only words >= 5 characters)
    assert len(exploded_words) == 3

def test_count_word_occurrences(spark_session):
    df = spark_session.createDataFrame([Row(word="hello"), Row(word="world"), Row(word="hello")])
    word_counts = count_word_occurrences(df)
    assert word_counts.filter(col("word") == "hello").select("count").collect()[0][0] == 2

def test_get_common_words(spark_session):
    df = spark_session.createDataFrame([Row(word="hello"), Row(word="world")])
    common_words = get_common_words(df, limit=1)
    assert common_words == ["hello"]

def test_add_word_columns(spark_session):
    df = spark_session.createDataFrame([Row(words=["hello", "world"])])
    common_words = ["hello", "world"]
    df_with_word_columns = add_word_columns(df, common_words)
    assert "hello" in df_with_word_columns.columns
    assert "world" in df_with_word_columns.columns

def test_select_columns(spark_session):
    df = spark_session.createDataFrame([Row(petition_id=1, hello=1, world=1)])
    selected_df = select_columns(df, ["hello", "world"])
    assert "petition_id" in selected_df.columns
    assert "hello" in selected_df.columns
    assert "world" in selected_df.columns
    assert "abstract" not in selected_df.columns

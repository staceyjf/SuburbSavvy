from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, length
from pyspark.sql.types import IntegerType
from config import Config


def main():
    spark = SparkSession.builder \
        .appName("Aus Property") \
        .config("spark.master", "local") \
        .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 100)

    # dataset
    csv_file_path = "data/aus-property-sales-sep2018-april2020.csv"

    # create a dataframe from reading the csv
    # use the headers from the csv
    df = spark.read.option("header", "true").csv(csv_file_path)

    df = df.orderBy("date_sold")

    # Java Database Connectivity URL for postgres and relating connection intergration
    jdbc_url = "jdbc:postgresql://localhost:5432/aus_property"
    connection_properties = {
        "user": Config.DB_USERNAME,
        "password": Config.DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # write the dataset to the db
    df.write.jdbc(url=jdbc_url, table="house_sales", mode="overwrite", properties=connection_properties)

    # investigate the data types
    print(df.dtypes)

    # Filter rows where 'price' is NULL and count them
    df = df.withColumn("price", col("price").cast(IntegerType()))
    df = df.filter(df["price"].isNotNull())
    # null_price_count_after = df.filter(df["price"].isNull()).count()  # previous 67020
    # print(f"Number of rows with NULL price after processing: {null_price_count_after}")

    # convert strings to relevant data types
    df = df.withColumn("date_sold", to_date(col("date_sold"), "yyyy/MM/dd"))
    null_date_count = df.filter(col("date_sold").isNull()).count()
    print(f"Number of rows with 'null' for date_sold: {null_date_count}")

    df = df.withColumn("bedrooms", col("bedrooms").cast(IntegerType()))

    # bedroom investigation
    null_bedroom_count = df.filter(col("bedrooms") == 0).count()
    print(f"Number of rows with '0' for bedroom: {null_bedroom_count}")  # 729 with zero bedrooms
    df = df.filter(df["bedrooms"] != 0)

    # state investigation
    states_incorrectly_formatted = df.filter(length("state") != 3)
    states_incorrectly_formatted.show()

    # convert co-ords to numbers
    df = df.withColumn("lat", col("lat").cast("float"))
    df = df.withColumn("lon", col("lon").cast("float"))

    # drop duplicate values
    df = df.dropDuplicates()

    # Inpect the data
    # df.explain()
    df.describe().show(100)

    spark.stop()


if __name__ == "__main__":
    main()

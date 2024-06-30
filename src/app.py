from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("Aus Property") \
        .config("spark.master", "local") \
        .getOrCreate()

    # dataset
    csv_file_path = "data/aus-property-sales-sep2018-april2020.csv"

    # create a dataframe from reading the csv
    # use the headers from the csv
    df = spark.read.option("header", "true").csv(csv_file_path)

    # Java Database Connectivity URL for postgres and relating connection intergration
    jdbc_url = "jdbc:postgresql://localhost:5432/aus_property"
    connection_properties = {
        "user": "stacey",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    # write the dataset to the db
    df.write.jdbc(url=jdbc_url, table="house_sales", mode="overwrite", properties=connection_properties)

    spark.stop()


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, length, avg, monotonically_increasing_id
from pyspark.sql.types import IntegerType
from config import Config
from contextlib import contextmanager
import logging


'''
Basic data pipeline to:
1) Ingest data
2) Clean and prepare data
3) Process and analyze data
4) Export data

This script can be automated using a task runner like Airflow to trigger execution based on events or schedules.
Currently, I manually trigger the generation of a JSON file for
my Flask postcode app to display average property prices by state.

The script includes (very basic) error handling for PySpark SQL operations
and uses a context manager to manage the Spark session lifecycle.

The Spark session is currently configured to run locally, utilizing all available logical processors
for parallelizing the workload.

The future aim is to refine this configuration to fully explore how Spark can be optimized
for processing large datasets and utilize cloud resources for scalability and efficiency.
'''


# TASK: investigate pyspark sql error handling
class DataProcessingError(Exception):
    pass


# Context manager for easy build and tear down
@contextmanager
def spark_session_context():
    try:
        spark = SparkSession.builder \
            .appName("Aus Property") \
            .config("spark.master", "local") \
            .getOrCreate()
        spark.conf.set("spark.sql.debug.maxToStringFields", 100)
        logging.info("Spark session successfully started")
        yield spark  # hands over control to the context manager
    except Exception as e:
        logging.error(f"Error starting Spark session: {e}")
        raise DataProcessingError("Failed to start Spark session") from e
        # chain on to keep original traceback
    finally:
        spark.stop()
        logging.info("Spark session successfully stopped")


def main():
    with spark_session_context() as spark:
        # dataset
        csv_file_path = "data/aus-property-sales-sep2018-april2020.csv"

        # create a dataframe from reading the csv
        # use the headers from the csv
        try:
            df = spark.read.option("header", "true").csv(csv_file_path)
        except Exception as e:
            logging.error(f"Error reading the data file: {e}")
            raise DataProcessingError("Failed to read the data file") from e

        # Java Database Connectivity URL for postgres and relating connection intergration
        # Task: Local development only / consider impact of real world configuration
        jdbc_url = "jdbc:postgresql://localhost:5432/aus_property"
        connection_properties = {
            "user": Config.DB_USERNAME,
            "password": Config.DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # write the dataset via the JDBC driver
        try:
            df.write.jdbc(url=jdbc_url, table="house_sales", mode="overwrite", properties=connection_properties)
        except Exception as e:
            logging.error(f"Error writing to the postgres db: {e}")
            raise DataProcessingError("Failed to write the data file to postgres") from e

        # investigate the data types
        # logging.info(df.dtypes)

        # Inpect the data
        # df.explain()
        # df.describe().show()

        # read from the postgres db via the JDBC driver
        try:
            postgres_db = (spark.read.format("jdbc")
                                .option("url", jdbc_url)
                                .options(**connection_properties)
                                .option("dbtable", "house_sales")
                                .load())
        except Exception as e:
            logging.error(f"Error reading the postgres db: {e}")
            raise DataProcessingError("Failed to read the postgres db") from e

        # Inpect the data
        df.explain()
        df.describe().show()

        cleaned_df = clean_data(postgres_db)

        avg_price_df = calculate_avg_price_by_state_across_time(cleaned_df)

        # creates a column with a unique id starting with 1
        # not guarenteed to be subquential but will be unique and increasing)
        avg_price_df_with_id = avg_price_df.withColumn('id', monotonically_increasing_id() + 1)

        # write the data to the postcheck dbs
        mysql_url = f"jdbc:mysql://localhost:3306/{Config.DB_MYSQL_DBNAME}"
        mysql_properties = {
            "user": Config.DB_MYSQL_USERNAME,
            "password": Config.DB_MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Write the final DataFrame to the MySQL db
        try:
            avg_price_df_with_id.write.jdbc(url=mysql_url, table="property_reporting",
                                            mode="overwrite", properties=mysql_properties)
        except Exception as e:
            logging.error(f"Error writing to the MySQL db: {e}")
            raise Exception("Failed to write the DataFrame to MySQL") from e


def clean_data(df):
    df = df.orderBy("date_sold")

    # Filter rows where 'price' is NULL and count them
    df = df.withColumn("price", col("price").cast(IntegerType()))
    df = df.filter(df["price"].isNotNull())
    # null_price_count_after = df.filter(df["price"].isNull()).count()  # previous 67020
    # logging.info(f"Number of rows with NULL price after processing: {null_price_count_after}")

    # convert strings to relevant data types
    df = df.withColumn("date_sold", to_date(col("date_sold"), "yyyy/MM/dd"))
    null_date_count = df.filter(col("date_sold").isNull()).count()
    logging.info(f"Number of rows with 'null' for date_sold: {null_date_count}")

    df = df.withColumn("bedrooms", col("bedrooms").cast(IntegerType()))

    # bedroom investigation
    null_bedroom_count = df.filter(col("bedrooms") == 0).count()
    logging.info(f"Number of rows with '0' for bedroom: {null_bedroom_count}")  # 729 with zero bedrooms
    df = df.filter(df["bedrooms"] != 0)

    # state investigation
    states_incorrectly_formatted = df.filter(length("state") != 3)
    states_incorrectly_formatted.show()

    # convert co-ords to numbers
    df = df.withColumn("lat", col("lat").cast("float"))
    df = df.withColumn("lon", col("lon").cast("float"))

    # drop duplicate values
    df = df.dropDuplicates()
    return df


def calculate_avg_price_by_state_across_time(df):
    return df.groupBy("state", "date_sold").agg(avg("price").alias("avg_price")).orderBy("state", "date_sold")


if __name__ == "__main__":
    main()

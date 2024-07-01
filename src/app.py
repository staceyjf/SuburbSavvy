from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, length, avg
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
        # Task: Local development only / consider impact when deployed
        jdbc_url = "jdbc:postgresql://localhost:5432/aus_property"
        connection_properties = {
            "user": Config.DB_USERNAME,
            "password": Config.DB_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # write the dataset to the db
        try:
            df.write.jdbc(url=jdbc_url, table="house_sales", mode="overwrite", properties=connection_properties)
        except Exception as e:
            logging.error(f"Error writing to the postgres db: {e}")
            raise DataProcessingError("Failed to write the datafile to postgres") from e

        # investigate the data types
        # logging.info(df.dtypes)

        # Inpect the data
        # df.explain()
        # df.describe().show()

        cleaned_df = clean_data(df)

        avg_price_df = calculate_avg_price_by_state_across_time(cleaned_df)

        # write the data to the postcheck dbs
        mysql_url = f"jdbc:mysql://localhost:3306/{Config.DB_MYSQL_DBNAME}"
        mysql_properties = {
            "user": Config.DB_MYSQL_USERNAME,
            "password": Config.DB_MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Write the final DataFrame to the MySQL db
        try:
            avg_price_df.write.jdbc(url=mysql_url, table="property_reporting",
                                    mode="overwrite", properties=mysql_properties)
        except Exception as e:
            logging.error(f"Error writing to the MySQL db: {e}")
            raise Exception("Failed to write the DataFrame to MySQL") from e

        # # convert the pandas df into a dict for flask
        # dict_avg_price_df = avg_price_df.to_dict(orient='records')

        # # write and return a file to Postcode Flask app's folder
        # # Task: investigate shared storage solution like Azure Blob storage for deployment
        # file_path = '/home/staceyf/projects/PostCheck-API-Flask/app/data/avg_price_by_state.json'

        # with open(file_path, 'w') as file:
        #     try:
        #         json.dump(dict_avg_price_df, file)  # serialised verison of the dic
        #     except Exception as e:
        #         logging.error(f"Error writing pandas output to a new file: {e}")
        #         raise DataProcessingError("Failed to write to a new file") from e

        # return 'avg_price_by_state.json'


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

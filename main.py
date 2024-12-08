from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import geohash2


def main():
    # Initialize Spark session
    spark_session = SparkSession.builder \
        .appName("Weather and Restaurant Data Merge") \
        .getOrCreate()

    # Load datasets
    restaurant_data = spark_session.read.csv('restaurant_csv/*.csv', header=True, inferSchema=True)
    weather_data = spark_session.read.parquet('weather_parq/*.parquet')

    # Rename latitude and longitude columns to avoid conflicts
    restaurant_data = restaurant_data.withColumnRenamed('lat', 'lat_rest') \
                                    .withColumnRenamed('lng', 'lng_rest')

    weather_data = weather_data.withColumnRenamed('lat', 'lat_wtr') \
                            .withColumnRenamed('lng', 'lng_wtr')

    # Function to generate a 4 char geohash
    def compute_geohash(latitude, longitude):
        if latitude is not None and longitude is not None:
            return geohash2.encode(latitude, longitude)[:4]
        return None

    # Register UDF to apply geohash encoding
    geohash_udf = udf(lambda lat, lon: compute_geohash(lat, lon), StringType())

    # Add geohash column to dataframes
    weather_data = weather_data.withColumn(
        "geohash", geohash_udf(col("lat_wtr").cast("double"), col("lng_wtr").cast("double"))
    )
    restaurant_data = restaurant_data.withColumn(
        "geohash", geohash_udf(col("lat_rest").cast("double"), col("lng_rest").cast("double"))
    )

    # Left join restaurants to weather on geohash and drop duplicates
    merged_data = restaurant_data.join(weather_data, on="geohash", how="left")
    merged_data = merged_data.dropDuplicates()

    # Save the merged dataframe to Parquet, partitioned by geohash
    output_dir = 'output_data/merged_weather_restaurant'
    merged_data.write.mode("overwrite").partitionBy("geohash").parquet(output_dir)

    # Stop the Spark session
    spark_session.stop()


if __name__ == "__main__":
    main()

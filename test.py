import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import geohash2

class TestSparkJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("TestJoinWeatherRestaurant") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_join_logic(self):
        # Create mock restaurant data
        restaurant_data = [
            {"restaurant_id": 1, "restaurant_lat": 37.7749, "restaurant_lng": -122.4194},
            {"restaurant_id": 2, "restaurant_lat": 34.0522, "restaurant_lng": -118.2437},
        ]
        restaurant_df = self.spark.createDataFrame(restaurant_data)

        # Create mock weather data
        weather_data = [
            {"weather_id": 101, "weather_lat": 37.7749, "weather_lng": -122.4194, "temperature": 20},
            {"weather_id": 102, "weather_lat": 34.0522, "weather_lng": -118.2437, "temperature": 25},
        ]
        weather_df = self.spark.createDataFrame(weather_data)

        # Add geohash columns
        def generate_geohash(lat, lon):
            return geohash2.encode(lat, lon)[:4]

        geohash_udf = self.spark.udf.register("geohash_udf", generate_geohash)

        restaurant_df = restaurant_df.withColumn(
            "geohash", geohash_udf(col("restaurant_lat"), col("restaurant_lng"))
        )
        weather_df = weather_df.withColumn(
            "geohash", geohash_udf(col("weather_lat"), col("weather_lng"))
        )

        # Perform the join
        df_joined = restaurant_df.join(weather_df, on="geohash", how="left")

        # Verify the join result
        joined_data = df_joined.collect()
        self.assertEqual(len(joined_data), 2)  # Ensure we have 2 records
        self.assertEqual(joined_data[0]["temperature"], 20)  # Verify temperature for first restaurant
        self.assertEqual(joined_data[1]["temperature"], 25)  # Verify temperature for second restaurant


if __name__ == "__main__":
    unittest.main()

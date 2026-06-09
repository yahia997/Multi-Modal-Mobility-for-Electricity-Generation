from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt
from schema import *
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel
import gc

class Load_NetMob:
  def __init__(self, datasets=[1], modes=['WALKING']):
    """
      datasets: (list)
        - 1 => + trips
        - 2 => + with individuals
        - 3 => + weather
      
        modes: 
        (list) that contains the main modes that we will filter on
    """
    self.spark = self.init_spark()
    self.datasets = datasets
    self.modes = modes
    self.data = None

    self.merge_all()

  # init spark session----------------------------------------------------------------------------------
  def init_spark(self):
    print("initializing spark session ...")

    return SparkSession.builder \
    .appName("load dataset") \
    .master("local[4]") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.driver.memory", "8g")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
  
  # collects all mergings  ----------------------------------------------------------------------------------
  def merge_all(self):
    print('merging datasets ...')
    self.datasets = set(self.datasets)

    # base dataset (GPS) all will be merged with it
    self.data = self.spark\
      .read\
      .option('header', True)\
      .schema(gps_schema)\
      .csv('../data/gps_road_merged_v2.csv')

    if 1 in self.datasets:
      self.merge_trips()
    
    if 2 in self.datasets:
      self.merge_individuals()

    if 3 in self.datasets:
      self.merge_weather()

    # remove inaccurate gps points
    self.data = self.data.filter(~(col('VALID').isin('NO FIX','Estimated (dead reckoning)')))

    # remove gps points far from roads
    self.data = self.data.filter(col('distance_to_road') <= 10)

    # Clear all cached data
    self.spark.catalog.clearCache()

    self.data.persist(StorageLevel.MEMORY_AND_DISK)

    gc.collect()

  # merge rips (1)  ----------------------------------------------------------------------------------
  def merge_trips(self):
    print("merging with trips ...")

    trip_df = self.spark\
      .read\
      .option('header', True)\
      .schema(trips_schema)\
      .csv('../data/final_dataset_to_use_ya.csv')
    
    # Trip start time stamp
    trip_df = trip_df.withColumn('Trip_start', to_timestamp(concat(trip_df.Date_EMG,lit(' '), trip_df.Time_O), 'yyyy-MM-dd HH:mm:ss'))

    # Trip end time stamp
    trip_df = trip_df.withColumn('Trip_end', to_timestamp(concat(trip_df.Date_EMG,lit(' '), trip_df.Time_D), 'yyyy-MM-dd HH:mm:ss'))

    # get only wanted Main_Mode (can be customized in parameters)
    trip_filtered = trip_df.where(col('Main_Mode').isin(self.modes))

    # join trips with gps points
    # each point is mapped to single trip
    self.data = self.data.join(broadcast(trip_filtered), 
                        (self.data['ID'] == trip_filtered['ID']) & 
                       (self.data['Local DATETIME'] <= trip_filtered['Trip_end']) &
                       (self.data['Local DATETIME'] >= trip_filtered['Trip_start'])
                      ).drop(trip_filtered['ID'])

  # merge individuals (2)  ----------------------------------------------------------------------------------
  def merge_individuals(self):
    print("merging with individuals ...")

    individuals_df = self.spark\
      .read\
      .option('header', True)\
      .schema(individuals_schema)\
      .csv(f'../data/finalCleanedIndiv.csv')\
      .select(
        'NB_CAR',
        'ID',
        'WEIGHT_INDIV',
        'AGE',
        'PRO_CAT',
        'NBPERS_HOUSE',
        'BIKE',
        'ELECT_SCOOTER',
        'TWO_WHEELER',
      )
    
    self.data = self.data.join(broadcast(individuals_df), on='ID')
    
  # merge weather (3) ----------------------------------------------------------------------------------
  def merge_weather(self):
    def haversine(lon1, lat1, lon2, lat2):
      """
      Calculate the great circle distance in kilometers between two points 
      on the earth (specified in decimal degrees)
      """
      # convert decimal degrees to radians 
      lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
      # haversine formula 
      dlon = lon2 - lon1 
      dlat = lat2 - lat1 
      a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
      c = 2 * asin(sqrt(a)) 
      r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
      return c * r

    # go through the six stations and find the nereast one for each GPS point
    def nereast_station(lon, lat):
      weather_stations = {
          "LFPN": {"latitude": 48.7519, "longitude": 2.1062},
          "LFPO": {"latitude": 48.7233, "longitude": 2.3794},
          "LFPM": {"latitude": 48.6047, "longitude": 2.67},
          "LFPG": {"latitude": 49.0097, "longitude": 2.5479},
          "LFPB": {"latitude": 48.9694, "longitude": 2.4414},
          "LFPT": {"latitude": 49.0967, "longitude": 2.0408},
      }
      nereast = ""
      dist = 1000000
      for key in weather_stations.keys():
          curr_dist = haversine(lon, lat, weather_stations[key]['longitude'], weather_stations[key]['latitude'])
          if curr_dist < dist:
              nereast = key
              dist = curr_dist
      return nereast
    

    print("merging with weather ...")

    # load cleaned weather dataset
    weather_df = self.spark\
      .read\
      .option('header', True)\
      .schema(weather_schema)\
      .csv('../data/idf_weather_hourly_cleaned.csv')\
      .select(
        'time_utc',
        'icao',
        'latitude',
        'longitude',
        'temp',
        'rhum',
        'prcp'
      )
    
    # convert to hour format 
    self.data = self.data.withColumn(
      "UTC_DATETIME", 
      date_format(col("UTC DATETIME"), 'yyyy-MM-dd HH')
    )

    # round format to hours to merge with
    weather_df = weather_df.withColumn(
      'time_utc',
      date_format(col('time_utc'), 'yyyy-MM-dd HH')
    )

    # Register UDF to compute the Haversine distance
    nereast_station_udf = udf(nereast_station, StringType())

    # apply the function to create column "station" that contains the nereast weather station
    self.data = self.data.withColumn(
      "station",
      nereast_station_udf('LONGITUDE', 'LATITUDE')
    )

    # join on the same time and the nereast station
    self.data = self.data.join(broadcast(weather_df),
                  (self.data['UTC_DATETIME'] == weather_df['time_utc']) &
                  (weather_df['icao'] == self.data['station'])
                  )\
                  .drop('station', 'UTC_DATETIME', weather_df['latitude'], weather_df['longitude'])

  # Join with nearest road and remove blurring (Private method)
  def remove_blurring(self):
    
    print(f"Remove blurring effect ...")

    # add rank column to partition and order the data
    data_with_ranks = self.data.withColumn(
      'rank_col', 
      row_number().over(Window.partitionBy('KEY').orderBy('LOCAL DATETIME'))
    )

    # self join to calculate the distance between consecutive rows
    data_with_distances = data_with_ranks.alias('t1').join(
      data_with_ranks.alias('t2'),
      (col('t1.KEY') == col('t2.KEY')) & (col('t1.rank_col') == col('t2.rank_col') + 1),
      'left'
    ).select(
        col('t1.*'),
        when(col('t2.LATITUDE').isNotNull() & col('t2.LONGITUDE').isNotNull(),
             expr("""
                 6371 * 2 * ASIN(SQRT(
                     POW(SIN(RADIANS(t2.LATITUDE - t1.LATITUDE) / 2), 2) +
                     COS(RADIANS(t1.LATITUDE)) * COS(RADIANS(t2.LATITUDE)) *
                     POW(SIN(RADIANS(t2.LONGITUDE - t1.LONGITUDE) / 2), 2)
                 ))
             """))
        .otherwise(lit(0.0)).alias('distance_km')
    )

    # Step 3: Calculate cumulative distance using window function
    window_spec_cumulative = Window.partitionBy('KEY').orderBy('rank_col')
    data_with_cumulative_distance = data_with_distances.withColumn(
      'cumulative_distance', 
      sum('distance_km').over(window_spec_cumulative.rowsBetween(Window.unboundedPreceding, 0))
    )

    # calculate total distance per KEY
    window_spec_total = Window.partitionBy('KEY')
    data_with_total_distance = data_with_cumulative_distance.withColumn(
      'total_distance', 
      max('cumulative_distance').over(window_spec_total)
    )
    
    # filter data and drop unnecessary columns
    self.data = data_with_total_distance.filter(
      (col('cumulative_distance') > 0) & 
      (col('cumulative_distance') < col('total_distance'))
    ).drop(
      'rank_col', 'distance_km', 'cumulative_distance', 'total_distance'
    )

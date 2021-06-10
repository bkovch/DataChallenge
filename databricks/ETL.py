# Databricks notebook source
# MAGIC %md # Read CSV Files

# COMMAND ----------

def csv_to_df(file_location, file_type):
  # Provide parameters:https://community.cloud.databricks.com/?o=882228783267793#
  #   file location like "/FileStore/tables/phData_challenge/airlines.csv"
  #   file type like "csv"

  # CSV options
  infer_schema = "false"
  first_row_is_header = "true"
  delimiter = ","

  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
  
  return df

df_airlines = csv_to_df("/FileStore/tables/phData_challenge/airlines.csv", "csv")
df_airports = csv_to_df("/FileStore/tables/phData_challenge/airports.csv", "csv")
df_flights = csv_to_df("/FileStore/tables/phData_challenge/flights/", "csv")

# COMMAND ----------

# MAGIC %md # Format Data

# COMMAND ----------

airlines_column_types = {
  'iata_code': 'varchar(2)',
  'airline': 'varchar(50)'  
}

airports_column_types = {
  'iata_code': 'varchar(3)',
  'airport': 'varchar(100)',
  'city': 'varchar(50)',
  'state': 'varchar(2)',
  'country': 'varchar(3)',
  'latitude': 'decimal(9,6)',
  'longitude': 'decimal(9,6)'
}

flights_column_types = {
  'year': 'decimal(4, 0)', 
  'month': 'decimal(4, 0)', 
  'day': 'decimal(4, 0)', 
  'day_of_week': 'decimal(1, 0)', 
  'airline': 'varchar(2)', 
  'flight_number': 'varchar(4)', 
  'tail_number': 'varchar(6)', 
  'origin_airport': 'varchar(3)', 
  'destination_airport': 'varchar(3)', 
  'scheduled_departure': 'varchar(4)', 
  'departure_time': 'varchar(4)', 
  'departure_delay': 'decimal(4, 0)', 
  'taxi_out': 'decimal(2, 0)', 
  'wheels_off': 'varchar(4)', 
  'scheduled_time': 'decimal(2, 0)', 
  'elapsed_time': 'decimal(2, 0)', 
  'air_time': 'decimal(2, 0)', 
  'distance': 'decimal(3, 0)', 
  'wheels_on': 'decimal(4, 0)', 
  'taxi_in': 'decimal(2, 0)', 
  'scheduled_arrival': 'decimal(4, 0)', 
  'arrival_time': 'varchar(4)', 
  'arrival_delay': 'decimal(4, 0)', # spec defines as string
  'diverted': 'boolean', # spec defines as number
  'cancelled': 'boolean', # spec defines as number
  'cancellation_reason': 'varchar(1)', 
  'air_system_delay': 'decimal(4, 0)', 
  'security_delay': 'decimal(4, 0)', 
  'airline_delay': 'decimal(4, 0)', 
  'late_aircraft_delay': 'decimal(4, 0)', 
  'weather_delay': 'decimal(4, 0)'
}

def change_column_types(df, column_types):
  for column_name, new_type in column_types.items():
    df = df.withColumn(column_name, df[column_name].cast(new_type))
  return df

df_airlines = change_column_types(df_airlines, airlines_column_types)
df_airports = change_column_types(df_airports, airports_column_types)
df_flights = change_column_types(df_flights, flights_column_types)

df_airlines.printSchema()
df_airports.printSchema()
df_flights.printSchema()  

# COMMAND ----------

# MAGIC %md # Write to Data Warehouse

# COMMAND ----------

# Databricks Community edition doesn't support secrets, so credentials are stored in a JSON file
# Confirmed with Databricks tech support Marcus de Varona on 6/4/2021
secret = spark.read.json("/FileStore/tables/phData_challenge/secret.json", multiLine=True).first().asDict()

# Snowflake connection options
options = {
  "sfUrl": secret['url'],
  "sfUser": secret['username'],
  "sfPassword": secret['password'],
  "sfDatabase": secret['database'],
  "sfSchema": secret['schema'],
  "sfWarehouse": secret['warehouse']
}

# COMMAND ----------

# Write data frame to data warehouse
def df_to_wh(df, db_table):
  df.write \
      .format("snowflake") \
      .options(**options) \
      .option("dbtable", db_table) \
      .mode("overwrite") \
      .save()
  
df_to_wh(df_airlines, 'AIRLINES');
df_to_wh(df_airports, 'AIRPORTS');
df_to_wh(df_flights, 'FLIGHTS');  

# COMMAND ----------

# MAGIC %md # View Loaded Data

# COMMAND ----------

# View data loaded into warehouse
def view_data(db_table): 
  df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("dbtable", db_table) \
    .load()
  display(df.take(1000))
  
view_data('AIRLINES')
view_data('AIRPORTS')
view_data('FLIGHTS')

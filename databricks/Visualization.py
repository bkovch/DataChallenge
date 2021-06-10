# Databricks notebook source
# DBTITLE 1,Visualization
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

# Query report views
def execute_query(query): 
  df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query", query) \
    .load()
  return df

# COMMAND ----------

# MAGIC %md # Flights by Month

# COMMAND ----------

# DBTITLE 0,Airline Flights by Month
df = execute_query('''
  select
      airline_name as airlines,
      month as month,
      sum(flights_from_airport) as num_flights
  from fligths_by_airline_airport_month
  group by airline_name, month
''')
display(df)

# COMMAND ----------

# MAGIC %md # On Time vs. Late Flights

# COMMAND ----------

# DBTITLE 0,On Time vs. Late Flights
df = execute_query('''
  select
    airline as airlines,
    on_time_flights as on_time,
    total_flights - on_time_flights as late
  from on_time_pct_by_airline_2015
  order by total_flights desc
''')
display(df)

# COMMAND ----------

df = execute_query('''
  select
    airline as airlines,
    on_time_pct as on_time,
    100 - on_time_pct as late
  from on_time_pct_by_airline_2015
  order by on_time_pct
''')
display(df)

# COMMAND ----------

# MAGIC %md # Departure & Arrival Delays

# COMMAND ----------

# DBTITLE 0,Departure and Arrival Delays
df = execute_query('''
  select
      airline_name as airlines,
      departure_delays as departure,
      arrival_delays as arrival,
      total_delays as total
  from delays_by_airline
  order by total_delays desc
''')
display(df)

# COMMAND ----------

# MAGIC %md # Cancellation Reasons

# COMMAND ----------

# DBTITLE 0,Cancellation Reasons by Airport (Top 10 Airports)
df = execute_query('''
  select top 10
    airport_code as airports,
    weather,
    airline_carrier,
    national_air_system,
    security
  from cancellation_reasons_by_airport_pivoted
  order by total desc
''')
display(df)

# COMMAND ----------

# MAGIC %md # Delay Reasons

# COMMAND ----------

df = execute_query('''
  select top 10
    airport_code as airports,
    air_system_delays,
    security_delays,
    airline_delays,
    late_aircraft_delays,
    weather_delays
  from delay_reasons_by_airport
  order by 
    air_system_delays +
    security_delays +
    airline_delays +
    late_aircraft_delays +
    weather_delays
    desc
''')
display(df)

# COMMAND ----------

# MAGIC %md # Most Unique Routes

# COMMAND ----------

df = execute_query('''
  select *
  from most_unique_routes_airlines
  order by unique_routes desc;
''')
display(df)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp
import requests
import sqlite3
from datetime import datetime


# Coordenadas de São Paulo
lat = -23.5505
lon = -46.6333

url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}"
headers = {
    "User-Agent": "anon-script"
}


resposta = requests.get(url, headers=headers)

if resposta.status_code == 200:
    dados = resposta.json()
    tempo = dados['properties']['timeseries'][0]['data']['instant']['details']
    
  
    temperatura = tempo['air_temperature']
    umidade = tempo.get('relative_humidity', None)
    pressao = tempo['air_pressure_at_sea_level']
    data_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

rows = []
for item in dados['properties']["timeseries"]:
    time = item["time"]
    details = item["data"]["instant"]["details"]
    row = {"time": time, **details}
    rows.append(row)

df = spark.createDataFrame(rows)
df = df.withColumn("time", col("time").cast(TimestampType()))
df = df.withColumn("data_processamento", current_timestamp())


# Coordenadas de São Paulo
lat = -23.5505
lon = -46.6333

url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}"
headers = {
    "User-Agent": "anon-script"
}


resposta = requests.get(url, headers=headers)

if resposta.status_code == 200:
    dados = resposta.json()
    tempo = dados['properties']['timeseries'][0]['data']['instant']['details']
    
  
    temperatura = tempo['air_temperature']
    umidade = tempo.get('relative_humidity', None)
    pressao = tempo['air_pressure_at_sea_level']
    data_coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df.write.mode("append").saveAsTable("clima.bronze.weather")

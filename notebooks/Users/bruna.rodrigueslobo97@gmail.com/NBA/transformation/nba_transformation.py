# Databricks notebook source
#dbtitle 1, Bibiotecas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

dbutils.fs.ls("/mnt/devnbadl/pre-landing-zone")

# COMMAND ----------

#DBTITLE, Varáveis
# Variáveis com os caminhos dos raw dados
plz_path_player_data = "/mnt/devnbadl/pre-landing-zone/player_data"
plz_path_players = "/mnt/devnbadl/pre-landing-zone/players"
plz_path_season_stats = "/mnt/devnbadl/pre-landing-zone/season_stats"

# COMMAND ----------

#DBTITLE, Variáveis de hora/tempo
# Data atual
current_date = datetime.now()

# Data de 1 mês atrás
last_month = current_date - relativedelta(months=1)
last_month = last_month.strftime('%Y-%m-%d')

# Data de 2 meses atrás
two_months_ago = current_date - relativedelta(months=2)
two_months_ago = two_months_ago.strftime('%Y-%m-%d')

# Data de 3 meses atrás
three_months_ago = current_date - relativedelta(months=3)
three_months_ago = three_months_ago.strftime('%Y-%m-%d')

# COMMAND ----------

#DBTITLE, Leitura de path com var
df_player_data = spark.read.csv(plz_path_player_data, sep=r',', header=True)
df_players = spark.read.csv(plz_path_players, sep=r',', header=True)
df_season_stats = spark.read.csv(plz_path_season_stats, sep=r',', header=True)

# COMMAND ----------

#DBTITLE, Analisando dados da "player_data"
df_player_data.display()

# COMMAND ----------

#DBTITLE, Analisando dados da "players"
df_players.display()

# COMMAND ----------

#DBTITLE, Analisando dados da "season_stats"
df_season_stats.display()

# COMMAND ----------


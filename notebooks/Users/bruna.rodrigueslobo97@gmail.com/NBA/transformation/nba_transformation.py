# Databricks notebook source
#dbtitle 1, Bibiotecas
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace, col, udf, from_unixtime, unix_timestamp, date_format

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

# Data de ontem
yesterday = current_date - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')

# COMMAND ----------

#DBTITLE, Leitura de path com var
df_player_data = spark.read.csv(plz_path_player_data, sep=r',', header=True)
df_players = spark.read.csv(plz_path_players, sep=r',', header=True)
df_season_stats = spark.read.csv(plz_path_season_stats, sep=r',', header=True)

# COMMAND ----------

#DBTITLE, Limpando os nomes
df_player_data = df_player_data.withColumn("name", regexp_replace(col("name"), "\\*$", ""))
df_players = df_players.withColumn("Player", regexp_replace(col("Player"), "\\*$", ""))
df_season_stats = df_season_stats.withColumn("Player", regexp_replace(col("Player"), "\\*$", ""))

# COMMAND ----------

#DBTITLE, Limpando nomes de colunas da season_stats que contém "%"
for col_name in df_season_stats.columns:
    if '%' in col_name:
        new_col_name = col_name.replace('%', '_pct')
        df_season_stats = df_season_stats.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

#DBTITLE, Arrumando o campo birth_date da df_player_data
def correct_date(date_str):

    # Se for nulo, retorna nada
    if date_str is None:
        return date_str
    
    # Split as datas em componentes
    components = date_str.split(' ')
    month = components[0]
    day = components[1].replace(',', '')
    year = components[2]
    
    # Se o dia for apenas um digito, colocar 0 na frente
    if len(day) == 1:
        day = '0' + day
    
    # Se o ano for 3 digitos, colocar o 0 no final
    if len(year) == 3:
        year = year + '0'
    
    return f"{month} {day}, {year}"

# Registra UDF no Spark
correct_date_udf = udf(correct_date, StringType())

df_player_data = df_player_data.withColumn("birth_date", correct_date_udf(col("birth_date")))

# COMMAND ----------

# Criar a coluna birth_date_key se precisarmos da data em formato "yyyy-MM-dd"
df_player_data = df_player_data.withColumn(
    "birth_date_key",
    date_format(
        from_unixtime(unix_timestamp("birth_date", "MMMM dd, yyyy")),
        "yyyy-MM-dd"
    )
)

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

#DBTITLE, Criando temp views
df_player_data.createOrReplaceTempView('player_data')
df_players.createOrReplaceTempView('players')
df_season_stats.createOrReplaceTempView('season_stats')

# COMMAND ----------

#DBTITLE, Criando métricas de porcentagem e somas
spark.sql('''
            SELECT DISTINCT
                Player AS name,
                SUM(FG) AS fg_total,
                SUM(FGA) AS fga_total,
                ROUND(AVG(FG_pct), 3) AS fg_percent,
                SUM(3P) AS 3p_total,
                SUM(3PA) AS 3ap_total,
                ROUND(AVG(3P_pct), 3) AS 3p_percent,
                SUM(FT) AS ft_total,
                SUM(FTA) AS fta_total,
                ROUND(AVG(FT_pct), 3) AS ft_percent,
                SUM(ORB) AS orb_total,
                SUM(DRB) AS drb_total,
                SUM(AST) AS ast_total,
                SUM(STL) AS stl_total,
                SUM(BLK) AS blk_total,
                SUM(TOV) AS tov_total,
                SUM(PF) AS pf_total,
                SUM(PTS) AS pts_total
            FROM
                season_stats
            GROUP BY
                1
                ''').createOrReplaceTempView('metrics_alltime')

# COMMAND ----------

#DBTITLE, Criando DF player_data
player_data = spark.sql('''
                        SELECT DISTINCT
                            player_data.name,
                            player_data.position,
                            player_data.height,
                            player_data.weight,
                            players.birth_city,
                            players.birth_state,
                            player_data.college,
                            metrics.fg_total,
                            metrics.fga_total,
                            metrics.fg_percent,
                            metrics.3p_total,
                            metrics.3ap_total,
                            metrics.3p_percent,
                            metrics.ft_total,
                            metrics.fta_total,
                            metrics.ft_percent,
                            metrics.orb_total,
                            metrics.drb_total,
                            metrics.ast_total,
                            metrics.stl_total,
                            metrics.blk_total,
                            metrics.tov_total,
                            metrics.pf_total,
                            metrics.pts_total,
                            player_data.year_start,
                            player_data.year_end,
                            player_data.year_end - player_data.year_start AS years_played,
                            player_data.birth_date,
                            player_data.birth_date_key
                        FROM
                            player_data

                        LEFT JOIN
                            players
                        ON
                            TRIM(player_data.name) = TRIM(players.Player)

                         LEFT JOIN
                            metrics_alltime AS metrics
                        ON
                            TRIM(player_data.name) = TRIM(metrics.name)
                        ''')

# COMMAND ----------

player_data.display()

# COMMAND ----------



# COMMAND ----------

player_data.filter(player_data.name == "Stephen Curry").display()

# COMMAND ----------

#DBTITLE, Opção em PySpark
# Calculando as métricas
metrics_alltime_pyspark = df_season_stats.groupBy("Player").agg(
    F.sum("FG").alias("fg_total"),
    F.sum("FGA").alias("fga_total"),
    F.round(F.avg("FG_pct"), 3).alias("fg_percent"),
    F.sum("3P").alias("3p_total"),
    F.sum("3PA").alias("3ap_total"),
    F.round(F.avg("3P_pct"), 3).alias("3p_percent"),
    F.sum("FT").alias("ft_total"),
    F.sum("FTA").alias("fta_total"),
    F.round(F.avg("FT_pct"), 3).alias("ft_percent"),
    F.sum("ORB").alias("orb_total"),
    F.sum("DRB").alias("drb_total"),
    F.sum("AST").alias("ast_total"),
    F.sum("STL").alias("stl_total"),
    F.sum("BLK").alias("blk_total"),
    F.sum("TOV").alias("tov_total"),
    F.sum("PF").alias("pf_total"),
    F.sum("PTS").alias("pts_total")
)


player_data_pyspark = (df_player_data
               .join(df_players, F.trim(df_player_data["name"]) == F.trim(df_players["Player"]), "left_outer")
               .join(metrics_alltime_pyspark, F.trim(df_player_data["name"]) == F.trim(metrics_alltime_pyspark["Player"]), "left_outer")
               .select(
                   df_player_data["name"],
                   df_player_data["position"],
                   df_player_data["height"], 
                   df_player_data["weight"],
                   "birth_city",
                   "birth_state",
                   df_player_data["college"],
                   "fg_total",
                   "fga_total",
                   "fg_percent",
                   "3p_total",
                   "3ap_total",
                   "3p_percent",
                   "ft_total",
                   "fta_total",
                   "ft_percent",
                   "orb_total",
                   "drb_total",
                   "ast_total",
                   "stl_total",
                   "blk_total",
                   "tov_total",
                   "pf_total",
                   "pts_total",
                   df_player_data["year_start"],
                   df_player_data["year_end"],
                   (df_player_data["year_end"] - df_player_data["year_start"]).alias("years_played"),
                   df_player_data["birth_date"],
                   df_player_data["birth_date_key"]
               ).distinct())

# COMMAND ----------

#DBTITLE, Arrumando e limpando a df_season_stats
season_stats = df_season_stats.drop(
    'GS',
    'MP',
    'PER',
    'TS_pct',
    '3par',
    'ftr',
    'orb_pct',
    'drb_pct',
    'trb_pct',
    'ast_pct',
    'stl_pct',
    'blk_pct',
    'to_pct',
    'usg_pct',
    'blanl',
    'ows',
    'dws',
    'ws',
    'ws/48',
    'blank2',
    'obpm',
    'dbpm',
    'bpm',
    'vorp'
    )

season_stats = (season_stats
                   .withColumnRenamed('_c0', 'COD')
                   .withColumnRenamed('Year', 'YEAR')
                   .withColumnRenamed('Player', 'NAME')
                   .withColumnRenamed('Pos', 'POS')
                   .withColumnRenamed('Age', 'AGE')
                   .withColumnRenamed('Tm', 'TEAM')
                   )

# COMMAND ----------

season_stats.display()

# COMMAND ----------

#DBTITLE, Escrevendo na consume zone
player_data.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/devnbadl/consume-zone/player_data")
season_stats.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("/mnt/devnbadl/consume-zone/season_stats")

# COMMAND ----------

#DBTITLE, Testando leitura direto da consume zone
cz_path_player_data = "/mnt/devnbadl/consume-zone/player_data"
cz_path_season_stats = "/mnt/devnbadl/consume-zone/season_stats"

# COMMAND ----------

#DBTITLE, Leitura de path com var
cz_player_data = spark.read.csv(cz_path_player_data, sep=r',', header=True)
cz_season_stats = spark.read.csv(cz_path_season_stats, sep=r',', header=True)

# COMMAND ----------

cz_player_data.display()

# COMMAND ----------

cz_season_stats.display()

# COMMAND ----------


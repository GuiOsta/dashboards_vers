# Databricks notebook source
# MAGIC %md
# MAGIC # FS | Indicadores Fechamento Semanal Card OPS
# MAGIC
# MAGIC **Data de Criação:** 29-08-2024
# MAGIC
# MAGIC **Última Atualização:** 27-06-2025
# MAGIC
# MAGIC ### ➡️ [Documentação](https://sites.google.com/picpay.com/fmp-automcao-controle/documenta%C3%A7%C3%B5es/materiais-gerenciais/fechamento-semanal?authuser=0) 
# MAGIC
# MAGIC ### ➡️ [Planilha do Fechamento](https://docs.google.com/spreadsheets/d/1g_zmZu4WNWJiI3Zuyo1tO6OJpgt8uTQlmFm6cZDm9Og) 
# MAGIC
# MAGIC ### ➡️ [Apresentaçaõ do Fechamento](https://docs.google.com/presentation/d/1KQvnqdw8O-vcOvOH8sHMV9RNU0Pwlx5ZZYuLXIE7I7E) 

# COMMAND ----------

# DBTITLE 1,Verifica a atualização das tabelas
# MAGIC %sql
# MAGIC refresh table self_service_analytics.cards_all_contacts_classifications;
# MAGIC refresh table self_service_analytics.all_contacts_classifications;
# MAGIC refresh table self_service_analytics.cards_npsa_answers;
# MAGIC refresh table self_service_analytics.card_mat;
# MAGIC refresh table self_service_analytics.consumer_card_product;
# MAGIC refresh table self_service_analytics.star_schema_ppcard_dim_account_base_history;
# MAGIC
# MAGIC create or replace temp view datas_att as
# MAGIC
# MAGIC with
# MAGIC mx_allc as (
# MAGIC   select count(distinct plataform_id) as daily_sum, created_date from self_service_analytics.all_contacts_classifications where created_date > current_date - 10 group by 2 having daily_sum > 15000
# MAGIC ),
# MAGIC mx_cacc as (
# MAGIC   select count(distinct plataform_id) as daily_sum, created_date from self_service_analytics.cards_all_contacts_classifications where created_date > current_date - 10 group by 2 having daily_sum > 5000
# MAGIC ),
# MAGIC mx_cacc_p as (
# MAGIC   select
# MAGIC     created_date,
# MAGIC     verifier_sum/daily_sum as part
# MAGIC   from
# MAGIC     mx_cacc
# MAGIC     left join
# MAGIC       (select count(distinct plataform_id) as verifier_sum, created_date, case when product = 'Sem identificação' then 'S/ ID' else 'Resto' end as verifier from self_service_analytics.cards_all_contacts_classifications group by all)
# MAGIC       using (created_date)
# MAGIC   where verifier = 'S/ ID'
# MAGIC   group by all
# MAGIC   having part < 0.1
# MAGIC ),
# MAGIC ref_dt as (
# MAGIC   select '3. NPS' as alias, 'self_service_analytics.cards_npsa_answers' as name, max(answer_date) as ref_date from self_service_analytics.cards_npsa_answers
# MAGIC   union all
# MAGIC   select '1. CACC (Geral)' as alias, 'self_service_analytics.cards_all_contacts_classifications' as name, max(created_date) as ref_date from mx_cacc
# MAGIC   union all
# MAGIC   select '2. MAT' as alias, 'self_service_analytics.card_mat' as name, max(date(execution_at)) as ref_date from self_service_analytics.card_mat
# MAGIC   union all
# MAGIC   select '0a. CCCP ☭' as alias, 'self_service_analytics.consumer_card_product' as name, max(reference_date) as ref_date from self_service_analytics.consumer_card_product
# MAGIC   union all
# MAGIC   select '0. Star Schema' as alias, 'self_service_analytics.star_schema_ppcard_dim_account_base_history' as name, max(reference_date) as ref_date from self_service_analytics.star_schema_ppcard_dim_account_base_history
# MAGIC   union all
# MAGIC   select '0. All Contacts CX' as alias, 'self_service_analytics.all_contacts_classifications' as name, max(created_date) as ref_date from mx_allc
# MAGIC   union all
# MAGIC   select '1. CACC (Produtos)' as alias, 'self_service_analytics.cards_all_contacts_classifications' as name, max(created_date) as ref_date from mx_cacc_p
# MAGIC )
# MAGIC
# MAGIC select *, if(ref_date < getArgument('data_atual'), '🔴', '🟢') as ready_to_use from ref_dt order by 1;
# MAGIC
# MAGIC select * from datas_att

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

dbutils.widgets.text("data_atual", "", "Data máxima a ser filtrada")
dbutils.widgets.dropdown("forcar_data", "False", ["True", "False"], "Forçar o uso dessa data?")
dbutils.widgets.text("dias_top", "", "Dias para montar o top")

data_atual_wg = dbutils.widgets.get("data_atual")
forcar_data = dbutils.widgets.get("forcar_data")
dias_top = dbutils.widgets.get("dias_top")

# COMMAND ----------

# DBTITLE 1,Bibliotecas
import sys
import subprocess
import pkg_resources
required = {'pygsheets', 'pyxlsb', 'openpyxl', 'fsspec', 's3fs'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
  python = sys.executable
  subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

from pyspark.sql import Window
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType

import pyspark.sql.functions as F

import pygsheets
import pandas as pd
import pyxlsb # xlsb format
import openpyxl # xlsx format
import datetime
import boto3

# COMMAND ----------

# DBTITLE 1,Funções
# Export de dados para o GSheets
client = pygsheets.authorize(service_file="/Workspace/Users/william.soares@picpay.com/upload/card_e_seguros_cesops.json")
url = "https://docs.google.com/spreadsheets/d/1g_zmZu4WNWJiI3Zuyo1tO6OJpgt8uTQlmFm6cZDm9Og"

def export_google_sheets(url, df_spark, sheet_name, start, end, set_rows=False, header=True):
  df_pandas = df_spark.toPandas()
  sh = client.open_by_url(url)
  wks = sh.worksheet_by_title(sheet_name)
  
  # Converte os campos onde tiver valor nulo para inteiro
  df = df_pandas.copy()
  df = df.fillna(-99999)    
  df = df.replace([-99999], [None])
  
  if set_rows:
    wks.rows = df.shape[0]
  
  wks.clear(start=start,end=end,fields='*')
  wks.set_dataframe(df, start, nan='', fit=False, copy_head=header)


# COMMAND ----------

# DBTITLE 1,Tabelas
tables_to_refresh = [
  "self_service_analytics.cards_all_contacts_classifications",
]

for table in tables_to_refresh:
  spark.catalog.refreshTable(table)

all_contacts = (
  spark.table("self_service_analytics.cards_all_contacts_classifications")
    .withColumnRenamed("created_month_date", "created_month")
    .withColumnRenamed("created_week_date", "created_week")
    .filter(f"""
          is_dr
      and is_duplicated is false
      and created_date between date_format(current_date() - INTERVAL 14 MONTH, 'yyyy-MM-01') and '{data_atual_wg}'
    """)
    .withColumn("tag_column_3levels", F.col("tag_classification_3levels"))
    .withColumn("tag_column", F.col("tag_classification"))
)

# display(all_contacts.filter("is_duplicated").count())

# COMMAND ----------

# DBTITLE 1,Datas
# Leitura da data de referência dos dados da all_contacts
data_ref_df = (
  all_contacts
    .select(F.max("created_date").alias("data_atual"))
    .withColumn("data_manual", F.lit(data_atual_wg))
    .withColumn("forcar_data", F.lit(forcar_data))
    .withColumn("data_ref",
      F.expr("""
        case
          when forcar_data = "True" then date(data_manual)
          when data_atual > data_manual then data_atual
          else date(data_manual)
        end
      """))
)

# Leitura da maior data dos dados na tabela de produtos
max_dt_products = spark.sql("""
  with
  mx_cacc as (
    select count(distinct plataform_id) as daily_sum, created_date from self_service_analytics.cards_all_contacts_classifications where created_date <= getArgument('data_atual') group by 2 having daily_sum > 5000
  ),
  mx_cacc_p as (
    select
      created_date,
      verifier_sum/daily_sum as part
    from
      mx_cacc
      left join
        (select count(distinct plataform_id) as verifier_sum, created_date, case when product = 'Sem identificação' then 'S/ ID' else 'Resto' end as verifier from self_service_analytics.cards_all_contacts_classifications group by all)
        using (created_date)
    where verifier = 'S/ ID'
    group by all
    having part < 0.1
  )
  select max(created_date) as max_dt_products from mx_cacc_p
""").collect()[0]['max_dt_products']

# Data em variável python
DATA_REF = str(data_ref_df.toPandas()["data_ref"][0])

datas = (
  data_ref_df
    .withColumn("ano_dados", F.expr("date_format(data_ref - INTERVAL 14 MONTH, 'yyyy-MM-01')"))
    .withColumn("ano_grafico", F.expr("date_format(data_ref - INTERVAL 12 MONTH, 'yyyy-MM-01')"))
    .withColumn("mes_comparacao_du", F.expr("date_format(data_ref - INTERVAL 3 MONTH, 'yyyy-MM-01')"))

    .withColumn("mes_anterior", F.expr("date_add(date_format(data_ref, 'yyyy-MM-01'), -1)"))
    .withColumn("mes_atual", F.expr("date_format(data_ref, 'yyyy-MM-01')"))

    .withColumn("semana_grafico", F.expr("date(date_trunc('week', data_ref) - INTERVAL 14 WEEK)"))
    .withColumn("semana_motivos", F.expr("date(date_trunc('week', data_ref) - INTERVAL 5 WEEK)"))
    .withColumn("semana_anterior", F.expr("date(date_trunc('week', date_add(date_trunc('week', data_ref), -1)))"))
    .withColumn("semana_atual", F.expr("date(date_trunc('week', data_ref))"))

    .withColumn("dias_semana", F.expr("dayofweek(data_ref)"))
    .withColumn("iniciado em:", F.expr("from_unixtime(unix_timestamp()) - interval '3 hour'"))

    .withColumn("max_dt_products", F.lit(max_dt_products))  # Nova data produtos

    .drop("data_manual", "forcar_data", "data_atual")
    .persist()
)

# Tranforma as datas em variáveis do python
datas_pd = datas.toPandas()
ano_dados = str(datas_pd["ano_dados"][0])
ano_grafico = str(datas_pd["ano_grafico"][0])
mes_comparacao_du = str(datas_pd["mes_comparacao_du"][0])
mes_anterior = str(datas_pd["mes_anterior"][0])
mes_atual = str(datas_pd["mes_atual"][0])
semana_grafico = str(datas_pd["semana_grafico"][0])
semana_motivos = str(datas_pd["semana_motivos"][0])
semana_anterior = str(datas_pd["semana_anterior"][0])
semana_atual = str(datas_pd["semana_atual"][0])
dias_semana = int(datas_pd["dias_semana"][0])
data_produtos = str(datas_pd["max_dt_products"][0])


if dias_semana == 1:
  dias_semana = 7
  calculo_prev_semana = 7
else:
  calculo_prev_semana = 6
  dias_semana = dias_semana - 1

calendar = F.broadcast(spark.read.table("shared.calendar"))

# COMMAND ----------

# DBTITLE 1,Produtos
# Produtos do top
products = ['Pré-Pago', 'N1', 'N2', 'N3', 'N4 SL', 'N4 BAU', 'Sem identificação', 'Sem identificação - Expurgado']
products_string = ("'"+"', '".join(map(str, products)) + "'")

# Ordem de apresentação dos produtos para seção de motivos
data = [
  ("N1", 1),
  ("N2", 2),
  ("N3", 3),
  ("N4 SL", 4),
  ("N4 BAU", 5),
  ("Pré-Pago", 6),
  ("Sem identificação", 7),
  ("Sem identificação - Expurgado", 8),
  ]

schema = StructType([
    StructField("product", F.StringType(), True),
    StructField("ordem_produto", F.StringType(), True)
  ])
 
df_ordem_apresentacao = spark.createDataFrame(data=data, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Atendimento

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Dados consolidados

# COMMAND ----------

# DBTITLE 1,Histórico Agrupado
# Seleciona o de/para de dia do mês x o dia útil que ele é dentro do mês
du = (
  calendar
    .filter("is_working_day and calendar_date between '2022-12-01' and current_date()")
    .withColumn("du_mes",
      F.row_number().over(Window.partitionBy(
        F.year(F.col("calendar_date")), F.month(F.col("calendar_date"))).orderBy("calendar_date")))
    .select(F.col("calendar_date").alias("created_date"), "du_mes")
    .distinct()
)

# Monta a base de comparação de "primeira segunda do mês" vs "primeira segunda do mês passado"
week_comparison = (
  calendar
    .filter("calendar_date between '2022-12-01' and current_date()")
    .withColumn("order_week",
      F.rank().over(Window.partitionBy("year_month", "weekday_name").orderBy("calendar_date")))
    .orderBy("calendar_date")
    .withColumn("dia_semana",
      F.when(F.col("weekday_name") == "monday", F.lit("a Seg"))
       .when(F.col("weekday_name") == "tuesday", F.lit("b Ter"))
       .when(F.col("weekday_name") == "wednesday", F.lit("c Qua"))
       .when(F.col("weekday_name") == "thursday", F.lit("d Qui"))
       .when(F.col("weekday_name") == "friday", F.lit("e Sex"))
       .when(F.col("weekday_name") == "saturday", F.lit("f Sáb"))
       .when(F.col("weekday_name") == "sunday", F.lit("g Dom"))
    )
    .withColumn("week_comparison", F.concat_ws("", "order_week", "dia_semana"))
    .select(F.col("calendar_date").alias("created_date"), "week_comparison")
    .distinct()
)

# Contatos agrupados
all_contacts_agg = (
  all_contacts
    .groupBy(
      "contact_type",
      "created_month",
      "created_week",
      "created_date",
      "brand_name",
      "treated_contact_origin",
      "product",
      "tag_classification",
      "tag_classification_3levels",
      "tag_column",
      "segment",
      "first_queue_type",
      "first_queue_name",
      "latest_queue_type",
      "latest_queue_type",
      "is_duplicated",
      "product_function",
    )
    .agg(F.countDistinct("plataform_id").alias("total_contacts"))
    .withColumn("tickets_prev",
      F.when(F.col("created_date") >= semana_atual, F.round(F.col("total_contacts")/dias_semana * calculo_prev_semana,0).cast("int"))
       .otherwise(F.col("total_contacts")))
    
    
    .join(du, ["created_date"], "left")
    .withColumn("du_mes", F.col("du_mes").cast("string"))
    .join(week_comparison, ["created_date"], "left")

    .orderBy(F.desc("created_date"), "contact_type", "treated_contact_origin", "product", "tag_column")
    .persist()
)

last_update = (
  all_contacts_agg
    .filter("treated_contact_origin <> 'Gatilho'")
    .groupBy(F.lit("data").alias("temp"))
    .pivot("treated_contact_origin")
    .agg(F.max("created_date"))
    .drop("temp")
    .persist()
)
# display(all_contacts_agg)
# all_contacts_agg.count()

# COMMAND ----------

# DBTITLE 1,Sheet CONTATOS_ANL
columns = [
  "empresa",
  "treated_contact_origin",
  "created_date",
  "created_week",
  "created_month",
  "product",
]

agg_contacs = (
  all_contacts_agg
    .withColumn("empresa",
      F.when(F.col("first_queue_name") == "Callink (voz)", F.lit("FIS"))
       .otherwise(F.lit("PicPay")))
    .groupBy(
      columns + ["du_mes", "week_comparison"]
    )
    .agg(
      F.sum("total_contacts").alias("total_retidos_dia"),
    )

    .withColumn("total_tickets_dia", F.lit(None))
    .withColumn("total_contatos_dia", F.lit(None))
    .withColumn("num_retidos_1k", F.expr("replace(cast(round(total_retidos_dia/1000, 4) as string), '.', ',')"))

    .distinct()
    .orderBy(columns)
    .select(columns + ["total_retidos_dia", "total_tickets_dia", "total_contatos_dia", "num_retidos_1k", "du_mes", "week_comparison"])
)


export_google_sheets(url, agg_contacs, "CONTATOS_ANL", "A2359", "J15000", header=False)
export_google_sheets(url, du.orderBy("created_date"), "CONTATOS_ANL", "O1", "P1000")
export_google_sheets(url, week_comparison.orderBy("created_date"), "CONTATOS_ANL", "R1", "S1000")

# COMMAND ----------

# DBTITLE 1,Sheet CANAL_AGG
channel_agg = spark.sql(f"""
with dados as (
  select distinct
      created_month
    , created_week
    , created_date
    , contact_type
    , case
        when latest_queue_name = 'FIS' or plataform like '%FIS%' or plataform like '%CALLINK_VOZ%' then 'FIS'
        else 'PicPay'
      end as new_company
    ,  case
        when channel in ('SAC', 'URA') or plataform = 'CALLINK_VOZ' then 'Voz'
        else 'Chat'
      end as new_channel
    , count(distinct plataform_id) as qtd
  from self_service_analytics.cards_all_contacts_classifications
  where 1=1
    and is_dr
    and created_date <= (select max(reference_date) from self_service_analytics.consumer_card_product)
    and created_date >= '{ano_dados}'
  group by all
  order by 3 desc,4,5,6
)

select
    *
  , case
      when created_date >= '{semana_atual}' then cast(round(qtd/{dias_semana} * {calculo_prev_semana},0) as int)
      else qtd
    end as qtd_prev
from dados

""")

if channel_agg.count() > 0:
    export_google_sheets(url, channel_agg, "CANAL_AGG", "A940", "H200000", set_rows=True)
else:
    print("No data to be exported.")

# COMMAND ----------

# DBTITLE 1,Sheet _base
tags_agg = spark.sql(f"""
  select
      max(created_date) over () as max_date
    , created_week
    , created_date
    , product
    , tag_classification
    , tag_classification_3levels
    , contact_category
    , reason
    , count(distinct plataform_id) as qtd
  from self_service_analytics.cards_all_contacts_classifications
  where 1=1
    and created_date >= '2024-12-01'
    and created_date <= '{max_dt_products}'
    and is_cr
    and product in ('Pré-Pago', 'N1', 'N2', 'N3', 'N4 BAU', 'N4 SL', 'Sem identificação', 'Sem identificação - Expurgado')
  group by all
  order by created_date desc
""")

export_google_sheets(url, tags_agg, "_base", "A1", "I20000", set_rows=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Top Motivos Representativos

# COMMAND ----------

# DBTITLE 1,Volume total
total_automatizado = (
  all_contacts_agg
    .filter(f"created_date >= date_sub('{DATA_REF}', {dias_top}) and contact_type = 'Automatizado'")
    .groupBy("product", "tag_column")
    .agg(F.sum("total_contacts").alias("qtd_contatos"))
    .withColumn("total_product",
      F.sum(F.col("qtd_contatos")).over(Window.partitionBy("product")))
    .orderBy("product", F.desc("qtd_contatos"))
)

export_google_sheets(url, total_automatizado, "AUT - Motivos Var. Representatividade", "AF4", "AI600")


total_tickets = (
  all_contacts_agg
    .filter(f"created_date >= date_sub('{DATA_REF}', {dias_top}) and contact_type = 'Ticket'")
    .groupBy("product", "tag_column")
    .agg(F.sum("total_contacts").alias("qtd_contatos"))
    .withColumn("total_product",
      F.sum(F.col("qtd_contatos")).over(Window.partitionBy("product")))
    .orderBy("product", F.desc("qtd_contatos"))
)

export_google_sheets(url, total_tickets, "TKT - Motivos Var. Representatividade", "AF4", "AI600")

# COMMAND ----------

# DBTITLE 1,Função
# Função para retornar o histórico de contatos do top 5 contatos de cada produto
def tag_historical_evolution(dados_df, top=5):
  # Retorna o Top 5 contatos com base nos últimos x dias
  window_spec = Window.partitionBy("contact_type", "product").orderBy(F.desc("qtd_contatos"))

   # Top 5 tags nos últimos x dias
  top5_product = (
    dados_df
      .where((F.col("created_date").between(F.date_add(F.lit(max_dt_products), -5), F.lit(max_dt_products))))
      .groupBy("contact_type", "product", "tag_column")
      .agg(F.sum("total_contacts").alias("qtd_contatos"))
      .withColumn("rank_contacts", F.row_number().over(window_spec))
      .where(F.col("rank_contacts") <= 5)
      .orderBy("contact_type", "product", F.desc("qtd_contatos"))
      .select("contact_type", "tag_column", "qtd_contatos")
  )

  # Usa o top para filtrar o histórico das tags
  top5_by_product = (
    dados_df
      .filter(F.col("created_date") <= F.lit(max_dt_products))
      .join(top5_product, "tag_column", "inner")
      .groupBy("created_month", "created_week", "created_date", "product", "tag_column")
      .agg(F.sum("total_contacts").alias("qtd_contatos"))
      .orderBy(F.desc("created_date"), F.desc("qtd_contatos"))
  )


  ####### Histórico do Top 5 por semana e mês ######
  # Volume das tags por SEMANA
  top5_tags_week = (
    top5_by_product
      .filter(F.col("created_date") >= semana_motivos)
      .groupBy("product", "tag_column")
      .pivot("created_week")
      .sum("qtd_contatos")
      .orderBy("product", F.col(semana_atual).desc())
      .withColumn("ordem", F.row_number().over(Window.orderBy("product", F.col(semana_atual).desc())))
  )

  # Volume das tags por MÊS
  top5_tags_month = (
    top5_by_product
      .withColumn("created_month", F.date_format(F.col("created_month"), "y-MM"))
      .groupBy("product", "tag_column")
      .pivot("created_month")
      .sum("qtd_contatos")
      .drop("product")
  )

  # Consolidado semana + mês
  view_top = (
    top5_tags_week
      .join(top5_tags_month, ["tag_column"], "left")
      .join(df_ordem_apresentacao, "product", "left")
      .orderBy("ordem_produto", "ordem", F.col(semana_atual).desc())
      # .distinct()
      .drop("ordem", "ordem_produto")
  )

  return view_top

# COMMAND ----------

# DBTITLE 1,Monta a visão
# Tickets
dados_tickets = (
  all_contacts_agg
    .filter(
        (F.col("contact_type") == "Ticket")
      & (F.col("created_date") >= mes_comparacao_du)
      & (F.col("first_queue_type") != 'D.Callink (voz)')
      & (~F.col("product").like('%Consignado%'))
    )
  )

view_tickets = tag_historical_evolution(dados_tickets).persist()
export_google_sheets(url, view_tickets, "TKT - Motivos Var. Representatividade", "Q4", "AB24")
print("Tickets - OK!")


# Automatizado
dados_automatizado = (
  all_contacts_agg
    .filter(
        (F.col("contact_type") == "Automatizado") 
      & (F.col("created_date") >= mes_comparacao_du)
      & (~F.col("tag_column").contains("Transferência FIS"))
      & (~F.col("product").like('%Consignado%'))
    )
  )

view_automatizado = tag_historical_evolution(dados_automatizado).persist()
export_google_sheets(url, view_automatizado, "AUT - Motivos Var. Representatividade", "Q4", "AB24")
print("Automatizado - OK!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Top Motivos Absolutos

# COMMAND ----------

# DBTITLE 1,Funções
# Adiciona linhas sem valor no dataframe de motivos de contato abs
def add_lines_reason(df, top):
  # Retorna os produtos que não completaram o top
  lines_by_product = (
      df.groupBy("product")
        .agg(F.countDistinct("tag_column").alias("qtd_line"))
        .filter(f"qtd_line < {top}")
        .toPandas()
    )

  # Percorre cada produto para adicionar linhas em branco
  for index, row in lines_by_product.iterrows():
    product = row["product"]
    lines = row["qtd_line"]

    # Adição de linhas
    for i in range(top - lines):
      values = [(product, "", 0, 0, 0, 0, 0, 0, 0)]

      new_row = spark.createDataFrame(values, df.schema)
      df = df.union(new_row)
      
  return df


def get_variation(df_contatos, top=5):
  window_spec_neg = Window.partitionBy("product").orderBy("var_abs")
  window_spec_pos = Window.partitionBy("product").orderBy(F.desc("var_abs"))

  # Todos os motivos de contato e suas variações absolutas da semana atual (prévia) - semana anterior
  df_var = (
    df_contatos
      .filter((~F.lower(F.col("tag_column")).contains("em aberto"))
            & (~F.lower(F.col("tag_column")).contains("utilização"))
            & (F.col("created_date") >= semana_motivos)
            & (F.col("tag_column") != F.col("product"))
        )
      .groupBy("product", "tag_column")
      .pivot("created_week")
      .sum("tickets_prev")
      .filter(~F.col(semana_atual).isNull())
      .withColumn(semana_anterior, F.when(F.col(semana_anterior).isNull(), F.lit(0)).otherwise(F.col(semana_anterior)))
      .withColumn("var_abs", F.col(semana_atual) - F.col(semana_anterior))
      .withColumn("var_neg", F.row_number().over(window_spec_neg))
      .withColumn("var_pos", F.row_number().over(window_spec_pos))
  )

  # Variações negativas (redução)
  var_neg = (
    df_var
      .where((F.col("var_neg") <= top) & (F.col("var_abs") <= 0))
      .drop(*["var_neg", "var_pos"])
  )
  var_neg = add_lines_reason(var_neg, top).orderBy("product", "var_abs")

  # Variações positivas (aumento)
  var_pos = (
    df_var
      .where((F.col("var_pos") <= top) & (F.col("var_abs") > 0))
      .drop(*["var_neg", "var_pos"])
  )
  var_pos = add_lines_reason(var_pos, top).orderBy("product", F.desc("var_abs"))


  # Dataframe com o top completo
  df_var_abs = var_neg.unionByName(var_pos)

  # Adiciona o dataframe na lista
  return (
    df_var_abs
      .withColumn("ordem", F.monotonically_increasing_id()+1)
      .orderBy("product", "ordem")
      .withColumn("ordem", F.monotonically_increasing_id()+1)
  )


def tag_historical_evolution_abs(df_contatos):
  # Retorna a evolução semanal das tags de acordo com o top 5  
  top_variation_week = get_variation(df_contatos, top=5)

  # Total dos meses
  reason_month = (
    df_contatos
      .withColumn("created_month", F.date_format(F.col("created_month"), "y-MM"))
      .filter(F.col("created_date") >= mes_comparacao_du)
      .groupBy("tag_column")
      .pivot("created_month")
      .sum("total_contacts")
  )

  final_top = (
    top_variation_week
      .join(reason_month, ["tag_column"], "left")
      .join(df_ordem_apresentacao, "product", "left")
      .orderBy("ordem_produto", "ordem")
      .drop("ordem", "ordem_produto")
      .na.fill(0)
  )

  return final_top

# COMMAND ----------

# DBTITLE 1,Monta a visão
# Tickets
dados_tickets = (
  all_contacts_agg
    .filter(
        (F.col("contact_type") == "Ticket")
      & (F.col("created_date") >= mes_comparacao_du)
      & (F.col("first_queue_type") != 'D.Callink (voz)')
      & (~F.col("product").like('%Consignado%'))
    )
  )

view_tickets_abs = tag_historical_evolution_abs(dados_tickets)
export_google_sheets(url, view_tickets_abs, "TKT - Motivos Var. Absoluta", "Q52", "AC122")
print("Tickets - OK!")


# Automatizado
dados_automatizado = (
  all_contacts_agg
    .filter(
        (F.col("contact_type") == "Automatizado")
      & (F.col("created_date") >= mes_comparacao_du)
      & (~F.col("tag_column").contains("Transferência FIS"))
      & (~F.col("product").like('%Consignado%'))
    )
  )

view_automatizado_abs = tag_historical_evolution_abs(dados_automatizado)
export_google_sheets(url, view_automatizado_abs, "AUT - Motivos Var. Absoluta", "Q52", "AC122")
print("Automatizado - OK!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### NPS

# COMMAND ----------

nps_fs_new = spark.sql("""
  select
    answer_month,
    case when detractors = 1 then 'detrator' when passives = 1 then 'neutro' when promoters = 1 then 'promotor' else null end as classification,
    contact_type,
    product_treated as ppcard_product,
    null as is_lg,
    count(distinct invite_id) as total_respondentes
  from
    self_service_analytics.cards_npsa_answers
  where
    answer_date >= date_format(current_date() - INTERVAL 14 MONTH, 'yyyy-MM-01')
    and product_treated not like 'Cons%'
  group by
    all
  order by
    1 desc
""")

export_google_sheets(url, nps_fs_new, "NPS", "A46", "D1240", header=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MAT Level + MAT Função

# COMMAND ----------

# MAGIC %sql refresh table self_service_analytics.card_mat

# COMMAND ----------

# DBTITLE 1,MAT Level + MAT Funcao
mat_sheet = "https://docs.google.com/spreadsheets/d/17RjxkEf50XqQMBqnDbZtwaSoKYcbmOmCcd2SzTKtcPU"

mat_level = spark.sql("""
  select distinct month_date, case when level = 'Débito' then 'Pré-Pago' else level end as level, value
  from self_service_analytics.card_mat
  where kpi = 'MAT level'
    and date_trunc('month', month_date) >= date_trunc('month', current_date - interval 13 month)
  order by 1 desc
""")

export_google_sheets(mat_sheet, mat_level, "Mod. Jun/25 - MAT LEVEL OPS", "V3", "X100")

mat_unico = spark.sql("""
  select distinct month_date, 'Único' as level, value
  from self_service_analytics.card_mat
  where kpi = 'MAT único'
    and date_trunc('month', month_date) >= date_trunc('month', current_date - interval 13 month)
  order by 1 desc
""")

export_google_sheets(mat_sheet, mat_unico, "Mod. Jun/25 - MAT LEVEL OPS", "Z3", "AB30")

mat_funcao = spark.sql("""
  select distinct month_date, product, value
  from self_service_analytics.card_mat
  where kpi = 'MAT função'
    and date_trunc('month', month_date) >= date_trunc('month', current_date - interval 13 month)
  order by 1 desc
""")

export_google_sheets(url, mat_funcao, "DR/CR MAT Função", "A1", "C30")

numerador_mat_funcao = spark.sql("""
  select distinct
    count(distinct plataform_id) as contatos,
    is_dr,
    is_cr,
    date(date_trunc('month', created_date)) as created_month,
    product_function
  from
    self_service_analytics.cards_all_contacts_classifications
  where 1=1
    and vertical = 'Card'
    and date_trunc('month', created_date) >= date_trunc('month', current_date - interval 13 month)
    and is_dr
  group by all
  order by created_month desc, product_function, is_dr, is_cr
""")

export_google_sheets(url, numerador_mat_funcao, "DR/CR MAT Função", "E1", "I80")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Finalização

# COMMAND ----------

# DBTITLE 1,Timestamp
datas = (
  datas
    .crossJoin(last_update)
    .drop("dias_semana")
    .withColumn("finalizado em:", F.expr("from_unixtime(unix_timestamp()) - interval '3 hour'"))
    .drop("ano_dados")
)

sh = client.open_by_url(url)
wks = sh.worksheet_by_title('metadados')
wks.clear(start='B7',end='Q8',fields='*')
wks.set_dataframe(datas.toPandas(),'B7')

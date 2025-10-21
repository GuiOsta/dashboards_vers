# Databricks notebook source
# MAGIC %md
# MAGIC ##self_service_analytics.logistics_tracking
# MAGIC ##### jira tasks: <nav> 
# MAGIC   https://picpay.atlassian.net/browse/FMP-1657
# MAGIC       </nav>   
# MAGIC ##### last authors : <nav>
# MAGIC   @Guilherme Ostaneli
# MAGIC       </nav>
# MAGIC ##### last update: 
# MAGIC   29/09/2025

# COMMAND ----------

# DBTITLE 1,Data e hora
from datetime import datetime, timedelta

current_datetime = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')


spark.createDataFrame([(current_datetime,)], ['current_datetime']).createOrReplaceTempView("current_datetime")
display(current_datetime)

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyspark.sql.types import StringType, IntegerType, DateType
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from functools import reduce
import pandas as pd
import boto3






import sys
import subprocess
import pkg_resources
required = {'pygsheets', 'pyxlsb', 'openpyxl', 'fsspec', 's3fs', 'plotly'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
spark.conf.set(
    "spark.sql.execution.arrow.pyspark.fallback.enabled", "true"
)

if missing:
  python = sys.executable
  subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType


import pygsheets
import pyxlsb
import openpyxl 



if missing:
    python = sys.executable
import numpy as np
import pandas as pd
import multiprocessing
from datetime import date

# COMMAND ----------

# DBTITLE 1,Tabelas
tables_to_update = [
  "shared.calendar",
  "card_operations.fis_users_events",
  "card_operations.fis_accounts",
  "ppcard.accounts_history",
  "card.ppcard_variant_upgrade",
  "card_operations.fis_embossings",
  "consumers.consumers",
  "card.embossing_processes",
  "card.embossing_expeditions",
  "card_operations.tracking_card_delivery",
  "card.fragmentation_card_deliveries",
  "card_operations.fis_updates_history",
  "consumers.consumers_all_addresses",
  # "card.ppcard_daily_userbase",
  "self_service_analytics.consumer_card_product"
]

for table in tables_to_update:
  spark.catalog.refreshTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Processamento

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INSS

# COMMAND ----------

# DBTITLE 1,Junção tabelas
benefit_cards = spark.table("benefits.benefits_cards")
benefit_collaborators = spark.table("benefits.collaborators")
benefit_companies = spark.table("benefits.companies")
benefit_accounts = spark.table("benefits.accounts")

benefit_inss_df = (
  benefit_cards
    .join(benefit_collaborators, "collaborator_id", "left")
    .join(benefit_companies, "company_id", "left")
    .join(benefit_accounts, "account_id", "left")
    .filter((benefit_companies.company_document == '42422253000101') & (benefit_cards.is_virtual_card == False))
    .withColumn("issue_reason",
        F.when(benefit_cards.issue == 1, F.lit(1))
         .when(benefit_cards.issue > 1, F.lit(3))
    )
    .select(
      F.lit(None).alias("fis_embossing_id"),
      benefit_companies.company_document.alias("organization_id"),
      benefit_cards.tracking_id.cast("long").alias("ar_code"),
      benefit_cards.processor_id.alias("account_number"),
      F.lit(False).alias("is_thief_account"),
      benefit_cards.processor_id.alias("fis_account_number"),
      benefit_cards.bin_card.alias("bin"),
      benefit_cards.credit_card_last4.alias("card_last4"),
      benefit_collaborators.consumer_id,
      F.lit(None).alias("document_number"),
      F.lit(None).alias("is_cpf"),
      F.lit(None).alias("product_id"), # precisá colocar delivery kit code no futuro
      F.lit("D").alias("holder_code"),
      "issue_reason",
      F.lit(False).alias("is_dependent"),
      F.lit(None).alias("fis_card_type"),
      F.lit(None).alias("card_modality"),
      F.lit(None).alias("production_kit"), # precisá colocar delivery kit code no futuro
      F.lit("J").alias("embossing_code"),
      F.lit(None).alias("courier_code"), # precisá colocar delivery kit code no futuro
      F.lit(None).alias("delivery_channel"),
      F.lit(None).alias("city"),
      F.lit(None).alias("state"),
      F.lit(None).alias("street"),
      F.lit(None).alias("street_number"),
      F.lit(None).alias("complement"),
      F.lit(None).alias("neighborhood"),
      F.lit(None).alias("post_code"),
      F.date_add(F.to_date(benefit_cards.created_at),1).alias("processing_date"),
      F.to_date(benefit_cards.created_at).alias("reference_date"),
      F.lit(None).alias("source_file_name")
    )
  .persist()
  .filter((F.col("reference_date") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳
)

# display(benefit_inss_df.filter("ar_code = 48369146"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FIS

# COMMAND ----------

# DBTITLE 1,Processados FIS
user_base = (
  spark.table ("self_service_analytics.consumer_card_product")
  .filter((F.col("reference_date") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳
  .selectExpr(
    "reference_date as processing_date",
    "consumer_id",
    "card_product as ppcard_product",
    "card_variant")
)

df_fis_processed = (
  spark.read.table("card_operations.fis_embossings")
    .unionByName(benefit_inss_df)
    .join(user_base, ["consumer_id", "processing_date"], "left")
    .filter((F.col("reference_date") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳

    .withColumnRenamed("organization_id", "org")
    .withColumn("courier_name",
      F.when(F.col("courier_code") == "02", F.lit("Flash"))
      .when(F.col("courier_code") == "99", F.lit("Correios"))
      .when(F.col("courier_code") == "30", F.lit("Mobi"))
      .when(F.col("courier_code") == "31", F.lit("Speedflow")))
    
    .withColumn("embossing_name",
      F.when(F.col("embossing_code") == "2", F.lit("Valid"))
       .when(F.col("embossing_code") == "5", F.lit("Thales")) 
       .when(F.col("embossing_code") == "J", F.lit("JallCard")))
    
    .withColumnRenamed("issue_reason", "original_issue_reason")
    .withColumnRenamed("processing_date", "fis_processing_date")
    .withColumnRenamed("reference_date", "fis_reference_date")
    .withColumnRenamed("document_number", "cpf")
    .withColumnRenamed("product_id", "logo")

    .withColumn("ppcard_product",
      F.when(F.col("org") == 211, F.col("ppcard_product"))
       .when(F.col("org") == 42422253000101, F.lit("INSS")))
    
    .withColumn("card_variant",
      F.when(F.col("org") == 211, F.col("card_variant")))

    .select(
      "org",
      "fis_reference_date",
      "fis_processing_date",
      "account_number",
      # "embossing_code",
      "original_issue_reason",
      "card_modality",
      "bin",
      "card_last4",
      "logo",
      "ar_code",
      "courier_code",
      "courier_name",
      "embossing_name",
      "city",
      "state",
      "post_code",
      "consumer_id",
      "cpf",

      # userbase
      "ppcard_product",
      "card_variant"
    )
    .persist()
)

additional_card = (
  spark.table("card_operations.fis_updates_history")
    .filter("is_card_dependent")
    .select("account_number", "card_last4", "is_card_dependent")
    .distinct()
)

fis_processed = (
  df_fis_processed
    .withColumn("latest_processing_date", F.max("fis_processing_date").over(Window.partitionBy()))
    
    # Identificação do cartão adicional
    .join(additional_card, ["account_number", "card_last4"], "left")
    .withColumn("is_card_dependent", F.coalesce("is_card_dependent", F.lit(False)))
)

# display(fis_processed.filter("consumer_id = 236282676378105351"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contas vs Processamento

# COMMAND ----------

# DBTITLE 1,OLD - Contas PP
# card_account = (
#     spark.table("self_service_analytics.logistics_cards_account")
#     .filter((F.col("credit_acquired_at") >= (F.current_date() - F.expr("interval 4 months"))) | (F.col("debit_acquired_at") >= (F.current_date() - F.expr("interval 4 months")))
#             | (F.col("card_acquired_at") >= (F.current_date() - F.expr("interval 4 months")))
#             | (F.col("card_account_opening_at") >= (F.current_date() - F.expr("interval 4 months"))))
#     .withColumnRenamed("card_acquired_at", "lca_card_acquired_at")
# )

# COMMAND ----------

# DBTITLE 1,NEW - Contas PP
card_account = (
    spark.table("self_service_analytics.logistics_cards_account").alias("a")
    .join(fis_processed.alias("b"), on=["account_number", "card_last4"], how="left")
    .withColumn(
        "lca_card_acquired_at",
        F.when(F.col("b.fis_reference_date").isNotNull(), F.col("b.fis_reference_date"))
         .otherwise(F.col("a.card_acquired_at"))
    )

    .select(
      "a.account_number",
      "a.cpf",
      "a.card_last4",
      "a.card_type",
      "a.block_code",
      "a.issue_reason",
      "a.consumer_id",
      "a.card_account_opening_at",
      "a.debit_acquired_at",
      "a.credit_acquired_at",
      "lca_card_acquired_at",
      "a.origin"
      )

    .filter((F.col("card_acquired_at") >= (F.current_date() - F.expr("interval 14 months")))
         | (F.col("updated_at") >= (F.current_date() - F.expr("interval 14 months")))
    )
)

# COMMAND ----------

# DBTITLE 1,Contas PP vs FIS
# # Embossings não localizados no PicPay (está na FIS, mas não no PicPay)
fis_without_pp = fis_processed.join(card_account.select("account_number", "card_last4"), ["account_number", "card_last4"], "left_anti")

# # Join para pegar as colunas e assim manter o formato esperado
fis_df = (
  fis_without_pp
    # .join(card_account.drop("consumer_id", "cpf"), "account_number", "left")
    .join(card_account.drop("consumer_id", "cpf"), ["account_number", "card_last4"], "left")
    .distinct()
)

# Contas PicPay x Embossing FIS
card_account_fis_pv = (
  card_account
    .join(fis_processed.drop("consumer_id", "cpf"), ["account_number", "card_last4"], "left")
    .distinct()
)

upgraded_acc = (
  spark.table("card.ppcard_variant_upgrade")
    .filter("""
          offered_at is not null
      and accepted_at is not null
      and upgraded_at is not null
    """)
    .withColumnRenamed("target_account_number", "account_number")
    .select("account_number", "accepted_at")
)

# Une as contas
card_account_fis = (
  card_account_fis_pv
    # .unionByName(card_account_fis_outros)
    .unionByName(fis_df)
    .withColumn("org", F.when(F.col("org").isNull(), F.lit("211")).otherwise(F.col("org")))
    .distinct()

    .join(upgraded_acc, "account_number", "left")
    .withColumn("is_upgrade",
      F.when(F.col("accepted_at").isNotNull(), F.lit(True))
       .otherwise(F.lit(False)))

    .withColumn("card_acquired_at",
      F.expr("""
        case
          when original_issue_reason is null then lca_card_acquired_at                                                              -- Quando o cartão está na cards_history, mas não está na fis_embossing
          when issue_reason <> 1 or is_card_dependent then fis_reference_date                                                       -- Quando o cartão é Reemissão
          when org = '42422253000101' then fis_reference_date                                                                       -- Quando o cartão é Benefício, assumimos a data da FIS
          when debit_acquired_at < card_account_opening_at and credit_acquired_at is null then lca_card_acquired_at                 -- Quando o cartão é antigo, assumimos a data da FIS
          when debit_acquired_at is null and credit_acquired_at is null and card_account_opening_at is null then fis_reference_date -- Para contas muito antigas (abriu conta sem pedir cartão), assumimos a data da FIS
          when debit_acquired_at is null and credit_acquired_at is null then lca_card_acquired_at                                   -- Para contas muito antigas (abriu conta sem pedir cartão), assumimos a data da FIS
          when issue_reason = 1 and card_modality in ('01', '03', '12', '24') then debit_acquired_at
          when issue_reason = 1 and (card_modality not in ('01', '03', '12', '24') or card_modality is null) then credit_acquired_at
          else lca_card_acquired_at
        end
      """))

      .withColumn("issue_reason",
      F.expr("""
        case
          when issue_reason > 1 then original_issue_reason else issue_reason end
      """))
)

card_account_fis.createOrReplaceTempView("pp_fis")

# COMMAND ----------

# DBTITLE 1,UF / Cidade
# Procura a UF quando ela estiver nula, pois podemos ter casos de legado onde enviamos a UF nula para a FIS e está assim lá até hoje
consumer_uf = (
  spark.read.table("consumers.consumers_all_addresses")

    .filter("upper(state) in ('SP','RJ','MG','BA','PR','PA','RS','CE','PE','GO','ES','AM','MA','SC',\
                              'MT','RN','PB','DF','MS','AL','PI','SE','RO','TO','AP','AC','RR')")

    .withColumn("last_id", F.max("consumer_all_address_id").over(Window.partitionBy("consumer_id")))
    .filter("last_id = consumer_all_address_id")
    .withColumnRenamed("state", "address_state")
    .select("consumer_id", "address_state")
)

card_account_fis = (
  card_account_fis
    .join(consumer_uf, "consumer_id", "left")
    .withColumn("state",
     F.when((F.col("state").isNull())
          | (F.length(F.col("state")) < 2)
          | (F.col("state") == "  ")
          | (~F.col("state").isin('SP','RJ','MG','BA','PR','PA','RS','CE','PE','GO','ES','AM','MA','SC',
          'MT','RN','PB','DF','MS','AL','PI','SE','RO','TO','AP','AC','RR')), F.col("address_state"))
      .otherwise(F.col("state")))
    .drop("address_state")
    .distinct()
)

# remove os clientes que estão pendentes de ordem de embossing na FIS, mas que já são clientes desativados. Essa alteração foi solicitada no card https://picpay.atlassian.net/browse/FMP-604
pp_fis = (
  card_account_fis
    .filter("""
    consumer_id not in (
      319623696169748866,983567371212650285,112972519391451980,110386958008704258,169903148242675615,242083932473734302,219362550929384591,317741917825793787,753465497153179905,234845669865246214,242279919494076728,232497477967940712,253588619160095505,269333209282549039,156422739817962504,194623369155354325,211688108150299808,226671793284961889,336433587636446654,121580613042670204,755350357507063498,177977097247597487,140726919242216534,816549771616901770
    )
    or consumer_id is null
    """)
)

pp_fis.createOrReplaceTempView("pp_fis")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Couriers

# COMMAND ----------

# DBTITLE 1,De x Para - Mobi
# MAGIC %sql
# MAGIC create or replace temp view status_mobi as
# MAGIC select * from values
# MAGIC ('Arquivo processado', 'Aguardando postagem'),
# MAGIC ('Correios - Objeto aguardando retirada no endereço indicado', 'Aguardando retirada'),
# MAGIC ('Correios - Objeto encaminhado para retirada no endereço indicado', 'Aguardando retirada'),
# MAGIC ('Em tratativa', 'Custódia'),
# MAGIC ('Entrada na custódia - HUB', 'Custódia'),
# MAGIC ('Custódia HUB000', 'Custódia'),
# MAGIC ('Fragmentado', 'Fragmentado'),
# MAGIC ('Ata de fragmentação gerada', 'Fragmentado'),
# MAGIC ('Em devolução para origem', 'Em devolução'),
# MAGIC ('Tratativa devolver', 'Em devolução'),
# MAGIC ('Devolução recebida na origem', 'Em devolução'),
# MAGIC ('Devolvido ao remetente', 'Em devolução'),
# MAGIC ('Em devolução para o remetente', 'Em devolução'),
# MAGIC ('Devolução automática', 'Em devolução'),
# MAGIC ('Preparada para devolução ao remetente', 'Em devolução'),
# MAGIC ('Devolução para redirecionamento', 'Em devolução'),
# MAGIC ('Devolução dos correios recebida', 'Em devolução'),
# MAGIC ('Correios - Objeto devolvido ao remetente', 'Em devolução'),
# MAGIC ('Correios - Objeto entregue ao remetente', 'Em devolução'),
# MAGIC ('Correios - Objeto saiu para entrega ao remetente', 'Em devolução'),
# MAGIC ('Remessa conciliada', 'Em rota de entrega'),
# MAGIC ('Remessa consolidada', 'Em rota de entrega'),
# MAGIC ('Carga transferida', 'Em rota de entrega'),
# MAGIC ('Carga recebida', 'Em rota de entrega'),
# MAGIC ('Remessa desconsolidada', 'Em rota de entrega'),
# MAGIC ('Remessa em rota de entrega', 'Em rota de entrega'),
# MAGIC ('Postado nos correios', 'Em rota de entrega'),
# MAGIC ('Enviado para postagem nos correios', 'Em rota de entrega'),
# MAGIC ('Preparar para postagem', 'Em rota de entrega'),
# MAGIC ('Entrega reprogramada', 'Em rota de entrega'),
# MAGIC ('Postagem Transferida', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto saiu para entrega ao destinatário', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto não localizado', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto com atraso na entrega', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto recebido na Unidade dos Correios', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto encaminhado', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto mal encaminhado', 'Em rota de entrega'),
# MAGIC ('Correios - Área com distribuição sujeita a prazo diferenciado', 'Em rota de entrega'),
# MAGIC ('Correios - Conferido', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto ainda não chegou à unidade', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto recebido na unidade de distribuição', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto postado após o horário limite da unidade', 'Em rota de entrega'),
# MAGIC ('Correios - Carteiro não atendido - Entrega não realizada', 'Em rota de entrega'),
# MAGIC ('Correios - Cliente mudou-se - Entrega não realizada', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto postado', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto em trânsito - por favor aguarde', 'Em rota de entrega'),
# MAGIC ('Correios - Objeto em correção de rota', 'Em rota de entrega'),
# MAGIC ('Remessa cadastrada', 'Em rota de entrega'),
# MAGIC ('Ausente', 'Entrega não efetuada'),
# MAGIC ('Local demolido', 'Entrega não efetuada'),
# MAGIC ('Local em reforma', 'Entrega não efetuada'),
# MAGIC ('Número não localizado', 'Entrega não efetuada'),
# MAGIC ('Destinatário falecido', 'Entrega não efetuada'),
# MAGIC ('Recusado pelo porteiro', 'Entrega não efetuada'),
# MAGIC ('Destinatário desconhecido', 'Entrega não efetuada'),
# MAGIC ('Destinatário não localizado', 'Entrega não efetuada'),
# MAGIC ('Endereço de difícil acesso', 'Entrega não efetuada'),
# MAGIC ('Retido a pedido do remetente', 'Entrega não efetuada'),
# MAGIC ('Rua não localizada', 'Entrega não efetuada'),
# MAGIC ('Cancelado a pedido do destinatário', 'Entrega não efetuada'),
# MAGIC ('Caixa postal', 'Entrega não efetuada'),
# MAGIC ('Fora de abrangência de atendimento', 'Entrega não efetuada'),
# MAGIC ('Recusado pelo destinatário', 'Entrega não efetuada'),
# MAGIC ('Mudou-se', 'Entrega não efetuada'),
# MAGIC ('Cep incorreto', 'Entrega não efetuada'),
# MAGIC ('Destinatário viajando', 'Entrega não efetuada'),
# MAGIC ('Destinatário em férias', 'Entrega não efetuada'),
# MAGIC ('Ausente 1ª tentativa', 'Entrega não efetuada'),
# MAGIC ('Ausente 2ª Tentativa', 'Entrega não efetuada'),
# MAGIC ('Ausente 3ª tentativa', 'Entrega não efetuada'),
# MAGIC ('Zona rural', 'Entrega não efetuada'),
# MAGIC ('Local fechado', 'Entrega não efetuada'),
# MAGIC ('Endereço insuficiente', 'Entrega não efetuada'),
# MAGIC ('Endereço incorreto', 'Entrega não efetuada'),
# MAGIC ('Área de risco', 'Entrega não efetuada'),
# MAGIC ('Greve geral', 'Entrega não efetuada'),
# MAGIC ('Cancelado a pedido do remetente', 'Entrega não efetuada'),
# MAGIC ('Paralisação rodoviária', 'Entrega não efetuada'),
# MAGIC ('Jogo Copa do Mundo', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Carteiro não atendido', 'Entrega não efetuada'),
# MAGIC ('Correios - Remetente não retirou objeto na Unidade dos Correios', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Cliente recusou-se a receber', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Cliente desconhecido no local', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Endereço incorreto', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Cliente mudou-se', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto disponível para retirada em Caixa Postal', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Empresa sem expediente', 'Entrega não efetuada'),
# MAGIC ('Correios - Destinatário não retirou objeto na Unidade dos Correios', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto e/ou conteúdo avariado', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto com data de entrega agendada', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Destinatário não apresentou documento', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Logradouro com numeração irregular', 'Entrega não efetuada'),
# MAGIC ('Correios - Coleta ou entrega de objeto não efetuada', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto e/ou conteúdo avariado por acidente com veículo', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto endereçado à empresa falida', 'Entrega não efetuada'),
# MAGIC ('Correios - A importação do objeto/conteúdo não foi autorizada pelos órgãos', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega do objeto está condicionada à composição', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto apreendido por órgão de fiscalização ou outro órgão anuente', 'Entrega não efetuada'),
# MAGIC ('Correios - Tentativa de entrega não efetuada', 'Entrega não efetuada'),
# MAGIC ('Correios - Saída para entrega cancelada', 'Entrega não efetuada'),
# MAGIC ('Correios - Retirada em Unidade dos Correios não autorizada pelo remetente', 'Entrega não efetuada'),
# MAGIC ('Correios - As dimensões do objeto impossibilitam o tratamento e a entrega', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto será devolvido por solicitação do remetente', 'Entrega não efetuada'),
# MAGIC ('Eventos naturais', 'Entrega não efetuada'),
# MAGIC ('Correios - Endereço incorreto - Entrega não realizada', 'Entrega não efetuada'),
# MAGIC ('Correios - Destinatário não retirou objeto no prazo', 'Entrega não efetuada'),
# MAGIC ('Empresa fechada - Coronavirus', 'Entrega não efetuada'),
# MAGIC ('Atraso na transferencia - Coronavirus', 'Entrega não efetuada'),
# MAGIC ('Correios - Solicitação de suspensão de entrega recebida', 'Entrega não efetuada'),
# MAGIC ('Correios - Cliente desconhecido no local - Entrega não realizada', 'Entrega não efetuada'),
# MAGIC ('Correios - Cliente recusou-se a receber o objeto - Entrega não realizada', 'Entrega não efetuada'),
# MAGIC ('Correios - Empresa sem expediente - Entrega não realizada', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não localizado no fluxo postal', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto será devolvido por solicitação do contratante/remetente', 'Entrega não efetuada'),
# MAGIC ('Correios - A entrega não pode ser efetuada - Destinatário não apresentou documento exigido', 'Entrega não efetuada'),
# MAGIC ('Enviar para Custódia', 'Entrega não efetuada'),
# MAGIC ('Recebedor desconhecido/Não localizado', 'Entrega não efetuada'),
# MAGIC ('Entregue em local errado, produto recuperado e insucesso na entrega', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - carteiro não atendido', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - endereço incorreto', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - cliente desconhecido no local', 'Entrega não efetuada'),
# MAGIC ('Correios - Prazo de retirada pelo destinatário encerrado', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - cliente recusou-se a receber o objeto', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - Endereço não encontrado', 'Entrega não efetuada'),
# MAGIC ('Correios - Direcionado para entrega interna a pedido do cliente', 'Entrega não efetuada'),
# MAGIC ('Correios - Objeto não entregue - cliente mudou-se', 'Entrega não efetuada'),
# MAGIC ('Objeto não entregue - prazo de retirada encerrado', 'Entrega não efetuada'),
# MAGIC ('Entregue com sucesso', 'Entregue'),
# MAGIC ('Comprovante digitalizado', 'Entregue'),
# MAGIC ('Correios - Objeto entregue ao destinatário', 'Entregue'),
# MAGIC ('Entregue na caixa de correspondência', 'Entregue'),
# MAGIC ('Objeto entregue', 'Entregue'),
# MAGIC ('Correios - Objeto entregue na Caixa de Correios Inteligente', 'Entregue'),
# MAGIC ('Entregue em local errado, produto recuperado e entregue', 'Entregue'),
# MAGIC ('Entregue com sucesso após acareação', 'Entregue'),
# MAGIC ('Remessa a ser redirecionada', 'Erro - Avaliar'),
# MAGIC ('Remessa finalizada para reentrega', 'Erro - Avaliar'),
# MAGIC ('Divergência CEP x UF', 'Inconsistência'),
# MAGIC ('Redirecionamento automático', 'Erro - Avaliar'),
# MAGIC ('Reter para devolver ao cliente', 'Erro - Avaliar'),
# MAGIC ('Reter para postagem', 'Erro - Avaliar'),
# MAGIC ('Correios - Objeto devolvido aos Correios', 'Erro - Avaliar'),
# MAGIC ('Correios - Desistência de postagem pelo remetente', 'Erro - Avaliar'),
# MAGIC ('Correios - A entrega do objeto está condicionada à composição do lote', 'Erro - Avaliar'),
# MAGIC ('Lockdown - Coronavirus', 'Erro - Avaliar'),
# MAGIC ('Reter e aguardar definição do cliente', 'Erro - Avaliar'),
# MAGIC ('Aguardando Saída Estoque', 'Erro - Avaliar'),
# MAGIC ('Enviado para Manuseio', 'Erro - Avaliar'),
# MAGIC ('Manuseio Realizado', 'Erro - Avaliar'),
# MAGIC ('Aguardando Manuseio', 'Erro - Avaliar'),
# MAGIC ('Produto coletado', 'Erro - Avaliar'),
# MAGIC ('Aguardando definição do cliente', 'Erro - Avaliar'),
# MAGIC ('Comprovante de devolução digitalizado', 'Erro - Avaliar'),
# MAGIC ('Remessa não coletada', 'Cancelado sem físico'),
# MAGIC ('Liberado para manuseio', 'Erro - Avaliar'),
# MAGIC ('Troca realizada', 'Erro - Avaliar'),
# MAGIC ('Reter para fragmentação', 'Pendente fragmentação'),
# MAGIC ('Remessa a ser fragmentada', 'Pendente fragmentação'),
# MAGIC ('Tratativa entregar', 'Pendente reenvio'),
# MAGIC ('Roubo', 'Sinistrado'),
# MAGIC ('Furto', 'Sinistrado'),
# MAGIC ('Extravio', 'Sinistrado'),
# MAGIC ('Avaria', 'Sinistrado'),
# MAGIC ('Correios - Objeto roubado', 'Sinistrado'),
# MAGIC ('Correios - Objeto Extraviado', 'Sinistrado'),
# MAGIC ('Entregue em local errado, produto não recuperado', 'Sinistrado')
# MAGIC   as temp_table(ultimo_status, novo_status)
# MAGIC

# COMMAND ----------

status_mobi_df = spark.table("status_mobi")

# COMMAND ----------

# DBTITLE 1,De x Para - Flash
# MAGIC %sql
# MAGIC create or replace temp view status_flash as
# MAGIC select * from values
# MAGIC ('PENDENTE_FLASH','Objeto Retirado da Custodia','Custódia Tratada'),
# MAGIC ('CUSTODIA_FLASH','Aguardando telemarketing','Custódia'),
# MAGIC ('CANCELADO_POSTAGEM','HAWB Cancelada - sem fisico','Cancelado sem físico'),
# MAGIC ('CUSTODIA_FLASH','Devolu?o Habilitada','Em devolução'),
# MAGIC ('CUSTODIA_FLASH','Devolução Conciliada','Em devolução'),
# MAGIC ('CUSTODIA_FLASH','Devolução Habilitada','Em devolução'),
# MAGIC ('CUSTODIA_FLASH','Devolucao Recebida - Avulsa','Em devolução'),
# MAGIC ('CUSTODIA_FLASH','AR digitalizada','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Comprovante registrado','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Devolução extraviada','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Devolvendo via Terceiro','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Em ligação - Telemarketing','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Entrega NAO efetuada(RT)','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Entrega registrada','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Entrega registrada via RT','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','null','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','OBJETO Recebido','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Postado - logistica iniciada','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Preparada para a transferencia','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Programado Nova Tentativa','Erro - Avaliar'),
# MAGIC ('CUSTODIA_FLASH','Importacao - pendecia gerada','Inconsistência'),
# MAGIC ('CUSTODIA_FLASH','Habilitado para Reenvio','Pendente reenvio'),
# MAGIC ('CUSTODIA_FLASH','HAWB - Dados cadastrados','Pendente reenvio'),
# MAGIC ('CUSTODIA_FLASH','Objeto Retirado da Custodia','Pendente reenvio'),
# MAGIC ('CUSTODIADO_FLASH','Aguardando telemarketing','Custódia'),
# MAGIC ('CUSTODIADO_FLASH','Devolu?o Habilitada','Em devolução'),
# MAGIC ('CUSTODIADO_FLASH','DevoluÃ§Ã£o Habilitada','Em devolução'),
# MAGIC ('CUSTODIADO_FLASH','Devolução Habilitada','Em devolução'),
# MAGIC ('CUSTODIADO_FLASH','Habilitado para Reenvio','Pendente reenvio'),
# MAGIC ('CUSTODIADO_FLASH','Objeto Retirado da Custodia','Pendente reenvio'),
# MAGIC ('DESTRUIDO','Destrui?o Validada','Fragmentado'),
# MAGIC ('DESTRUIDO','DestruiÃ§Ã£o Validada','Fragmentado'),
# MAGIC ('DESTRUIDO','Destruição Validada','Fragmentado'),
# MAGIC ('DESTRUIDO_FLASH','Devolução Habilitada','Erro - Avaliar'),
# MAGIC ('DESTRUIDO_FLASH','null','Erro - Avaliar'),
# MAGIC ('DESTRUIDO_FLASH','Destrui?o Validada','Fragmentado'),
# MAGIC ('DESTRUIDO_FLASH','Destruição Validada','Fragmentado'),
# MAGIC ('DESTRUIDO_FLASH','Protocolado para Destruicao','Pendente fragmentação'),
# MAGIC ('DEVOLVIDO_FLASH','Devol. Protocolada - FRANQUIA','Em devolução'),
# MAGIC ('DEVOLVIDO_FLASH','Devolu?o Conciliada','Em devolução'),
# MAGIC ('DEVOLVIDO_FLASH','Devolução Conciliada','Em devolução'),
# MAGIC ('DEVOLVIDO_FLASH','Devolução Habilitada','Em devolução'),
# MAGIC ('DEVOLVIDO_FLASH','Devolucao Recebida - Avulsa','Em devolução'),
# MAGIC ('DEVOLVIDO_FLASH','Aguardando telemarketing','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','AR digitalizada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Comprovante registrado','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Devolu?o extraviada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Devolução extraviada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Devolvendo via Terceiro','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Em ligação - Telemarketing','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Entrega NAO efetuada(RT)','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Entrega registrada via RT','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Habilitado para Reenvio','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','HAWB - Dados cadastrados','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Importacao - pendecia gerada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','null','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','OBJETO Recebido','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Objeto Retirado da Custodia','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Postado - logistica iniciada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Preparada para a transferencia','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','Programado Nova Tentativa','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_FLASH','','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devolução Conciliada','Em devolução'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devolucao Recebida - Avulsa','Em devolução'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devolvendo via Terceiro','Em devolução'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devolvido pelo Terceiro','Em devolução'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Aguardando Retirada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Aguardando telemarketing','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','AR digitalizada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Comprovante registrado','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Correios Pi','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devol. Protocolada - FRANQUIA','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Devolução extraviada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Entrega NAO efetuada(RT)','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Entregue pelo Terceiro','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','HAWB - Dados cadastrados','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Importacao - pendecia gerada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','null','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','OBJETO Recebido','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Objeto Retirado da Custodia','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Postado - logistica iniciada','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Preparada para a transferencia','Erro - Avaliar'),
# MAGIC ('DEVOLVIDO_TERCEIRO','Redespachado por Terceiro','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','AR digitalizada','Entregue'),
# MAGIC ('ENTREGUE_FLASH','Comprovante registrado','Entregue'),
# MAGIC ('ENTREGUE_FLASH','Entrega registrada','Entregue'),
# MAGIC ('ENTREGUE_FLASH','Entrega registrada via RT','Entregue'),
# MAGIC ('ENTREGUE_FLASH','Em arquivo-aguardando Postagem','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Entrega NAO efetuada(RT)','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Habilitado para Reenvio','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','HAWB - Dados cadastrados','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','null','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','OBJETO Recebido','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Postado - logistica iniciada','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Preparada para a transferencia','Erro - Avaliar'),
# MAGIC ('ENTREGUE_FLASH','Programado Nova Tentativa','Erro - Avaliar'),
# MAGIC ('ENTREGUE_TERCEIRO','Entregue pelo Terceiro','Entregue'),
# MAGIC ('ENTREGUE_TERCEIRO','null','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Em arquivo-aguardando Postagem','Aguardando postagem'),
# MAGIC ('PENDENTE_FLASH','Pendencia corrigida','Aguardando postagem'),
# MAGIC ('PENDENTE_FLASH','Devol. Protocolada - FRANQUIA','Em devolução'),
# MAGIC ('PENDENTE_FLASH','Devolução Conciliada','Em devolução'),
# MAGIC ('PENDENTE_FLASH','Devolucao Recebida - Avulsa','Em devolução'),
# MAGIC ('PENDENTE_FLASH','Entrega em andamento (na rua)','Em rota de entrega'),
# MAGIC ('PENDENTE_FLASH','OBJETO Recebido','Em rota de entrega'),
# MAGIC ('PENDENTE_FLASH','Postado - logistica iniciada','Em rota de entrega'),
# MAGIC ('PENDENTE_FLASH','Preparada para a transferencia','Em rota de entrega'),
# MAGIC ('PENDENTE_FLASH','Programado Nova Tentativa','Em rota de entrega'),
# MAGIC ('PENDENTE_FLASH','Entrega NAO efetuada','Entrega não efetuada'),
# MAGIC ('PENDENTE_FLASH','Entrega NAO efetuada(RT)','Entrega não efetuada'),
# MAGIC ('PENDENTE_FLASH','Aguardando Retirada','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','AR digitalizada','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Devolução extraviada','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Devolvendo via Terceiro','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Em ligação - Telemarketing','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Envio protocolado p/ Terceiro','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Habilitado para Reenvio','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','HAWB - Dados cadastrados','Custódia Tratada'),
# MAGIC ('PENDENTE_FLASH','Preparado para Redespacho','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Redespachado por Terceiro','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','','Erro - Avaliar'),
# MAGIC ('PENDENTE_FLASH','Importacao - pendecia gerada','Inconsistência'),
# MAGIC ('PENDENTE_FLASH','Importacao - pendencia gerada','Inconsistência'),
# MAGIC ('PENDENTE_FLASH','Protocolado para Destruicao','Pendente fragmentação'),
# MAGIC ('PENDENTE_TERCEIRO','Em arquivo-aguardando Postagem','Aguardando postagem'),
# MAGIC ('PENDENTE_TERCEIRO','Pendencia corrigida','Aguardando postagem'),
# MAGIC ('PENDENTE_TERCEIRO','Aguardando Retirada','Aguardando retirada'),
# MAGIC ('PENDENTE_TERCEIRO','Postado - logistica iniciada','Em rota de entrega'),
# MAGIC ('PENDENTE_TERCEIRO','Redespachado por Terceiro','Em rota de entrega'),
# MAGIC ('PENDENTE_TERCEIRO','Devolvendo via Terceiro','Em devolução'),
# MAGIC ('PENDENTE_TERCEIRO','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','Entrega NAO efetuada(RT)','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','Habilitado para Reenvio','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','HAWB - Dados cadastrados','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','OBJETO Recebido','Erro - Avaliar'),
# MAGIC ('PENDENTE_TERCEIRO','Importacao - pendencia gerada','Inconsistência'),
# MAGIC ('SINISTRADO_FLASH','Entrega em andamento (na rua)','Erro - Avaliar'),
# MAGIC ('SINISTRADO_FLASH','Entrega NAO efetuada','Erro - Avaliar'),
# MAGIC ('SINISTRADO_FLASH','null','Erro - Avaliar'),
# MAGIC ('SINISTRADO_FLASH','Postado - logistica iniciada','Erro - Avaliar'),
# MAGIC ('SINISTRADO_FLASH','Sinistro - Cliente Avisado','Sinistrado'),
# MAGIC ('SINISTRADO_TERCEIRO','Aguardando Retirada','Erro - Avaliar'),
# MAGIC ('SINISTRADO_TERCEIRO','Sinistrado pelo Terceiro','Sinistrado')
# MAGIC   as temp_table(baixa, ultimo_status, novo_status)

# COMMAND ----------

status_flash_df = spark.table("status_flash") 

# COMMAND ----------

# DBTITLE 1,Tudo junto
new_card_tracking = (
  spark.table("card_operations.tracking_card_delivery")
  .filter((F.col("collect_date") >= F.current_date() - F.expr("interval 14 month")) | #⏳⏳⏳⏳
          (F.col("collect_date").isNull())) 
    .drop("tracking_card_delivery_id")
)

card_tracking = (
    new_card_tracking
    .withColumn("hawb_code",
        F.when(F.col("delivery_company") == "MOBI", F.col("ar_code"))
        .otherwise(F.col("hawb_code").cast("long"))
    )
    .withColumn("first_hawb_code", F.min("hawb_code").over(Window.partitionBy("ar_code")))
    .withColumn("delivery_type",
        F.when(F.col("last_status") == "Tratativa entregar", "Reenvio")
         .when(F.col("hawb_code") == F.col("first_hawb_code"), "Primeiro envio")
        .otherwise("Reenvio")
    )

    .filter("upper(last_status) <> 'FERIADO'")
    .filter(~(
        F.col("file_name").contains("TRACKING_PICPAY_30122024_001.csv") |
        F.col("file_name").contains("TRACKING_PICPAY_30122024_002.csv") |
        F.col("file_name").contains("TRACKING_PICPAY_23122024_135122_1.csv") |
        F.col("file_name").contains("TRACKING_PICPAY_23122024_135122_3.CSV") |
        F.col("file_name").contains("TRACKING_PICPAY_14122024_012827.CSV")
    ))
)

card_tracking_ordered = (
  card_tracking
    .withColumn("first_post_date", F.min("post_date").over(Window.partitionBy("ar_code")))
    .withColumn("first_collect_date", F.min("collect_date").over(Window.partitionBy("ar_code")))

    .withColumn("rn_last_status",
      F.row_number()
       .over(Window.partitionBy("ar_code")
             .orderBy(F.desc("last_status_at"), F.desc("delivery_date"), "last_status", F.desc("return_reason"))))
    .filter("rn_last_status = 1")
)

couriers = (
  card_tracking_ordered
    # Ajustes por conta da Flash não enviar a data de entrega ou enviar status diferentes de entrega após o cartão ser entregue
    .join(status_mobi_df.alias("status_mobi"), F.col("last_status") == F.col("status_mobi.ultimo_status"), "left")
    .join(status_flash_df.alias("status_flash"), (F.col("last_status") == F.col("status_flash.ultimo_status")) & (F.col("discharge_type") == F.col("status_flash.baixa")), "left")

    .withColumn("delivery_date",
       F.when(F.col("discharge_type") == "ENTREGUE_TERCEIRO", F.to_date(F.col("last_status_at")))
         .when((F.col("discharge_type").like("%ENTREGUE%")) & F.col("delivery_date").isNull(), F.col("discharge_date"))
         .otherwise(F.col("delivery_date"))
      )
    .withColumn("delivery_date", F.to_date("delivery_date"))
    
    .withColumn("status_delivery",
      F.expr("""
        case
          -- Flash
          when delivery_date is not null then 'Entregue'

          when last_status = status_flash.ultimo_status and discharge_type = status_flash.baixa then status_flash.novo_status

          when discharge_type like '%DESTRUIDO%' then 'Fragmentado'
          when discharge_type like '%CUSTODIA%' then 'Custódia'
          when discharge_type like '%DEVOLVIDO%' then 'Em devolução'
          when discharge_type like '%ENTREGUE%' and delivery_date is null then 'Entregue - sem data'
          when upper(discharge_type) like '%SINISTRADO%' then 'Sinistrado'
          
          when delivery_company = 'FLASH'
            and discharge_type = ''
            and delivery_date is null
          then 'Entregue - sem data'
          
          when discharge_type in ('CANCELADO_FLASH', 'CANCELADO_TERCEIRO', 'SINISTRO_FLASH', 'SINISTRO_TERCEIRO') then discharge_type
          when last_status = 'Em arquivo-aguardando Postagem' or post_date is null then 'Aguardando postagem'
          when discharge_type like '%ENTREGUE%' then 'Em rota de entrega'
          when discharge_type like '%PENDENTE%' then 'Em rota de entrega'
          
          when discharge_type like '%REENVIADO%' then 'Reenviado - em rota'
          when length(trim(discharge_type)) = 0 and delivery_company = 'FLASH' then 'Em rota de entrega'

          -- Mobi
          when last_status = status_mobi.ultimo_status then status_mobi.novo_status
          when upper(discharge_type) = 'EM TRATATIVA' then 'Custódia'
          when upper(discharge_type) = 'FRAGMENTADO' then 'Fragmentado'
          
          when discharge_type is null then 'Avaliar'
          else discharge_type
        end
      """))
    # .join(de_para_return_flash, "return_reason", "left")
   
    .select(
      "ar_code",
      "delivery_company",
      "first_collect_date",
      "first_post_date",
      "collect_date",
      "post_date",
      "delivery_date",
      "delivery_type",
      "product_name",
      F.col("last_status_at").alias("last_status_delivery_at"),
      "status_delivery",
      "discharge_type",
      "return_reason",
      "last_status",
    )
    .distinct()
)
# display(couriers.filter("ar_code = 814438836"))
couriers.createOrReplaceTempView("couriers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Fragmentação FAC
# MAGIC

# COMMAND ----------

fragmentacao = spark.sql("""
  select
    'Mobi' as canal,
    substr(cif_code, 1, 34) as cif,
    file_date as processing_date,
    return_reason_code,
    case
      when return_reason_code = 01 then 'Mudou-se'
      when return_reason_code = 02 then 'Endereço insuficiente'
      when return_reason_code = 03 then 'Não existe o número indicado'
      when return_reason_code = 04 then 'Falecido'
      when return_reason_code = 05 then 'Desconhecido'
      when return_reason_code = 06 then 'Recusado'
      when return_reason_code = 07 then 'Ausente'
      when return_reason_code = 08 then 'Não procurado'
      when return_reason_code = 09 then 'Outros'
      when return_reason_code = 10 then 'Objeto Danificado'
      when return_reason_code = 11 then 'Endereço Desconhecido na Localidade'
      when return_reason_code = 12 then 'Falta Complemento'
      when return_reason_code = 13 then 'Caixa Postal Cancelada'
      when return_reason_code = 14 then 'Entrega Controlada'
      else 'N/D'
    end return_reason_desc,
    rank() over (partition by substr(cif_code, 1, 34) order by file_date desc) as rk
  from card.fragmentation_card_deliveries
  where post_date between current_date() - interval 14 month and current_date() --⏳⏳⏳⏳
""").filter("rk = 1")
fragmentacao.createOrReplaceTempView("fragmentacao")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Embossadoras

# COMMAND ----------

# DBTITLE 1,INSS
benefit_embossing_processes = spark.table("benefits.embossing_processes")
benefit_embossing_expeditions = spark.table("benefits.embossing_expeditions")

new_embossing_proc = (
  spark.table("benefits.embossing_processes")
    .drop("id", )
    .filter((F.col("processed_at") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳

    .withColumn("embossing_status", F.lit("Processado"))
    .withColumn("origem", F.lit("new_embossing_proc"))
    .withColumn("expedition_at", F.lit(""))
)

new_embossing_disp = (
  spark.table("benefits.embossing_expeditions")
    .filter((F.col("processed_at") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳
    .drop("id")

    .withColumn(
        "embossing_status",
        F.when(F.col("embossing_finisher_status") == "E", "Expedido")
        .when(F.col("embossing_finisher_status") == "R", "Rejeitado no processamento embossadora")
        .when(F.col("embossing_finisher_status") == "A", "Agilizado por solicitação do Cliente")
        .when(F.col("embossing_finisher_status") == "X", "Retido por solicitação do Cliente")
        .otherwise(F.lit("Expedido"))
    )
    .withColumn("origem", F.lit("new_embossing_disp"))
    .drop("embossing_finisher_status", "rejection_reason")
)

new_embossing_wo_dup_g = (
  new_embossing_proc.unionByName(new_embossing_disp)
    .withColumnRenamed("processed_at", "embossing_processed_date")
    .withColumnRenamed("expedition_at", "embossing_expedition_date")

    .withColumn("embossing_rejection_reason", F.lit(""))
    .withColumn("ar_code_transformed", when(col("ar_code").rlike("[A-Za-z]"), col("ar_code")).otherwise(col("ar_code").cast("long")))

    .orderBy("ar_code", "origem", F.desc("embossing_expedition_date"), F.desc("embossing_processed_date"))
    .dropDuplicates(["ar_code"])
    .distinct()
)

new_embossing = (
  new_embossing_wo_dup_g
    .withColumn("embossing_processed_date", F.date_format("embossing_processed_date", "yyyy-MM-dd"))
    .withColumn("embossing_expedition_date", F.date_format("embossing_expedition_date", "yyyy-MM-dd"))
    .withColumn("embossing_company",
      F.expr("""
        case
          when embossing_factory = 'JSBC' then 'JallCard'
          when embossing_factory = 'JCWB' then 'JallCard'
          when embossing_factory = 'VSOR' then 'Valid'
          when embossing_factory = 'TBAR' then 'Thales'
        end
      """))
    .withColumn("plastic_code", F.lit(""))
    .withColumn("cif_code", F.lit(""))
    .withColumn("post_card_code", F.lit(""))
    .withColumn("lot_cif", F.lit(""))
    .withColumn("expedition_channel", F.lit("delivery_product"))
    .select(
      "org",
      "embossing_company",
      "ar_code",
      # "original_ar_code",
      "plastic_code",
      "embossing_status",
      "embossing_processed_date",
      "embossing_expedition_date",
      "embossing_factory",
      "expedition_channel",
      "cif_code",
      "post_card_code",
      "lot_cif",
      F.lit("new").alias("origem"),
      # "embossing_final_status",
      "embossing_rejection_reason",
      "ar_code_transformed",
    )
)

# # DF consolidado, onde remove duplicidades, dando preferência pelo uso da tabela manual
embossing_prod_benef = (
  new_embossing
    .withColumn("ar_code",
      F.expr("case when substr(ar_code, 1, 1) = '0' then substr(ar_code, 2, length(ar_code)) else ar_code end"))
    .orderBy("ar_code", F.desc("origem"))
    .dropDuplicates(["ar_code"])
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Jall, Thales e Valid
new_embossing_proc = (
  spark.table("card.embossing_processes")
    .drop("embossing_process_id")
    .filter((F.col("processed_at") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳

    .withColumn(
        "embossing_status",
        # F.when(F.col("embossing_finisher_status") == "E", "Expedido")
        F.when(F.col("embossing_finisher_status") == "R", "Rejeitado no processamento embossadora")
        .when(F.col("embossing_finisher_status") == "A", "Agilizado por solicitação do Cliente")
        .when(F.col("embossing_finisher_status") == "X", "Retido por solicitação do Cliente")
        .otherwise(F.lit("Processado"))
    )

    .withColumn("origem", F.lit("new_embossing_proc"))
    .withColumn("expedition_at", F.lit(""))
)

new_embossing_disp = (
  spark.table("card.embossing_expeditions")
    .filter((F.col("processed_at") >= F.current_date() - F.expr("interval 14 month"))) #⏳⏳⏳⏳
    .filter("expedition_at is not null") # @new regra para considerar somente o que foi expedido, tirando os rejeitados (https://picpay.atlassian.net/browse/FMP-1614)
    .drop("embossing_expedition_id")

    .withColumn(
        "embossing_status",
        F.when(F.col("embossing_finisher_status") == "E", "Expedido")
        .when(F.col("embossing_finisher_status") == "R", "Rejeitado no processamento embossadora")
        .when(F.col("embossing_finisher_status") == "A", "Agilizado por solicitação do Cliente")
        .when(F.col("embossing_finisher_status") == "X", "Retido por solicitação do Cliente")
        .otherwise(F.lit("Expedido"))
    )
    .withColumn("origem", F.lit("new_embossing_disp"))
)

new_embossing_wo_dup = (
  new_embossing_proc
    .unionByName(new_embossing_disp)

    .withColumnRenamed("processed_at", "embossing_processed_date")
    .withColumnRenamed("expedition_at", "embossing_expedition_date")
    .withColumn(
        "embossing_rejection_reason",
        F.when(F.col("rejection_reason") == "1", "CEP Inválido")
        .when(F.col("rejection_reason") == "2", "Código ou Card ID do Produto Inválido")
        .when(F.col("rejection_reason") == "3", "Courier Inválida")
        .when(F.col("rejection_reason") == "4", "Modelo divergente do Plástico x Logo")
        .otherwise(F.lit(None).cast(F.StringType()))
    )
    .withColumn("ar_code_transformed", when(col("fopag_tracking_number").isNull(), col("ar_code").cast("long")).otherwise(col("fopag_tracking_number")))

    .orderBy("ar_code", "origem", F.desc("embossing_expedition_date"), F.desc("embossing_processed_date"))
    .dropDuplicates(["ar_code"])
    .distinct()
)

new_embossing = (
  new_embossing_wo_dup
    .withColumn("embossing_processed_date", F.date_format("embossing_processed_date", "yyyy-MM-dd"))
    .withColumn("embossing_expedition_date", F.date_format("embossing_expedition_date", "yyyy-MM-dd"))
    .withColumn("embossing_company",
      F.expr("""
        case
          when embossing_factory = 'JSBC' then 'JallCard'
          when embossing_factory = 'JCWB' then 'JallCard'
          when embossing_factory = 'VSOR' then 'Valid'
          when embossing_factory = 'TBAR' then 'Thales'
        end
      """))
    .select(
      "org",
      "embossing_company",
      "ar_code",
      # "original_ar_code",
      "plastic_code",
      "embossing_status",
      "embossing_processed_date",
      "embossing_expedition_date",
      "embossing_factory",
      "expedition_channel",
      "cif_code",
      "post_card_code",
      "lot_cif",
      F.lit("new").alias("origem"),
      # "embossing_final_status",
      "embossing_rejection_reason",
      "ar_code_transformed",
    )
)

# DF consolidado, onde remove duplicidades, dando preferência pelo uso da tabela manual
embossing_prod = (
  new_embossing
    .withColumn("ar_code",
      F.expr("case when substr(ar_code, 1, 1) = '0' then substr(ar_code, 2, length(ar_code)) else ar_code end"))
    .orderBy("ar_code", F.desc("origem"))
    .dropDuplicates(["ar_code"])
    .distinct()
)

# display(embossing_prod.filter("ar_code = 'PI2025051900058FPJ'"))

# COMMAND ----------

# DBTITLE 1,Tratamento do legado
tmp_delivery = couriers.select("ar_code", "first_collect_date")
tmp_fis = fis_processed.select("ar_code", "fis_processing_date")

embossing_out = (
  # embossing_prod.alias("a")
  embossing_prod
  .unionByName(embossing_prod_benef).alias("a")
  
    .join(tmp_delivery.alias("b"), "ar_code", "left")
    .join(tmp_fis.alias("c"), "ar_code", "left")
    .withColumn("diff_collect", F.datediff("b.first_collect_date", F.to_date("a.embossing_processed_date")))
    .withColumn("diff_fis", F.datediff("b.first_collect_date", "c.fis_processing_date"))
    
    .withColumn("embossing_expedition_date",
      F.expr("""
        case
          when  a.embossing_status = 'Processado' and date(a.embossing_expedition_date) < '2022-06-15'
            and a.embossing_company = 'Valid' and diff_collect < 0
            then date_add(c.fis_processing_date, 1)

          when a.embossing_status = 'Processado' and date(a.embossing_expedition_date) < '2022-06-15'
            and a.embossing_company = 'Valid' and b.first_collect_date is not null
            then date_add(b.first_collect_date, -1)

          when  a.embossing_status = 'Processado' and date(a.embossing_expedition_date) < '2022-04-01'
            and a.embossing_company = 'JallCard' and diff_collect < 0
            then date_add(c.fis_processing_date, 1)

          when a.embossing_status = 'Processado' and date(a.embossing_expedition_date) < '2022-04-01'
            and a.embossing_company = 'JallCard' and b.first_collect_date is not null
            then date_add(b.first_collect_date, -1)

          else a.embossing_expedition_date
        end
      """))
   
    .withColumn("embossing_processed_date",
            F.expr("""
            case
              when  a.embossing_status = 'Processado' and date(embossing_expedition_date) < '2022-06-15'
                and a.embossing_company = 'Valid' and diff_collect < 0
                then c.fis_processing_date

              when  a.embossing_status = 'Processado' and date(embossing_expedition_date) < '2022-04-01'
                and a.embossing_company = 'JallCard' and diff_collect < 0
                then c.fis_processing_date

              else a.embossing_processed_date
            end
            """))
    
    # .withColumn("embossing_status",
    #             F.when(F.col("embossing_expedition_date").isNotNull(), F.lit("Expedido"))
    #             .otherwise(F.col("embossing_status")))

    .withColumn("embossing_status",
                F.when(
                  F.col("embossing_expedition_date").isNull() & F.col("embossing_rejection_reason").isNotNull(),
                  F.col("embossing_rejection_reason"))
                .when(
                  F.col("embossing_status").isNotNull(), F.col("embossing_status")
                )
                .when(
                  F.col("embossing_expedition_date").isNotNull(), F.lit("Expedido")
                ).otherwise(F.col("embossing_status")))
    
    .drop("collect_date", "fis_processing_date", "first_collect_date")
    .distinct()
)

# display(embossing_out.filter("ar_code = 652606450"))
embossing_out.createOrReplaceTempView("embossadoras")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bases complementares

# COMMAND ----------

# DBTITLE 1,Consumers
consumers = (
  spark.table("consumers.consumers")
    .select("consumer_id", "cpf", "age")
)
consumers.createOrReplaceTempView("consumers")

# COMMAND ----------

# DBTITLE 1,Bloqueio
# Quantidade de bloqueios distintos já aplicados
distinct_blocks = (
  spark.table("card_operations.fis_updates_history")
    .filter("cardholder_code = 2 or organization_id = 212")
    .withColumnRenamed("card_last4", "card_last4")
    .groupBy("account_number", "card_last4")
    .agg(F.countDistinct("card_block").alias("qtd_blocks"))
)

# Histórico de bloqueios do cartão
block_history = (
  spark.table("card_operations.fis_updates_history")
    .filter("cardholder_code = 2 or organization_id = 212")
    .withColumnRenamed("card_last4", "card_last4")
    
    .join(distinct_blocks, ["account_number", "card_last4"], "left")
    
    .select(
      "account_number",
      "card_last4",
      "card_block",
      "card_block_date",
      "reference_date",
      "qtd_blocks"
    )

    .withColumn("lag_card_block",
      F.lag("card_block").over(Window.partitionBy("account_number", "card_last4")
       .orderBy("account_number", "card_last4", "card_block_date", "reference_date")))
    
    .withColumn("is_current_block",
      F.rank().over(Window.partitionBy("account_number", "card_last4")
              .orderBy("account_number", "card_last4", F.desc("card_block_date"), F.desc("reference_date"))))
    
    .filter("""
        1=1
      and (lag_card_block = 'T' or lag_card_block is null)
      and (coalesce(lag_card_block, 'nulo') <> coalesce(card_block, 'nulo'))
      or is_current_block = 1
    """)

    .persist()
)


# Data de desbloqueio
ashi_unblock = (
  spark.table("card_operations.fis_users_events")
    .filter("action_code = 'DBLK' and trim(ashi_line2) = 'UNBLOCKING PHYSICAL CARD'")
    .withColumn("card_last4", F.expr("right(trim(ashi_line1), 4)"))
    .select("account_number",
            "card_last4",
            F.to_date(F.col("created_at")).alias("card_unblock_date"),
            F.lit(1).alias("rk_preference")
            )
)
updates_unblock = (
  block_history
    .filter("lag_card_block = 'T' and card_block_date >= '2022-11-01' and card_block is null")
    .withColumn("rk",
      F.rank().over(Window.partitionBy("account_number", "card_last4")
              .orderBy("account_number", "card_last4", "card_block_date", "reference_date")))
    .filter("rk = 1")
    .select("account_number", "card_last4", "card_block_date")
    .withColumnRenamed("card_block_date", "card_unblock_date")
    .withColumn("rk_preference", F.lit(2))
)

# Data de desbloqueio
unblock_date = (
  ashi_unblock.unionByName(updates_unblock)
  .withColumn("rn_unblock", F.row_number().over(Window.partitionBy("account_number", "card_last4").orderBy("rk_preference")))
  .filter("rn_unblock = 1")
  .drop("rn_unblock", "rk_preference")
)

# display(unblock_date)

# Bloqueio atual da via
# Após refatoração, utilizar a ppcard_accounts_status
current_block = (
  block_history
    .filter("is_current_block = 1")
    .select("account_number", "card_last4", F.col("card_block").alias("current_block"))
)


# Cartões que não podem mais serem usados mesmo se entregues
# Saiu de um bloqueio para outro sem passar pelo desbloqueio. Ex.: T -> E
card_non_unblockable = (
  block_history
    .withColumn("was_card_discarted",
      F.expr("""
        case
          when (lag_card_block = 'T'
              and (card_block is not null and card_block <> 'T'))
            or (lag_card_block is null
              and card_block in ('U', 'P', 'R', 'L') and card_block_date >= '2022-12-01' and qtd_blocks = 1)
            then True
          else False
        end """))
    .filter("was_card_discarted")
    .select("account_number", "card_last4", "was_card_discarted")
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,First card
cardholder_first_card = (
  spark.table("card_operations.fis_updates_history")
    .filter("cardholder_code = 2 or organization_id = 212")
    .groupBy("account_number")
    .agg(F.min(F.col("card_last4").cast("int")).alias("cardholder_first_card"))
    .withColumn("cardholder_first_card", F.lpad("cardholder_first_card", 4, "0"))
)

cardholder_first_card.createOrReplaceTempView("cardholder_first_card")

# COMMAND ----------

# DBTITLE 1,Transação
ppcard_transactions = spark.sql("""
with debito as (
  select distinct
    'Débito' as transaction_type,
    b.cpf,
    card_last4 as card_last4,
    date(completed_at) as transaction_date
  from card.card_debit_authorizations as a
  left join consumers as b
    on a.consumer_id = b.consumer_id
  where 1=1
    and transaction_type = 'PURCHASE'
    and status = 'SUCCESS'
)

, credito as (
  select distinct
    'Múltiplo' as transaction_type,
    b.cpf,
    card_last4 as card_last4,
    date(authorization_at) as transaction_date
  from card_operations.fis_authorizations as a
  left join consumers as b
    on a.consumer_id = b.consumer_id
  where 1=1
    and authorization_request_code = 'A'
    and authorization_type = 'A'
    and message_type not in ('065', '086', '254')
)

, ppcard_transactions as (
  select * from debito
  union all
  select * from credito
)

select
  cpf,
  card_last4,
  case
    when min(transaction_date) is not null then True
    when min(transaction_date) is null then false
    else False
  end was_activated,
  min(transaction_date) as first_transaction_date,
  max(transaction_date) as last_transaction_date
from ppcard_transactions
where date(transaction_date) between current_date() - interval 14 month and current_date()
group by 1,2
order by cpf, card_last4
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Montagem da Base

# COMMAND ----------

# DBTITLE 1,Tabela Base
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

base_analitica_df = spark.sql("""
select distinct
--   pp_fis.org,
  case
    when pp_fis.org = '211' then 'PicPay PF'
    when pp_fis.org = '212' and pp_fis.logo in ('501', '502', '504', '507', '509', '513', '515', '519', '512', '510', '516', '524') then 'Consignado Benefício'
    when pp_fis.org = '212' and pp_fis.logo in ('503', '505', '506', '508', '514', '520', '511', '517') then 'Consignado'
    when pp_fis.org = '42422253000101' then 'INSS Vale+'
    when pp_fis.org = '212' then 'Original PF'
    else 'Não identificado'
  end as segment_base,
  case
    when pp_fis.org = '211' then 'PicPay'
    when pp_fis.org = '212' then 'Original'
    when pp_fis.org = '42422253000101' then 'Original'
    else 'Não identificado'
  end as org_name,
  pp_fis.consumer_id,
  case
    when pp_fis.ppcard_product is null and pp_fis.org = '212' and pp_fis.logo in ('501', '502', '504', '507', '509', '513', '515', '519', '512', '510', '516') then 'Benefício'
    when pp_fis.ppcard_product is null and pp_fis.org = '212' and pp_fis.logo in ('503', '505', '506', '508', '514', '520', '511', '517') then 'Consignado'
    else pp_fis.ppcard_product
  end as account_type,
  consumer.cpf,
  pp_fis.ar_code as fis_ar_code,
  emb.ar_code_transformed as ar_code,
  emb.cif_code,
  pp_fis.account_number,
  
  case
    when pp_fis.org = '212' and pp_fis.logo not in ('011', '051', '101', '151', '201', '203', '01D', '11D') then pp_fis.fis_reference_date
    when pp_fis.issue_reason = '1' or pp_fis.issue_reason is null then (pp_fis.card_acquired_at)
    else pp_fis.fis_reference_date
  end as card_acquired_at,

  case
    when pp_fis.card_modality in ('04') or pp_fis.is_card_dependent -- adicional
      then False
    when (pp_fis.issue_reason = '1' or pp_fis.ar_code is null) -- venda
      and pp_fis.card_last4 = fc.cardholder_first_card -- primeiro cartão emitido
      then True 
    when  (pp_fis.issue_reason = '1' or pp_fis.ar_code is null) -- venda
      and pp_fis.origin = 'DEBIT' and pp_fis.debit_acquired_at is null and date_diff(pp_fis.credit_acquired_at, pp_fis.card_account_opening_at) >= 5 -- vendas de débito sem marcação na base original
      then False
    when pp_fis.issue_reason <> '1' then False
    else True
  end as is_cardholder_sale_with_issue,
  pp_fis.is_upgrade,
  pp_fis.card_account_opening_at as fis_account_created_at,
  pp_fis.fis_reference_date,
  pp_fis.fis_processing_date,
  pp_fis.logo,
  case
    when pp_fis.org = '212' and pp_fis.logo in ('501', '502', '503', '504', '505') then 'Gold'
    when pp_fis.org = '42422253000101' then 'Gold'
    
    when pp_fis.org = '211' and pp_fis.logo in ('002', '005', '006', '007', '008') then 'Gold'
    when pp_fis.org = '211' and pp_fis.logo = '152' then 'Platinum'
    when pp_fis.org = '211' and pp_fis.logo = '102' then 'Black'
    when pp_fis.org = '211' and pp_fis.logo = '012' then 'Epic'
    when pp_fis.org = '211' and pp_fis.logo is null and pp_fis.card_variant is not null then pp_fis.card_variant -- userbase

    -- logo 001 Original é o que?
    when pp_fis.org = '212' and pp_fis.logo in ('051') then 'Gold'
    when pp_fis.org = '212' and pp_fis.logo = '151' then 'Platinum'
    when pp_fis.org = '212' and pp_fis.logo = '101' then 'Black'
    when pp_fis.org = '212' and pp_fis.logo = '011' then 'Internacional'
    when pp_fis.org = '212' and pp_fis.logo in ('201', '203') then 'Empresa'

    else 'Não identificado'
  end as variant,
  case 
    when pp_fis.org = '212' and pp_fis.logo in ('501', '502', '504') then pp_fis.logo
    when pp_fis.org = '212' and pp_fis.logo in ('503', '505') then pp_fis.logo
    when pp_fis.org = '42422253000101' then '1996' -- Número aleatório para termos mapeado
    else pp_fis.card_modality
  end as card_modality,
  case 
    when pp_fis.card_modality in ('01', '03', '12', '24') then 'Débito'
    when pp_fis.card_modality in ('11', '14', '15') then 'Limite Garantido'
    when pp_fis.card_modality in (
      '02', '04', '05', '09', '10', '13', '16', '17', '18', '19',
      '20', '21', '22', '23', '25', '26', '27', '28', '29', '30', '31', '35') then 'Múltiplo'
    when pp_fis.org = '42422253000101' then 'Crédito'
    else 'Não identificado'
  end as card_product,
  case
    when pp_fis.org = '211' and pp_fis.ppcard_product = 'Small Limits' then pp_fis.ppcard_product -- userbase

    when pp_fis.card_modality in ('12') then 'Débito Underage (até 15)'
    -- ajuste seguindo o card https://picpay.atlassian.net/browse/FMP-1652
    when pp_fis.card_modality in ('01', '03') and (consumer.age <= 15) then 'Débito Underage (até 15)'
    when pp_fis.card_modality in ('01', '03') and (consumer.age <= 18) then 'Débito Underage (16-18)'
    
    when pp_fis.card_modality in ('01') then 'Débito'
    when pp_fis.card_modality in ('03') and pp_fis.original_issue_reason = '1' and pp_fis.org <> '212' then 'Débito Pagante'
    when pp_fis.card_modality in ('03') then 'Débito Exceção'
    when pp_fis.card_modality in ('24') then 'Débito Braille'
    
    when pp_fis.card_modality in ('11', '14') then 'Limite Garantido'
    -- when pp_fis.card_modality in ('05') then 'Múltiplo LOL'
    when pp_fis.card_modality in ('02', '05', '09', '10', '16', '18') then 'Múltiplo'

    when pp_fis.card_modality in ('25', '26', '27') then 'Múltiplo Braille'

    when pp_fis.card_modality in ('13') then 'Small Limits'

    when pp_fis.card_modality in ('20') then 'Múltiplo <3k'
    when pp_fis.card_modality in ('21') then 'Múltiplo >3k'
    when pp_fis.card_modality in ('22') then 'Adicional Múltiplo <3k'
    when pp_fis.card_modality in ('23') then 'Adicional Múltiplo >3k'

    when pp_fis.card_modality in ('04', '15', '17', '19') then 'Adicional'

    -- EPIC
    when pp_fis.card_modality = '28' then 'Veneer'
    when pp_fis.card_modality = '29' then 'PVC'
    when pp_fis.card_modality = '30' then 'Adicional Veneer'
    when pp_fis.card_modality = '31' then 'Adicional PVC'

    -- Legend
    when pp_fis.card_modality = '36' then 'Veneer'
    when pp_fis.card_modality = '37' then 'Adicional Veneer'

    -- Teste THALES
    when pp_fis.card_modality in ('33', '34', '35') then 'Teste THALES'

    when pp_fis.org = '42422253000101' then 'Crédito - INSS Vale+' 
    
    else 'Não identificado'
  end as card_model,
  pp_fis.bin,
  pp_fis.card_last4,
  case
    when pp_fis.issue_reason = '1' then 'Primeira via'
    when pp_fis.issue_reason = '3' then 'Reemissão'
    when pp_fis.issue_reason = '5' then 'Reemissão'
    when pp_fis.issue_reason = '7' then 'Renovação Automática'
    when pp_fis.issue_reason is null then 'Reemissão'
  end as issue_reason_name,
  case when pp_fis.org = '211' and pp_fis.logo = '012' then 'JallCard' else pp_fis.embossing_name end as embossing_company,
  emb.embossing_factory,
  emb.embossing_status,
  cast(emb.embossing_processed_date as date) as embossing_processed_date,
  cast(emb.embossing_expedition_date as date) as embossing_expedition_date,
  emb.embossing_rejection_reason,
  case
    when pp_fis.courier_name in ('Mobi', 'Speedflow') then pp_fis.courier_name
    when couriers.product_name in ('Doc', 'Flash Vermelho', 'Flash - Correio') then 'Flash'
    when upper(emb.expedition_channel) in ('99C') then 'Correios'
    when upper(emb.expedition_channel) in ('17F', '17C') then 'Flash'
    else pp_fis.courier_name
  end as delivery_company,
--   emb.expedition_channel,
  case
    when pp_fis.courier_name in ('Speedflow') then pp_fis.courier_name
    when couriers.product_name in ('Doc', 'Flash Vermelho') then 'Flash'
    when couriers.product_name in ('Flash - Correio') then 'Flash - Redespacho'
    when couriers.discharge_type like '%TERCEIRO%' then 'Flash - Redespacho'
    when couriers.product_name in ('Flex - Correios') then 'Mobi - Redespacho'
    when upper(emb.expedition_channel) in ('99C') then 'Correios'
    when upper(emb.expedition_channel) in ('17F') then 'Flash'
    when upper(emb.expedition_channel) in ('17C') then 'Flash - Redespacho'
    else pp_fis.courier_name
  end as delivery_product,
  
  case
    when trim(pp_fis.state) in ('DF','GO','MT','MS') then 'Centro-Oeste'
    when trim(pp_fis.state) in ('AL','BA','CE','MA','PB','PE','PI','RN','SE') then 'Nordeste'
    when trim(pp_fis.state) in ('AC','AP','AM','PA','RO','RR','TO') then 'Norte'
    when trim(pp_fis.state) in ('ES','MG','RJ','SP') then 'Sudeste'
    when trim(pp_fis.state) in ('PR','RS','SC') then 'Sul'
    else 'Não identificado'
    end as region,
  case when pp_fis.state is null then 'Não identificado' else pp_fis.state end as state,
  case when trim(pp_fis.city) is null then 'Não identificado' else pp_fis.city end as city,

  case 
    when couriers.status_delivery = 'Entregue'   then couriers.delivery_date
    when couriers.status_delivery = 'Custódia'   then date(couriers.last_status_delivery_at)
    when couriers.status_delivery = 'Sinistrado' then date(couriers.last_status_delivery_at)
  end as finalizer_status_date,
  
  couriers.collect_date,
  couriers.post_date,
  couriers.delivery_type,
  couriers.delivery_date,
  couriers.discharge_type,
  couriers.status_delivery,
  couriers.last_status as last_status_original,
  couriers.last_status_delivery_at,
  couriers.return_reason as reasons,
--   couriers.first_collect_date,
  couriers.first_post_date,

 case
    when couriers.return_reason is not null then couriers.return_reason
    when couriers.return_reason is null then frag.return_reason_desc
  end as return_reason,
  frag.processing_date as fragmentation_date,
  frag.return_reason_code as fragmentation_code,
  case
    when couriers.status_delivery = 'Entregue' then null
    when couriers.status_delivery = 'Enviado via Correios' then null
    when
      ((upper(emb.expedition_channel) in ('99C')) or 
      (pp_fis.courier_name = 'Correios') or
      (pp_fis.card_modality in ('01')))
      and frag.return_reason_desc is null then 'CIF não localizado'
    else frag.return_reason_desc
  end as fragmentation_reason,
  pp_fis.card_type,

  bbc.card_status

--   couriers.is_last_status_delivery
from pp_fis
left join embossadoras as emb
  on pp_fis.ar_code = emb.ar_code

left join
  (select * from fragmentacao where return_reason_code is not null) as frag
  on emb.cif_code = frag.cif

left join couriers
  on couriers.ar_code = emb.ar_code_transformed

left join consumers as consumer
  using (consumer_id)

left join cardholder_first_card as fc
  using (account_number)
  
left join benefits.benefits_cards bbc
  on pp_fis.ar_code = cast(bbc.tracking_id as bigint)

""")
# base_analitica_df.display()
# spark.sql("drop table if exists validation.cops_base_tracking_temp5")
# (base_analitica_df.write
#   .format("delta")
#   .mode("overwrite")
#   .option("overwriteSchema", "true")
#   .saveAsTable("validation.cops_base_tracking_temp5"))
base_analitica_df.createOrReplaceTempView("cops_base_tracking_temp")

#Time
# 07/07: 13m
# 16/07: 16m

# COMMAND ----------

# DBTITLE 1,Coluna: status_delivery e funil
# base_analitica = spark.table("validation.cops_base_tracking_temp5").alias("a")

base_analitica = spark.table("cops_base_tracking_temp").alias("a")

base_analitica = (
  base_analitica.alias("a")
    .withColumn("status_delivery", F.expr("""
      case
        -- when courier_code = '99' then 'Enviado via Correios'
        when delivery_company = 'Correios' then 'Enviado via Correios'
        when  status_delivery is null
          and ar_code is not null
          and post_date is null
          and (   embossing_status <> 'Expedido'
               or embossing_status is null) then 'Aguardando Embossing do Cartão'
        when  status_delivery is null
          and ar_code is not null
          and post_date is null
          and embossing_status = 'Expedido' then 'Aguardando postagem'
        else status_delivery
      end
    """))
    .withColumn("fmp_card_tracking_id", F.monotonically_increasing_id()+1)

    .join(unblock_date.alias("d"), ["account_number", "card_last4"], "left")
    .join(current_block.alias("e"), ["account_number", "card_last4"], "left")
    .join(card_non_unblockable.alias("f"), ["account_number", "card_last4"], "left")
    .withColumn("card_tracking_status",
     F.expr("""
      case
        when a.card_status = 'Bloqueado' then 'Bloqueado'
        when a.card_status = 'Ativo' then 'Desbloqueado'
        when a.card_status = 'Cartão Não Recebido' then 'Sem rastreio de bloqueio'
        when a.card_modality = '04' and e.current_block not in ('T', 'E') and f.was_card_discarted is null then 'Desbloqueado'
        when card_unblock_date is not null then 'Desbloqueado'
        when e.current_block in ('E') or f.was_card_discarted then 'Descartado'
        when e.current_block in ('T') and f.was_card_discarted is null then 'Bloqueado'
        when e.current_block not in ('T', 'E') and f.was_card_discarted is null then 'Desbloqueado'
        -- when e.current_block = ' ' and card_unblock_date is null then 'Desbloqueado (Auth <> FIS)'
        when e.current_block = ' ' then 'Desbloqueado'
        else 'Sem rastreio de bloqueio'
      end
     """))
    
    .join(ppcard_transactions, ["cpf", "card_last4"], "left")

    .withColumn("funnel_status_embossing",
      F.expr("""
        case
          when ar_code is null then '1. Não processado FIS'
          when embossing_status = 'Processado' and delivery_date is not null then '5. Expedido' -- > divergência embossadora
          when embossing_status is null then '3. Não processado Embossing'
          when embossing_status = 'Processado' then '4. Processado Embossing'
          when embossing_status = 'Expedido' then '5. Expedido'
          when ar_code is not null then '2. Processado FIS'
        end
      """))
    
        .withColumn("funnel_status_delivery",
      F.expr("""
        case
          when embossing_status is null then '1. Não processado'
          when delivery_company = 'Correios' then '8. Enviado via Correios'
          when delivery_date is not null or status_delivery like '%Entregue%' then '4. Entregue' 
          when delivery_company = 'Correios' and (card_unblock_date is not null or was_activated) then '4. Entregue'
          when status_delivery in ('Entregue', 'Entregue - sem data', 'Sucesso na acareação') then '4. Entregue'
          when post_date is null and delivery_date is null then '1. Pendente postagem'
          when status_delivery like '%Custódia%' then '5. Custódia' 

          when status_delivery = 'Fragmentado' then '6. Fragmentado'
          when delivery_company = 'Correios' and fragmentation_date is not null then '6. Fragmentado'
          when lower(status_delivery) like '%sinist%' then '7. Sinistrado' 
          when delivery_date is null then '3. Pendente entrega'  

          when post_date is null and delivery_company = 'Correios' then '8. Enviado via Correios'
          when post_date is not null then '2. Postado'
          when post_date is null and delivery_date is not null then '2. Postado' -- > divergência courier
        end
      """))
        
    .persist()
    # .drop("account_number_16pos", "auth_unblock_date")
)
base_analitica.createOrReplaceTempView("base_analitica")

# COMMAND ----------

# DBTITLE 1,Complemento: status_delivery e funil
dados_anl = spark.sql("""
	select distinct
    *,
    case 
				when status_delivery like 'Entregue%' then status_delivery
				when  delivery_company = 'Correios' and fragmentation_code is not null then 'Devolvido'
				when 	delivery_company = 'Correios'
					and fragmentation_code is null
					and card_tracking_status = 'Bloqueado' then 'Em rota de entrega'
				when 	status_delivery like 'Entregue%'
					and card_tracking_status = 'Sem rastreio de bloqueio' then 'Entregue (outros)'
				else card_tracking_status
			end as card_tracking_substatus
  from base_analitica
""")

dados_anl1 = (
  dados_anl

		.withColumn("substatus_delivery",
			F.expr("""
				case 
					when status_delivery = 'Entregue' and card_tracking_status = 'Bloqueado' then 'Entregue - Bloqueado'
          when status_delivery = 'Entregue' and card_tracking_status = 'Desbloqueado' then 'Entregue - Desbloqueado'
          when status_delivery = 'Entregue' and card_tracking_status = 'Descartado' then 'Entregue - Descartado'
					when status_delivery = 'Entregue'
     				and (card_tracking_substatus not in ('Desbloqueado', 'Entregue - Sem data', 'Entregue (outros)'))
						then concat('Entregue', ' - ', card_tracking_substatus)
      
					when card_tracking_status like 'Entregue%'
        		and (delivery_type = 'Primeiro envio') then 'Entregue Direto'
          
          when card_tracking_status like 'Entregue%'
            and delivery_type = 'Reenvio' then 'Entregue Reenvio'
            
          when card_tracking_status = 'Bloqueado' and status_delivery = 'Descartado' then 'Descartado'
          
          
					else card_tracking_substatus
				end
   		"""))
)

base_analitica.createOrReplaceTempView("base_analiticas")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Visão de custódia

# COMMAND ----------

# DBTITLE 1,Mobi
base_mobi = spark.sql('''
with
base_geral_custodia as (
select
  ar_code,
  return_reason,
  last_status_at,
  row_number() over (partition by ar_code order by last_status_at) as rn
from card_operations.tracking_card_delivery
where delivery_company = 'MOBI'
  and date(collect_date) between current_date() - interval 14 month and current_date() --⏳⏳⏳⏳
  and last_status = 'Em tratativa'
  and last_status_at is not null
),

base_historica_custodia as (
select
    *,
    return_reason as first_reason,
    date(last_status_at) as custody_start_date
  from base_geral_custodia
  where rn = 1
),

base_atual_custodia as (
  select
    ar_code,
  last_status_delivery_at as custody_last_date,
  return_reason as custody_current_reason
  from base_analitica
  where funnel_status_delivery = '5. Custódia'
    and delivery_company in ('Mobi', 'Mobi - Redespacho')
)

select
  case
    when a.ar_code is not null then a.ar_code
    when a.ar_code is null then b.ar_code
    end as ar_code,
  case
    when c.current_block in ('E', 'F', 'G', 'M', 'N', 'P', 'R', 'U', 'Y') then "Bloqueio_definitivo"
    when c.current_block is not null then "Bloqueio_parcial"
  end as current_block,
  a.custody_start_date,
  first_reason,
  date(b.custody_last_date) as custody_current_date,
  custody_current_reason
from base_atual_custodia b
full outer join base_historica_custodia a
  on b.ar_code = a.ar_code
left join base_analitica c
  on a.ar_code = c.ar_code
left join (select ar_code from card_operations.fis_embossings where embossing_code in ('2', 'J')) d
  on a.ar_code = d.ar_code
where 1=1
''')
base_mobi.createOrReplaceTempView("base_mobi")

# COMMAND ----------

# DBTITLE 1,Flash
base_flash = spark.sql('''
with
qtde_hawb as (
select distinct
  ar_code,
  count(distinct bigint(hawb_code)) as qtde
from card_operations.tracking_card_delivery
where last_status like '%Aguardando telemarketing%'
  and date(collect_date) between current_date() - interval 14 month and current_date() --⏳⏳⏳⏳
group by all
having qtde >= 1
),

rank_ar as (
select
  ar_code,
  return_reason,
  last_status_at,
  bigint(hawb_code) as hawb_code,
  row_number() over (partition by ar_code order by last_status_at asc) as rn
from card_operations.tracking_card_delivery
where 1=1
  and delivery_company = 'FLASH'
  and date(collect_date) between current_date() - interval 14 month and current_date()
  and ar_code in (select ar_code from qtde_hawb)
  and last_status like '%Aguardando telemarketing%'
  and last_status_at is not null
),

base_segmentada as (
select
  *,
  min(last_status_at) as pri_status
from rank_ar
group by all
),

base_rn as (
select
  *,
  pri_status,
  row_number () over (partition by ar_code order by pri_status) as status_order
from base_segmentada
),

base_historica_custodia as (
  select
    *,
    return_reason as first_reason,
    date(pri_status) as custody_start_date
  from base_rn
  where status_order = 1
    -- and ar_code = 625769630
),

base_atual_custodia as (
  select
    ar_code,
    last_status_delivery_at as custody_last_date,
    return_reason as custody_current_reason
  from base_analitica
  where funnel_status_delivery = '5. Custódia'
    and delivery_company in ('Flash', 'Flash - Redespacho')
)

select
  case
    when a.ar_code is not null then a.ar_code
    when a.ar_code is null then b.ar_code
    end as ar_code,
  case
    when c.current_block in ('E', 'F', 'G', 'M', 'N', 'P', 'R', 'U', 'Y') then "Bloqueio_definitivo"
    when c.current_block is not null then "Bloqueio_parcial"
  end as current_block,
  a.custody_start_date,
  first_reason,
  date(b.custody_last_date) as custody_current_date,
  custody_current_reason
from base_atual_custodia b
full outer join base_historica_custodia a
  on b.ar_code = a.ar_code
left join base_analitica c
  on a.ar_code = c.ar_code
left join (select ar_code from card_operations.fis_embossings where embossing_code in ('2', 'J')) d
  on a.ar_code = d.ar_code
where 1=1
''')

base_flash.createOrReplaceTempView("base_flash")

# COMMAND ----------

# DBTITLE 1,Union
bGeral = spark.sql("""
    select * from base_mobi
    union all 
    select * from base_flash
""")

bGeral.createOrReplaceTempView("bGeral")

# COMMAND ----------

# DBTITLE 1,Reasons
# MAGIC %sql
# MAGIC create or replace temp view reasons_custodia as
# MAGIC select * from values
# MAGIC ('Rua não localizada','Endereço'),
# MAGIC ('Reter e aguardar definição do cliente','Outros '),
# MAGIC ('Recusado pelo porteiro','Recusado Port./Dest.'),
# MAGIC ('Recusado pelo destinatário','Recusado Port./Dest.'),
# MAGIC ('Recusado (Destinat?io / Interlocutor)','Recusado Port./Dest.'),
# MAGIC ('Recusado','Recusado Port./Dest.'),
# MAGIC ('Pessoa Sem Documentação','Ausente'),
# MAGIC ('Outros','Outros '),
# MAGIC ('Objeto não entregue - prazo de retirada encerrado','Outros '),
# MAGIC ('Número não localizado','Endereço'),
# MAGIC ('Número não existe','Endereço'),
# MAGIC ('N?ero n? existe','Endereço'),
# MAGIC ('Mudou-se','Mudou-se'),
# MAGIC ('Greve/Recesso','Ausente'),
# MAGIC ('Falta o Número','Endereço'),
# MAGIC ('Falta o N?ero','Endereço'),
# MAGIC ('Falta de complemento','Endereço'),
# MAGIC ('Falta complemento','Endereço'),
# MAGIC ('Ex-Funcion?io/Tranferido','Mudou-se'),
# MAGIC ('Enviar para Custódia','Outros '),
# MAGIC ('Entregue em local errado','Outros '),
# MAGIC ('Endereço insuficiente','Endereço'),
# MAGIC ('Endereço incorreto','Endereço'),
# MAGIC ('Endereço errado/insuficiente','Endereço'),
# MAGIC ('Endereço de difícil acesso','Outros '),
# MAGIC ('Endere? errado/insuficiente','Endereço'),
# MAGIC ('Em Férias/Licença','Ausente'),
# MAGIC ('Em F?ias/Licen?','Ausente'),
# MAGIC ('Difícil Acesso','Outros '),
# MAGIC ('Dif?il Acesso','Outros '),
# MAGIC ('Devolução dos correios recebida','Outros '),
# MAGIC ('Devolução dos correios','Outros '),
# MAGIC ('Destinat?io Ausente','Ausente'),
# MAGIC ('Desconhecido (Destinat?io / Interlocutor)','Desconhecido'),
# MAGIC ('Desconhecido','Desconhecido'),
# MAGIC ('Cep incorreto','Endereço'),
# MAGIC ('Ausente 3ª tentativa','Ausente'),
# MAGIC ('Ausente','Ausente'),
# MAGIC ('Arquivo processado','Outros '),
# MAGIC ('Aguardando definição do cliente','Outros '),
# MAGIC ('A pedido do cliente','Endereço'),
# MAGIC ('null','Outros ')
# MAGIC   as temp_table(last_status_delivery, novo_status)

# COMMAND ----------

# DBTITLE 1,Union
bCustody = spark.sql("""
select 
  bGeral.ar_code,
  current_block,
  custody_start_date,
  case
    when first_reason = c.last_status_delivery then c.novo_status
    when first_reason = 'Aguardando definição' then 'Análise - Cadastro de endereçamento'
    when first_reason like '%Risco%' then 'Área de risco'
    when first_reason like '%Correios%' then 'Devolução dos correios'
    when first_reason = 'Atraso no Roteiro (Programado nova tentativa)' then 'Outros'
    else first_reason
  end as first_reason,
  custody_current_date,
  case
    when custody_current_reason = c.last_status_delivery then c.novo_status
    when custody_current_reason = 'Aguardando definição' then 'Análise - Cadastro de endereçamento'
    when custody_current_reason like '%Risco%' then 'Área de risco'
    when custody_current_reason like '%Correios%' then 'Devolução dos correios'
    when custody_current_reason = 'Atraso no Roteiro (Programado nova tentativa)' then 'Outros'
    else custody_current_reason
  end as custody_current_reason
  from bGeral
  left join cops_base_tracking_temp b
  -- left join validation.cops_base_tracking_temp5 b
    on bGeral.ar_code = b.ar_code
  left join reasons_custodia c
    on bGeral.first_reason = c.last_status_delivery
  """)

bCustody.createOrReplaceTempView("bCustody")

# COMMAND ----------

# DBTITLE 1,Colunas finais
base_analitica_with_custody = (
    dados_anl1.alias("a")
    .join(bCustody.alias("b"), ["ar_code"], "left")
    .select(
        "a.fmp_card_tracking_id",
        "a.org_name",
        "a.segment_base",
        "a.cpf",
        "a.account_number",
        "a.consumer_id",
        "a.account_type",
        "a.bin",
        "a.card_last4",
        "a.card_tracking_status",
        "a.funnel_status_embossing",
        "a.funnel_status_delivery",
        "a.fis_ar_code",
        "a.ar_code",
        "a.cif_code",
        "a.card_acquired_at",
        "a.is_cardholder_sale_with_issue",
        "a.is_upgrade",
        "a.issue_reason_name",
        "a.fis_account_created_at",
        "a.fis_reference_date",
        "a.fis_processing_date",
        "a.logo",
        "a.variant",
        "a.card_modality",
        "a.card_product",
        "a.card_model",
        "a.embossing_company",
        "a.embossing_factory",
        "a.embossing_status",
        "a.embossing_processed_date",
        "a.embossing_expedition_date",
        # "a.embossing_final_status",
        "a.embossing_rejection_reason",
        "a.delivery_company",
        "a.delivery_product",
        "a.region",
        "a.state",
        "a.city",
        "a.collect_date",
        "a.post_date",
        "a.delivery_type", 
        "a.delivery_date",
        "a.discharge_type",
        "a.status_delivery",
        "a.last_status_original",
        "a.substatus_delivery",
        "a.last_status_delivery_at",
        "a.finalizer_status_date",
        "a.return_reason",
        "a.first_post_date",
        "b.custody_start_date",
        "b.first_reason",
        "b.custody_current_date",
        "b.custody_current_reason",
        "a.card_unblock_date",
        "a.current_block",
        "a.card_type",
        "a.was_card_discarted",
        "a.was_activated",
        "a.first_transaction_date",
        "a.last_transaction_date",
    )
)

base_analitica_with_custody.createOrReplaceTempView("base_analitica_with_custody")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculo dias úteis

# COMMAND ----------

# DBTITLE 1,Configuração
INITIAL_DATE = (datetime.today() - relativedelta(months=14)).strftime("%Y-%m-%d")
END_DATE = datetime.today().strftime("%Y-%m-%d")

calendar = F.broadcast(
  spark.table("shared.calendar")
    .filter(F.col("calendar_date").between(INITIAL_DATE, END_DATE))
  )

# Data Inicial, ajusta quando começar em um dia não útil
first_dates_df = calendar.select(F.col("calendar_date").alias("start_date"))
working_days = calendar.filter("is_working_day = true").select(F.col("calendar_date").alias("working_day"))

first_dates = (
  first_dates_df
    .crossJoin(working_days)
    .filter("working_day >= start_date")
    .withColumn("rk_wd", F.row_number().over(Window.partitionBy("start_date").orderBy("working_day")))
    .filter("rk_wd = 1")
    .withColumn("was_ajusted", F.when(F.col("start_date") != F.col("working_day"), F.lit(True)).otherwise(F.lit(False)))
    .select("start_date", F.col("working_day").alias("adjusted_start_date"), "was_ajusted")
)

# Data final
end_dates = calendar.select(F.col("calendar_date").alias("end_date"))

# Dataframa com todos as datas para serem calculadas
dates = F.broadcast(first_dates.crossJoin(end_dates).orderBy("start_date", "end_date").filter("end_date >= start_date"))

# Esse modelo para trazer os dias úteis deixa escapar situações onde há dias não úteis em sequência. Por exemplo 2025-01-11	(sábado) 2025-01-12 (domingo). Nesse caso, a quantidade de dias úteis será 0

working_days = (
  dates
    .crossJoin(calendar)
    .filter("""
         (was_ajusted and calendar_date between date(adjusted_start_date) and date(end_date) and is_working_day) -- Quando ajustado, considera a data inicial no filtro
      or (was_ajusted is false and calendar_date between date_add(adjusted_start_date, 1) and date(end_date) and is_working_day) -- Quando não ajustado, adiciona um dia para não considerar no cálculo
      or (calendar_date = start_date and calendar_date = end_date)
      """)
    .groupBy("start_date", "end_date")
    .agg((F.count("*")).alias("working_days"))
    .withColumn("working_days",
      F.when(F.col("start_date") == F.col("end_date"), F.lit(0))
       .otherwise(F.col("working_days")))
    .withColumn("origin", F.lit("working_days"))
    .orderBy("start_date", "end_date")
)

# Ajusta os casos não mapeados para ficarem com 0 dias úteis
not_mapped = (
  dates
    .join(working_days, on=["start_date", "end_date"], how="left_anti")
    .withColumn("working_days", F.lit(0))
    .select("start_date", "end_date", "working_days")
    .withColumn("origin", F.lit("not_mapped"))
)

working_days_calendar = F.broadcast(
  working_days
    .unionByName(not_mapped)
    .orderBy("start_date", "end_date")
    .drop(*["origin"])
)

# display(working_days_calendar)

# COMMAND ----------

def set_working_days(df: DataFrame, start_date: str, end_date: str, return_column: str) -> DataFrame:
  return (
    df
      .withColumn("start_date", F.to_date(start_date))
      .withColumn("end_date", F.to_date(end_date))
      .join(working_days_calendar, ["start_date", "end_date"], "left")
      .withColumnRenamed("working_days", return_column)
      .drop(*["start_date", "end_date"])
  )

# COMMAND ----------

# DBTITLE 1,Criação - Colunas
base_analitica = (
  base_analitica_with_custody
    .withColumn("data_atual", F.expr("current_date()"))
    .withColumn("ontem", F.expr("current_date()-1"))

    # Visão Cliente
    .transform(set_working_days,  "card_acquired_at", "fis_processing_date", "du_customer_fis")
    .transform(set_working_days,  "card_acquired_at", "embossing_expedition_date", "du_customer_emb")
    .transform(set_working_days,  "card_acquired_at", "collect_date", "du_customer_courier_collect")
    .transform(set_working_days,  "card_acquired_at", "post_date", "du_customer_courier_post")
    .transform(set_working_days,  "card_acquired_at", "delivery_date", "du_customer_delivery")
    .transform(set_working_days,  "card_acquired_at", "card_unblock_date", "du_customer_unblock")
    .transform(set_working_days,  "card_acquired_at", "first_transaction_date", "du_customer_activation")
    
    # Visão FIS
    .transform(set_working_days,  "fis_processing_date", "embossing_processed_date", "du_fis_emb_processing")
    .transform(set_working_days,  "fis_processing_date", "embossing_expedition_date", "du_fis_emb_dispatch")
    .transform(set_working_days,  "fis_processing_date", "collect_date", "du_fis_courier_collect")
    .transform(set_working_days,  "fis_processing_date", "post_date", "du_fis_courier_post")
    .transform(set_working_days,  "fis_processing_date", "delivery_date", "du_fis_delivery")
    .transform(set_working_days,  "fis_processing_date", "card_unblock_date", "du_fis_unblock")
    .transform(set_working_days,  "fis_processing_date", "first_transaction_date", "du_fis_activation")

    # Visão Embossadora
    .transform(set_working_days,  "embossing_processed_date", "fis_processing_date", "du_emb_processing")
    .transform(set_working_days,  "embossing_processed_date", "embossing_expedition_date", "du_emb_processing_dispatch")
    .transform(set_working_days,  "embossing_expedition_date", "collect_date", "du_emb_courier_collect")
    .transform(set_working_days,  "embossing_expedition_date", "post_date", "du_emb_courier_post")

    .transform(set_working_days,  "embossing_expedition_date", "first_post_date", "du_emb_courier_first_post")
    .transform(set_working_days,  "embossing_expedition_date", "finalizer_status_date", "du_emb_finalizer_status")

    .transform(set_working_days,  "embossing_expedition_date", "delivery_date", "du_emb_courier_delivery")
    .transform(set_working_days,  "embossing_expedition_date", "card_unblock_date", "du_emb_unblock")
    .transform(set_working_days,  "embossing_expedition_date", "first_transaction_date", "du_emb_activation")

    # Visão Courier
    .transform(set_working_days,  "collect_date", "post_date", "du_collect_post")
    .transform(set_working_days,  "collect_date", "delivery_date", "du_collect_delivery")
    .transform(set_working_days,  "post_date", "delivery_date", "du_post_delivery")
    .transform(set_working_days,  "post_date", "card_unblock_date", "du_post_unblock")
    .transform(set_working_days,  "post_date", "first_transaction_date", "du_post_activation")

    # Desbloqueio
    .transform(set_working_days,  "embossing_expedition_date", "card_unblock_date", "du_dispatch_unblock")
    .transform(set_working_days,  "collect_date", "card_unblock_date", "du_collect_unblock")
    .transform(set_working_days,  "delivery_date", "card_unblock_date", "du_delivery_unblock")

    # Visão Pendente
    .transform(set_working_days,  "fis_account_created_at", "data_atual", "du_pend_from_customer_acquisition")
    .transform(set_working_days,  "fis_processing_date", "data_atual", "du_pend_from_fis_processing")
    .transform(set_working_days,  "embossing_processed_date", "data_atual", "du_pend_from_embossing_processing")
    .transform(set_working_days,  "embossing_expedition_date", "data_atual", "du_pend_from_embossing_dispatch")
    .transform(set_working_days,  "collect_date", "data_atual", "du_pend_from_collect")
    .transform(set_working_days,  "post_date", "data_atual", "du_pend_from_post")
    .transform(set_working_days,  "card_acquired_at", "data_atual", "du_pend_from_customer_delivery")
    .transform(set_working_days,  "last_status_delivery_at", "data_atual", "du_pend_from_last_status")

    # Ativação
    .transform(set_working_days,  "embossing_expedition_date", "first_transaction_date", "du_dispatch_activation")
    .transform(set_working_days,  "collect_date", "first_transaction_date", "du_collect_activation")
    .transform(set_working_days,  "delivery_date", "first_transaction_date", "du_delivery_activation")
    .transform(set_working_days,  "card_unblock_date", "first_transaction_date", "du_activation_unblock")
    .transform(set_working_days,  "card_unblock_date", "first_transaction_date", "du_unblock_activation")

    # Pendentes
    .transform(set_working_days,  "collect_date", "ontem", "du_last_status_collect")
    .transform(set_working_days,  "embossing_expedition_date", "ontem", "du_embossing_pending")
    .transform(set_working_days,  "last_status_delivery_at", "ontem", "du_last_status_pending")
).persist()


# COMMAND ----------

# DBTITLE 1,Finalização
# Calcula os dias úteis entre as datas
_output_df_ = (
  base_analitica
    .drop("data_atual", "ontem")
    # .join(du_df, "fmp_card_tracking_id", "left")
    .withColumn("du_pend_from_customer_acquisition",
                F.when(~F.col("fis_processing_date").isNull(), F.lit(None).cast(F.StringType()))
                .otherwise(F.col("du_pend_from_customer_acquisition")))
    
    .withColumn("du_pend_from_customer_delivery",
                F.when(~F.col("delivery_date").isNotNull(), F.lit(None).cast(F.StringType()))
                .otherwise(F.col("du_pend_from_customer_acquisition")))
    
    .withColumn("du_pend_from_embossing_processing",
                F.when(~F.col("embossing_processed_date").isNull(), F.lit(None).cast(F.StringType()))
                .otherwise(F.col("du_pend_from_embossing_processing")))

    .withColumn("du_pend_from_embossing_dispatch",
                F.when(~F.col("collect_date").isNull(), F.lit(None).cast(F.StringType()))
                .otherwise(F.col("du_pend_from_embossing_dispatch")))

    .withColumn("du_pend_from_collect",
                F.when(~F.col("collect_date").isNull(), F.lit(None).cast(F.StringType()))
                .otherwise(F.col("du_pend_from_collect")))

    .withColumn("du_pend_from_last_status", F.expr("""
      case 
        when status_delivery in ('Entregue', 'Remessa a ser fragmentada', 'Pendente fragmentação', 'Custódia', 'Fragmentado', 'Custódia - HUB000', 'Sinistrado', 'Entregue - sem data')
        then null
        else du_pend_from_last_status
      end
    """))

    .withColumn("du_pend_from_fis_processing", F.expr("""
      case 
        when status_delivery in ('Entregue', 'Remessa a ser fragmentada', 'Pendente fragmentação', 'Custódia', 'Fragmentado', 'Custódia - HUB000', 'Sinistrado', 'Entregue - sem data')
        then null
        else du_pend_from_fis_processing
      end
    """))
    
    .withColumn("du_pend_from_post", F.expr("""
      case 
        when status_delivery in ('Entregue', 'Remessa a ser fragmentada', 'Pendente fragmentação', 'Custódia', 'Fragmentado', 'Custódia - HUB000', 'Sinistrado', 'Entregue - sem data')
        then null
        else du_pend_from_post
      end
    """))
    
    .withColumn("du_fis_emb_dispatch", F.expr("""
      case 
        when du_fis_emb_dispatch is null and embossing_status = 'expedido' then 0
        else du_fis_emb_dispatch
      end
    """))

    .withColumn("du_fis_emb_processing", F.expr("""
      case 
        when du_fis_emb_processing is null and embossing_status = 'expedido' then 0
        else du_fis_emb_processing
      end
    """))

    .withColumn("du_fis_courier_collect", F.expr("""
      case 
        when du_fis_courier_collect is null and embossing_status = 'expedido' then 0
        else du_fis_courier_collect
      end
    """))

    # .withColumn("is_single_tracking", F.lit(True))
)

_output_df_.createOrReplaceTempView("_output_df_")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Tabela final

# COMMAND ----------

_output_df_ = spark.sql ('''
    select distinct
      *,
    case
        when du_customer_activation <= 4 then 'a. Até 4'
        when du_customer_activation <= 7 then 'b. Até 7'
        when du_customer_activation <= 10 then 'c. Até 10'
        when du_customer_activation <= 15 then 'd. Até 15'
        when du_customer_activation <= 30 then 'e. Até 30'
        when du_customer_activation <= 60 then 'f. Até 60'
        when du_customer_activation <= 90 then 'g. Até 90'
        when du_customer_activation > 90 then 'h. Acima de 90'
      end as class_du_customer_activation -- DIAS ÚTEIS: ATIVAÇÃO CLIENTE
    , case
        when datediff(first_transaction_date, card_acquired_at) <= 4 then 'a. Até 4'
        when datediff(first_transaction_date, card_acquired_at) <= 7 then 'b. Até 7'
        when datediff(first_transaction_date, card_acquired_at) <= 10 then 'c. Até 10'
        when datediff(first_transaction_date, card_acquired_at) <= 15 then 'd. Até 15'
        when datediff(first_transaction_date, card_acquired_at) <= 30 then 'e. Até 30'
        when datediff(first_transaction_date, card_acquired_at) <= 60 then 'f. Até 60'
        when datediff(first_transaction_date, card_acquired_at) <= 90 then 'g. Até 90'
        when datediff(first_transaction_date, card_acquired_at) > 90 then 'h. Acima de 90'
      end as class_dc_customer_activation -- DIAS CORRIDOS: ATIVAÇÃO CLIENTE
    , case
        when du_dispatch_activation <= 4 then 'a. Até 4'
        when du_dispatch_activation <= 7 then 'b. Até 7'
        when du_dispatch_activation <= 10 then 'c. Até 10'
        when du_dispatch_activation <= 15 then 'd. Até 15'
        when du_dispatch_activation <= 30 then 'e. Até 30'
        when du_dispatch_activation <= 60 then 'f. Até 60'
        when du_dispatch_activation <= 90 then 'g. Até 90'
        when du_dispatch_activation > 90 then 'h. Acima de 90'
      end as class_du_dispatch_activation -- DIAS ÚTEIS: ATIVAÇÃO "COLETA"
    , case
        when datediff(first_transaction_date, embossing_expedition_date) <= 4 then 'a. Até 4'
        when datediff(first_transaction_date, embossing_expedition_date) <= 7 then 'b. Até 7'
        when datediff(first_transaction_date, embossing_expedition_date) <= 10 then 'c. Até 10'
        when datediff(first_transaction_date, embossing_expedition_date) <= 15 then 'd. Até 15'
        when datediff(first_transaction_date, embossing_expedition_date) <= 30 then 'e. Até 30'
        when datediff(first_transaction_date, embossing_expedition_date) <= 60 then 'f. Até 60'
        when datediff(first_transaction_date, embossing_expedition_date) <= 90 then 'g. Até 90'
        when datediff(first_transaction_date, embossing_expedition_date) > 90 then 'h. Acima de 90'
      end as class_dc_dispatch_activation -- DIAS CORRIDOS: ATIVAÇÃO "COLETA"
    , case
        when du_customer_unblock <= 4 then 'a. Até 4'
        when du_customer_unblock <= 7 then 'b. Até 7'
        when du_customer_unblock <= 10 then 'c. Até 10'
        when du_customer_unblock <= 15 then 'd. Até 15'
        when du_customer_unblock <= 30 then 'e. Até 30'
        when du_customer_unblock <= 60 then 'f. Até 60'
        when du_customer_unblock <= 90 then 'g. Até 90'
        when du_customer_unblock > 90 then 'h. Acima de 90'
      end as class_du_customer_unblock -- DIAS ÚTEIS: VISAO CLIENTE - BLOQUEIO
    , case
        when datediff(card_unblock_date, card_acquired_at) <= 4 then 'a. Até 4'
        when datediff(card_unblock_date, card_acquired_at) <= 7 then 'b. Até 7'
        when datediff(card_unblock_date, card_acquired_at) <= 10 then 'c. Até 10'
        when datediff(card_unblock_date, card_acquired_at) <= 15 then 'd. Até 15'
        when datediff(card_unblock_date, card_acquired_at) <= 30 then 'e. Até 30'
        when datediff(card_unblock_date, card_acquired_at) <= 60 then 'f. Até 60'
        when datediff(card_unblock_date, card_acquired_at) <= 90 then 'g. Até 90'
        when datediff(card_unblock_date, card_acquired_at) > 90 then 'h. Acima de 90'
      end as class_dc_customer_unblock -- DIAS CORRIDOS: VISAO CLIENTE - BLOQUEIO
    , case
        when du_dispatch_unblock <= 4 then 'a. Até 4'
        when du_dispatch_unblock <= 7 then 'b. Até 7'
        when du_dispatch_unblock <= 10 then 'c. Até 10'
        when du_dispatch_unblock <= 15 then 'd. Até 15'
        when du_dispatch_unblock <= 30 then 'e. Até 30'
        when du_dispatch_unblock <= 60 then 'f. Até 60'
        when du_dispatch_unblock <= 90 then 'g. Até 90'
        when du_dispatch_unblock > 90 then 'h. Acima de 90'
      end as class_du_dispatch_unblock -- DIAS ÚTEIS: ATIVAÇÃO
    , case
        when datediff(card_unblock_date, embossing_expedition_date) <= 4 then 'a. Até 4'
        when datediff(card_unblock_date, embossing_expedition_date) <= 7 then 'b. Até 7'
        when datediff(card_unblock_date, embossing_expedition_date) <= 10 then 'c. Até 10'
        when datediff(card_unblock_date, embossing_expedition_date) <= 15 then 'd. Até 15'
        when datediff(card_unblock_date, embossing_expedition_date) <= 30 then 'e. Até 30'
        when datediff(card_unblock_date, embossing_expedition_date) <= 60 then 'f. Até 60'
        when datediff(card_unblock_date, embossing_expedition_date) <= 90 then 'g. Até 90'
        when datediff(card_unblock_date, embossing_expedition_date) > 90 then 'h. Acima de 90'
      end as class_dc_dispatch_unblock -- DIAS CORRIDOS: ATIVAÇÃO
    , case
        when du_emb_courier_delivery <= 4 then 'a. Até 4'
        when du_emb_courier_delivery <= 7 then 'b. Até 7'
        when du_emb_courier_delivery <= 10 then 'c. Até 10'
        when du_emb_courier_delivery <= 15 then 'd. Até 15'
        when du_emb_courier_delivery <= 30 then 'e. Até 30'
        when du_emb_courier_delivery > 30 then 'f. Acima de 30'
      end as class_du_collect_delivery -- GERAL
    , case
        when datediff(delivery_date, embossing_expedition_date) <= 4 then 'a. Até 4'
        when datediff(delivery_date, embossing_expedition_date) <= 7 then 'b. Até 7'
        when datediff(delivery_date, embossing_expedition_date) <= 10 then 'c. Até 10'
        when datediff(delivery_date, embossing_expedition_date) <= 15 then 'd. Até 15'
        when datediff(delivery_date, embossing_expedition_date) <= 30 then 'e. Até 30'
        when datediff(delivery_date, embossing_expedition_date) > 30 then 'f. Acima de 30'
      end as class_dc_collect_delivery -- GERAL
    , case
        when du_customer_delivery <= 4 then 'a. Até 4'
        when du_customer_delivery <= 7 then 'b. Até 7'
        when du_customer_delivery <= 10 then 'c. Até 10'
        when du_customer_delivery <= 15 then 'd. Até 15'
        when du_customer_delivery <= 30 then 'e. Até 30'
        when du_customer_delivery > 30 then 'f. Acima de 30'
      end as class_du_customer_delivery -- GERAL
    , case
        when datediff(delivery_date, card_acquired_at) <= 4 then 'a. Até 4'
        when datediff(delivery_date, card_acquired_at) <= 7 then 'b. Até 7'
        when datediff(delivery_date, card_acquired_at) <= 10 then 'c. Até 10'
        when datediff(delivery_date, card_acquired_at) <= 15 then 'd. Até 15'
        when datediff(delivery_date, card_acquired_at) <= 30 then 'e. Até 30'
        when datediff(delivery_date, card_acquired_at) > 30 then 'f. Acima de 30'
      end as class_dc_customer_delivery -- GERAL
    , case
        when du_delivery_unblock <= 4 then 'a. Até 4'
        when du_delivery_unblock <= 7 then 'b. Até 7'
        when du_delivery_unblock <= 10 then 'c. Até 10'
        when du_delivery_unblock <= 15 then 'd. Até 15'
        when du_delivery_unblock <= 30 then 'e. Até 30'
        when du_delivery_unblock <= 60 then 'f. Até 60'
        when du_delivery_unblock <= 90 then 'g. Até 90'
        when du_delivery_unblock > 90 then 'h. Acima de 90'
        when card_unblock_date < date(delivery_date) then 'Erro - Avaliar'
      end as class_du_delivery_unblock -- ENTREGUE BLOQUEIO - DIAS ÚTEIS
    , case
        when datediff(card_unblock_date, delivery_date) <= 4 then 'a. Até 4'
        when datediff(card_unblock_date, delivery_date) <= 7 then 'b. Até 7'
        when datediff(card_unblock_date, delivery_date) <= 10 then 'c. Até 10'
        when datediff(card_unblock_date, delivery_date) <= 15 then 'd. Até 15'
        when datediff(card_unblock_date, delivery_date) <= 30 then 'e. Até 30'
        when datediff(card_unblock_date, delivery_date) <= 60 then 'f. Até 60'
        when datediff(card_unblock_date, delivery_date) <= 90 then 'g. Até 90'
        when datediff(card_unblock_date, delivery_date) > 90 then 'h. Acima de 90'
        when first_transaction_date < date(delivery_date) then 'Erro - Avaliar'
      end as class_dc_delivery_unblock -- ENTREGUE BLOQUEIO - DIAS CORRIDOS
    , case
        when du_delivery_activation <= 4 then 'a. Até 4'
        when du_delivery_activation <= 7 then 'b. Até 7'
        when du_delivery_activation <= 10 then 'c. Até 10'
        when du_delivery_activation <= 15 then 'd. Até 15'
        when du_delivery_activation <= 30 then 'e. Até 30'
        when du_delivery_activation <= 60 then 'f. Até 60'
        when du_delivery_activation <= 90 then 'g. Até 90'
        when du_delivery_activation > 90 then 'h. Acima de 90'
        when card_unblock_date < date(delivery_date) then 'Erro - Avaliar'
      end as class_du_delivery_activation -- ENTREGUE ATIVAÇÃO - DIAS ÚTEIS
    , case
        when datediff(first_transaction_date, delivery_date) <= 4 then 'a. Até 4'
        when datediff(first_transaction_date, delivery_date) <= 7 then 'b. Até 7'
        when datediff(first_transaction_date, delivery_date) <= 10 then 'c. Até 10'
        when datediff(first_transaction_date, delivery_date) <= 15 then 'd. Até 15'
        when datediff(first_transaction_date, delivery_date) <= 30 then 'e. Até 30'
        when datediff(first_transaction_date, delivery_date) <= 60 then 'f. Até 60'
        when datediff(first_transaction_date, delivery_date) <= 90 then 'g. Até 90'
        when datediff(first_transaction_date, delivery_date) > 90 then 'h. Acima de 90'
        when first_transaction_date < date(delivery_date) then 'Erro - Avaliar'
      end as class_dc_delivery_activation -- ENTREGUE ATIVAÇÃO - DIAS CORRIDOS
    , case
        when du_embossing_pending <= 1 then 'a. D1'
        when du_embossing_pending = 2 then 'b. D2'
        when du_embossing_pending = 3 then 'c. D3'
        when du_embossing_pending = 4 then 'd. D4'
        when du_embossing_pending = 5 then 'e. D5'
        when du_embossing_pending = 6 then 'f. D6'
        when du_embossing_pending = 7 then 'g. D7'
        when du_embossing_pending = 8 then 'h. D8'
        when du_embossing_pending = 9 then 'i. D9'
        when du_embossing_pending = 10 then 'j. D10'
        when du_embossing_pending <= 15 then 'k. Até 15'
        when du_embossing_pending <= 30 then 'l. Até 30'
        when du_embossing_pending > 30 then 'm. Acima de 30'
      end as class_du_embossing_pending
    , case
        when datediff(current_date()-1, embossing_expedition_date) <= 1 then 'a. D1'
        when datediff(current_date()-1, embossing_expedition_date) = 2 then 'b. D2'
        when datediff(current_date()-1, embossing_expedition_date) = 3 then 'c. D3'
        when datediff(current_date()-1, embossing_expedition_date) = 4 then 'd. D4'
        when datediff(current_date()-1, embossing_expedition_date) = 5 then 'e. D5'
        when datediff(current_date()-1, embossing_expedition_date) = 6 then 'f. D6'
        when datediff(current_date()-1, embossing_expedition_date) = 7 then 'g. D7'
        when datediff(current_date()-1, embossing_expedition_date) = 8 then 'h. D8'
        when datediff(current_date()-1, embossing_expedition_date) = 9 then 'i. D9'
        when datediff(current_date()-1, embossing_expedition_date) = 10 then 'j. D10'
        when datediff(current_date()-1, embossing_expedition_date) <= 15 then 'k. Até 15'
        when datediff(current_date()-1, embossing_expedition_date) <= 30 then 'l. Até 30'
        when datediff(current_date()-1, embossing_expedition_date) > 30 then 'm. Acima de 30'
      end as class_dc_embossing_pending
    , case
        when du_last_status_pending <= 1 then 'a. D1'
        when du_last_status_pending = 2 then 'b. D2'
        when du_last_status_pending = 3 then 'c. D3'
        when du_last_status_pending = 4 then 'd. D4'
        when du_last_status_pending = 5 then 'e. D5'
        when du_last_status_pending = 6 then 'f. D6'
        when du_last_status_pending = 7 then 'g. D7'
        when du_last_status_pending = 8 then 'h. D8'
        when du_last_status_pending = 9 then 'i. D9'
        when du_last_status_pending = 10 then 'j. D10'
        when du_last_status_pending <= 15 then 'k. Até 15'
        when du_last_status_pending <= 30 then 'l. Até 30'
        when du_last_status_pending > 30 then 'm. Acima de 30'
      end as class_du_last_status_pending
    , case
        when datediff(current_date()-1, last_status_delivery_at) = 1 then 'a. D1'
        when datediff(current_date()-1, last_status_delivery_at) = 2 then 'b. D2'
        when datediff(current_date()-1, last_status_delivery_at) = 3 then 'c. D3'
        when datediff(current_date()-1, last_status_delivery_at) = 4 then 'd. D4'
        when datediff(current_date()-1, last_status_delivery_at) = 5 then 'e. D5'
        when datediff(current_date()-1, last_status_delivery_at) = 6 then 'f. D6'
        when datediff(current_date()-1, last_status_delivery_at) = 7 then 'g. D7'
        when datediff(current_date()-1, last_status_delivery_at) = 8 then 'h. D8'
        when datediff(current_date()-1, last_status_delivery_at) = 9 then 'i. D9'
        when datediff(current_date()-1, last_status_delivery_at) = 10 then 'j. D10'
        when datediff(current_date()-1, last_status_delivery_at) <= 15 then 'k. Até 15'
        when datediff(current_date()-1, last_status_delivery_at) <= 30 then 'l. Até 30'
        when datediff(current_date()-1, last_status_delivery_at) > 30 then 'm. Acima de 30'
      end as class_dc_last_status_pending
    , cd.current_datetime as datetime_update
  from _output_df_
  cross join current_datetime cd
                         ''')

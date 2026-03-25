# Databricks notebook source
# MAGIC %md
# MAGIC # 03_06_gold_kpi_review_logistics
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `gold_fact_orders`
# MAGIC - Agregar indicadores que relacionam atraso e experiência do cliente
# MAGIC - Produzir uma tabela KPI pronta para consumo executivo
# MAGIC - Persistir a tabela `gold_kpi_review_logistics` em Delta
# MAGIC
# MAGIC Granularidade final:
# MAGIC - `order_purchase_month`
# MAGIC - `seller_state`
# MAGIC - `product_category_name`

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "gold_fact_orders")
dbutils.widgets.text("target_table_name", "gold_kpi_review_logistics")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
SOURCE_TABLE_NAME = dbutils.widgets.get("source_table_name").strip()
TARGET_TABLE_NAME = dbutils.widgets.get("target_table_name").strip()
WRITE_MODE = dbutils.widgets.get("write_mode").strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_SOURCE_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{SOURCE_TABLE_NAME}"
FULL_TARGET_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{TARGET_TABLE_NAME}"

print("Parâmetros carregados com sucesso.")
print(f"FULL_SCHEMA_NAME = {FULL_SCHEMA_NAME}")
print(f"FULL_SOURCE_TABLE_NAME = {FULL_SOURCE_TABLE_NAME}")
print(f"FULL_TARGET_TABLE_NAME = {FULL_TARGET_TABLE_NAME}")
print(f"WRITE_MODE = {WRITE_MODE}")

# COMMAND ----------

# DBTITLE 1,Funções auxiliares
def log_step(step_name: str, status: str, message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    payload = {
        "timestamp": timestamp,
        "step": step_name,
        "status": status,
        "message": message,
    }
    print(json.dumps(payload, ensure_ascii=False))


def ensure_table_exists(full_table_name: str, description: str) -> None:
    if not spark.catalog.tableExists(full_table_name):
        raise FileNotFoundError(f"{description} não encontrada: {full_table_name}")
    log_step("table_validation", "OK", f"{description} encontrada: {full_table_name}")


# COMMAND ----------

# DBTITLE 1,Uso do catálogo e schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

log_step("context_setup", "OK", f"Contexto definido para {FULL_SCHEMA_NAME}")

# COMMAND ----------

# DBTITLE 1,Validação da tabela Gold de origem
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Gold de fact_orders")

# COMMAND ----------

# DBTITLE 1,Leitura da Gold
fact_orders_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("gold_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da tabela KPI
gold_kpi_review_logistics_df = (
    fact_orders_df
    .withColumn("seller_state_group", F.coalesce(F.col("seller_state"), F.lit("unknown")))
    .withColumn("product_category_group", F.coalesce(F.col("product_category_name"), F.lit("unknown")))
    .groupBy("order_purchase_month", "seller_state_group", "product_category_group")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("has_review_comment") == True, F.col("order_id"))).alias("orders_with_review_comment"),
        F.countDistinct(F.when(F.col("has_negative_review") == True, F.col("order_id"))).alias("orders_with_negative_review"),
        F.countDistinct(F.when(F.col("review_sentiment_group") == "positive", F.col("order_id"))).alias("orders_with_positive_review"),
        F.countDistinct(F.when(F.col("is_delayed") == True, F.col("order_id"))).alias("delayed_orders"),
        F.countDistinct(F.when((F.col("is_delayed") == True) & (F.col("has_negative_review") == True), F.col("order_id"))).alias("delayed_orders_with_negative_review"),
        F.countDistinct(F.when((F.col("is_delayed") == False) & (F.col("has_negative_review") == True), F.col("order_id"))).alias("on_time_orders_with_negative_review"),
        F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
        F.round(F.avg(F.when(F.col("is_delayed") == True, F.col("avg_review_score"))), 2).alias("avg_review_score_delayed_orders"),
        F.round(F.avg(F.when(F.col("is_delayed") == False, F.col("avg_review_score"))), 2).alias("avg_review_score_on_time_orders"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
        F.round(F.avg("avg_review_response_hours"), 2).alias("avg_review_response_hours")
    )
    .withColumn(
        "negative_review_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_negative_review") / F.col("total_orders"), 4))
    )
    .withColumn(
        "positive_review_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_positive_review") / F.col("total_orders"), 4))
    )
    .withColumn(
        "delay_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("delayed_orders") / F.col("total_orders"), 4))
    )
    .withColumn(
        "delayed_negative_review_rate",
        F.when(F.col("delayed_orders") > F.lit(0), F.round(F.col("delayed_orders_with_negative_review") / F.col("delayed_orders"), 4))
    )
    .withColumn(
        "on_time_negative_review_rate",
        F.when(
            (F.col("total_orders") - F.col("delayed_orders")) > F.lit(0),
            F.round(F.col("on_time_orders_with_negative_review") / (F.col("total_orders") - F.col("delayed_orders")), 4)
        )
    )
    .withColumn(
        "review_logistics_risk_flag",
        (
            (F.col("delay_rate_orders") >= F.lit(0.20)) &
            (F.col("negative_review_rate_orders") >= F.lit(0.15))
        )
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "order_purchase_month",
        F.col("seller_state_group").alias("seller_state"),
        F.col("product_category_group").alias("product_category_name"),
        "total_orders",
        "orders_with_review_comment",
        "orders_with_negative_review",
        "orders_with_positive_review",
        "negative_review_rate_orders",
        "positive_review_rate_orders",
        "delayed_orders",
        "delay_rate_orders",
        "delayed_orders_with_negative_review",
        "on_time_orders_with_negative_review",
        "delayed_negative_review_rate",
        "on_time_negative_review_rate",
        "avg_review_score",
        "avg_review_score_delayed_orders",
        "avg_review_score_on_time_orders",
        "avg_delay_days",
        "avg_review_response_hours",
        "review_logistics_risk_flag",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de kpi_review_logistics montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_kpi_review_logistics_df.count()
months_count = gold_kpi_review_logistics_df.select("order_purchase_month").distinct().count()
seller_states_count = gold_kpi_review_logistics_df.select("seller_state").distinct().count()
categories_count = gold_kpi_review_logistics_df.select("product_category_name").distinct().count()
risk_flag_count = gold_kpi_review_logistics_df.filter(F.col("review_logistics_risk_flag") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct months count: {months_count}")
log_step("gold_metrics", "OK", f"Distinct seller_state count: {seller_states_count}")
log_step("gold_metrics", "OK", f"Distinct product_category_name count: {categories_count}")
log_step("gold_metrics", "OK", f"Rows with review_logistics_risk_flag: {risk_flag_count}")

print("Schema da Gold:")
gold_kpi_review_logistics_df.printSchema()

display(gold_kpi_review_logistics_df.orderBy(F.col("delayed_negative_review_rate").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_kpi_review_logistics_df.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TARGET_TABLE_NAME)
)

log_step("delta_write", "OK", f"Tabela gravada com sucesso: {FULL_TARGET_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Evidências pós-gravação
persisted_df = spark.table(FULL_TARGET_TABLE_NAME)
persisted_row_count = persisted_df.count()

log_step("post_write_validation", "OK", f"Persisted row count: {persisted_row_count}")

print(f"Tabela criada/atualizada: {FULL_TARGET_TABLE_NAME}")
display(persisted_df.orderBy(F.col("delayed_negative_review_rate").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

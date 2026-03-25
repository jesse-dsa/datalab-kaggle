# Databricks notebook source
# MAGIC %md
# MAGIC # 03_05_gold_kpi_delivery_performance
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `gold_fact_orders`
# MAGIC - Agregar os principais indicadores de performance logística
# MAGIC - Produzir uma tabela KPI pronta para consumo executivo
# MAGIC - Persistir a tabela `gold_kpi_delivery_performance` em Delta
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
dbutils.widgets.text("target_table_name", "gold_kpi_delivery_performance")
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
gold_kpi_delivery_performance_df = (
    fact_orders_df
    .withColumn("seller_state_group", F.coalesce(F.col("seller_state"), F.lit("unknown")))
    .withColumn("product_category_group", F.coalesce(F.col("product_category_name"), F.lit("unknown")))
    .groupBy("order_purchase_month", "seller_state_group", "product_category_group")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("order_item_key").alias("total_order_items"),
        F.countDistinct(F.when(F.col("is_delayed") == True, F.col("order_id"))).alias("delayed_orders"),
        F.countDistinct(F.when(F.col("high_friction_flag") == True, F.col("order_id"))).alias("high_friction_orders"),
        F.countDistinct(F.when(F.col("has_negative_review") == True, F.col("order_id"))).alias("orders_with_negative_review"),
        F.countDistinct(F.when(F.col("friction_flag") == True, F.col("order_id"))).alias("orders_with_friction"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
        F.round(F.expr("percentile_approx(delivery_delay_days, 0.5)"), 2).alias("p50_delay_days"),
        F.round(F.expr("percentile_approx(delivery_delay_days, 0.9)"), 2).alias("p90_delay_days"),
        F.round(F.avg("purchase_to_delivery_hours"), 2).alias("avg_purchase_to_delivery_hours"),
        F.round(F.avg("approval_to_carrier_hours"), 2).alias("avg_approval_to_carrier_hours"),
        F.round(F.avg("carrier_to_customer_hours"), 2).alias("avg_carrier_to_customer_hours"),
        F.round(F.avg("item_total_value_amount"), 2).alias("avg_item_total_value_amount"),
        F.round(F.sum("item_total_value_amount"), 2).alias("sum_item_total_value_amount"),
        F.round(F.avg("order_total_payment_value_amount"), 2).alias("avg_order_total_payment_value_amount"),
        F.round(F.avg("payment_to_items_value_gap_amount"), 2).alias("avg_payment_gap_amount")
    )
    .withColumn(
        "delay_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("delayed_orders") / F.col("total_orders"), 4))
    )
    .withColumn(
        "negative_review_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_negative_review") / F.col("total_orders"), 4))
    )
    .withColumn(
        "friction_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_friction") / F.col("total_orders"), 4))
    )
    .withColumn(
        "high_friction_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("high_friction_orders") / F.col("total_orders"), 4))
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "order_purchase_month",
        F.col("seller_state_group").alias("seller_state"),
        F.col("product_category_group").alias("product_category_name"),
        "total_orders",
        "total_order_items",
        "delayed_orders",
        "delay_rate_orders",
        "orders_with_negative_review",
        "negative_review_rate_orders",
        "orders_with_friction",
        "friction_rate_orders",
        "high_friction_orders",
        "high_friction_rate_orders",
        "avg_delay_days",
        "p50_delay_days",
        "p90_delay_days",
        "avg_purchase_to_delivery_hours",
        "avg_approval_to_carrier_hours",
        "avg_carrier_to_customer_hours",
        "avg_item_total_value_amount",
        "sum_item_total_value_amount",
        "avg_order_total_payment_value_amount",
        "avg_payment_gap_amount",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de kpi_delivery_performance montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_kpi_delivery_performance_df.count()
months_count = gold_kpi_delivery_performance_df.select("order_purchase_month").distinct().count()
seller_states_count = gold_kpi_delivery_performance_df.select("seller_state").distinct().count()
categories_count = gold_kpi_delivery_performance_df.select("product_category_name").distinct().count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct months count: {months_count}")
log_step("gold_metrics", "OK", f"Distinct seller_state count: {seller_states_count}")
log_step("gold_metrics", "OK", f"Distinct product_category_name count: {categories_count}")

print("Schema da Gold:")
gold_kpi_delivery_performance_df.printSchema()

display(gold_kpi_delivery_performance_df.orderBy(F.col("delay_rate_orders").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_kpi_delivery_performance_df.write
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
display(persisted_df.orderBy(F.col("delay_rate_orders").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

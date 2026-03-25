# Databricks notebook source
# MAGIC %md
# MAGIC # 03_01_gold_fact_orders
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `silver_orders_enriched`
# MAGIC - Selecionar as métricas e dimensões mais relevantes para consumo analítico
# MAGIC - Padronizar o fato principal do projeto
# MAGIC - Persistir a tabela `gold_fact_orders` em Delta
# MAGIC
# MAGIC Observação:
# MAGIC - A granularidade final desta tabela é de item do pedido
# MAGIC - Um mesmo `order_id` pode aparecer em múltiplas linhas

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "silver_orders_enriched")
dbutils.widgets.text("target_table_name", "gold_fact_orders")
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

# DBTITLE 1,Validação da tabela Silver integrada
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Silver integrada")

# COMMAND ----------

# DBTITLE 1,Leitura da Silver integrada
orders_enriched_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("silver_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Gold
gold_fact_orders_df = (
    orders_enriched_df
    .withColumn(
        "delivery_status_group",
        F.when(F.col("is_canceled") == True, F.lit("canceled"))
         .when(F.col("is_delivered") == True, F.lit("delivered"))
         .otherwise(F.lit("other"))
    )
    .withColumn(
        "review_sentiment_group",
        F.when(F.col("has_negative_review") == True, F.lit("negative"))
         .when(F.col("has_positive_review") == True, F.lit("positive"))
         .when(F.col("avg_review_score").isNotNull(), F.lit("neutral_or_mixed"))
         .otherwise(F.lit("no_review"))
    )
    .withColumn(
        "friction_flag",
        (
            (F.col("is_delayed") == True) |
            (F.col("has_negative_review") == True) |
            (F.col("is_order_payment_value_consistent") == False)
        )
    )
    .withColumn(
        "high_friction_flag",
        (
            (F.col("is_delayed") == True) &
            (F.col("has_negative_review") == True)
        )
    )
    .withColumn(
        "order_item_key",
        F.concat_ws("_", F.col("order_id"), F.col("order_item_id_int").cast("string"))
    )
    .withColumn(
        "order_purchase_month",
        F.date_trunc("month", F.col("order_purchase_date"))
    )
    .withColumn(
        "delay_days_bucket",
        F.when(F.col("delivery_delay_days").isNull(), F.lit("unknown"))
         .when(F.col("delivery_delay_days") <= F.lit(0), F.lit("on_time_or_early"))
         .when(F.col("delivery_delay_days") <= F.lit(3), F.lit("delay_1_to_3_days"))
         .when(F.col("delivery_delay_days") <= F.lit(7), F.lit("delay_4_to_7_days"))
         .otherwise(F.lit("delay_above_7_days"))
    )
    .select(
        "order_item_key",
        "order_id",
        "order_item_id_int",
        "order_purchase_date",
        "order_purchase_month",
        "order_status",
        "delivery_status_group",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "seller_id",
        "seller_city",
        "seller_state",
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_volume_cm3",
        "price_amount",
        "freight_value_amount",
        "item_total_value_amount",
        "order_total_items_value_amount",
        "order_total_payment_value_amount",
        "payment_to_items_value_gap_amount",
        "is_order_payment_value_consistent",
        "max_payment_installments",
        "has_credit_card_payment",
        "has_boleto_payment",
        "has_voucher_payment",
        "has_invalid_payment_value",
        "purchase_to_approval_hours",
        "approval_to_carrier_hours",
        "carrier_to_customer_hours",
        "purchase_to_delivery_hours",
        "delivery_delay_days",
        "delay_days_bucket",
        "is_delayed",
        "avg_review_score",
        "avg_review_response_hours",
        "review_sentiment_group",
        "has_negative_review",
        "has_review_comment",
        "friction_flag",
        "high_friction_flag",
        "has_purchase_approval_inconsistency",
        "has_approval_carrier_inconsistency",
        "has_carrier_customer_inconsistency",
        "has_non_positive_price",
        "has_negative_freight_value",
        "has_invalid_dimensions",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de fact_orders montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_fact_orders_df.count()
distinct_order_count = gold_fact_orders_df.select("order_id").distinct().count()
distinct_order_item_key_count = gold_fact_orders_df.select("order_item_key").distinct().count()
delayed_rows_count = gold_fact_orders_df.filter(F.col("is_delayed") == True).count()
negative_review_rows_count = gold_fact_orders_df.filter(F.col("has_negative_review") == True).count()
friction_rows_count = gold_fact_orders_df.filter(F.col("friction_flag") == True).count()
high_friction_rows_count = gold_fact_orders_df.filter(F.col("high_friction_flag") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct order count: {distinct_order_count}")
log_step("gold_metrics", "OK", f"Distinct order_item_key count: {distinct_order_item_key_count}")
log_step("gold_metrics", "OK", f"Delayed rows count: {delayed_rows_count}")
log_step("gold_metrics", "OK", f"Rows with negative review: {negative_review_rows_count}")
log_step("gold_metrics", "OK", f"Rows with friction_flag: {friction_rows_count}")
log_step("gold_metrics", "OK", f"Rows with high_friction_flag: {high_friction_rows_count}")

print("Schema da Gold:")
gold_fact_orders_df.printSchema()

display(gold_fact_orders_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_fact_orders_df.write
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
persisted_distinct_order_count = persisted_df.select("order_id").distinct().count()

log_step("post_write_validation", "OK", f"Persisted row count: {persisted_row_count}")
log_step("post_write_validation", "OK", f"Persisted distinct order count: {persisted_distinct_order_count}")

print(f"Tabela criada/atualizada: {FULL_TARGET_TABLE_NAME}")
display(persisted_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

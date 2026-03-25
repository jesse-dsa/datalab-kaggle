# Databricks notebook source
# MAGIC %md
# MAGIC # 03_07_gold_kpi_payment_consistency
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `gold_fact_orders`
# MAGIC - Agregar indicadores de consistência entre valor pago e soma dos itens
# MAGIC - Produzir uma tabela KPI pronta para consumo executivo
# MAGIC - Persistir a tabela `gold_kpi_payment_consistency` em Delta
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
dbutils.widgets.text("target_table_name", "gold_kpi_payment_consistency")
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
gold_kpi_payment_consistency_df = (
    fact_orders_df
    .withColumn("seller_state_group", F.coalesce(F.col("seller_state"), F.lit("unknown")))
    .withColumn("product_category_group", F.coalesce(F.col("product_category_name"), F.lit("unknown")))
    .groupBy("order_purchase_month", "seller_state_group", "product_category_group")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("order_item_key").alias("total_order_items"),
        F.countDistinct(F.when(F.col("is_order_payment_value_consistent") == False, F.col("order_id"))).alias("orders_with_payment_gap"),
        F.countDistinct(F.when(F.col("has_credit_card_payment") == True, F.col("order_id"))).alias("orders_with_credit_card"),
        F.countDistinct(F.when(F.col("has_boleto_payment") == True, F.col("order_id"))).alias("orders_with_boleto"),
        F.countDistinct(F.when(F.col("has_voucher_payment") == True, F.col("order_id"))).alias("orders_with_voucher"),
        F.countDistinct(F.when(F.col("has_invalid_payment_value") == True, F.col("order_id"))).alias("orders_with_invalid_payment_value"),
        F.round(F.avg("order_total_payment_value_amount"), 2).alias("avg_order_total_payment_value_amount"),
        F.round(F.avg("order_total_items_value_amount"), 2).alias("avg_order_total_items_value_amount"),
        F.round(F.avg("payment_to_items_value_gap_amount"), 2).alias("avg_payment_gap_amount"),
        F.round(F.expr("percentile_approx(payment_to_items_value_gap_amount, 0.5)"), 2).alias("p50_payment_gap_amount"),
        F.round(F.expr("percentile_approx(payment_to_items_value_gap_amount, 0.9)"), 2).alias("p90_payment_gap_amount"),
        F.round(F.avg("max_payment_installments"), 2).alias("avg_max_payment_installments"),
        F.round(F.avg("item_total_value_amount"), 2).alias("avg_item_total_value_amount"),
        F.round(F.sum("item_total_value_amount"), 2).alias("sum_item_total_value_amount")
    )
    .withColumn(
        "payment_gap_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_payment_gap") / F.col("total_orders"), 4))
    )
    .withColumn(
        "credit_card_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_credit_card") / F.col("total_orders"), 4))
    )
    .withColumn(
        "boleto_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_boleto") / F.col("total_orders"), 4))
    )
    .withColumn(
        "voucher_rate_orders",
        F.when(F.col("total_orders") > F.lit(0), F.round(F.col("orders_with_voucher") / F.col("total_orders"), 4))
    )
    .withColumn(
        "payment_consistency_risk_flag",
        (
            (F.col("payment_gap_rate_orders") >= F.lit(0.10)) |
            (F.abs(F.col("avg_payment_gap_amount")) >= F.lit(5.0))
        )
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "order_purchase_month",
        F.col("seller_state_group").alias("seller_state"),
        F.col("product_category_group").alias("product_category_name"),
        "total_orders",
        "total_order_items",
        "orders_with_payment_gap",
        "payment_gap_rate_orders",
        "orders_with_credit_card",
        "credit_card_rate_orders",
        "orders_with_boleto",
        "boleto_rate_orders",
        "orders_with_voucher",
        "voucher_rate_orders",
        "orders_with_invalid_payment_value",
        "avg_order_total_payment_value_amount",
        "avg_order_total_items_value_amount",
        "avg_payment_gap_amount",
        "p50_payment_gap_amount",
        "p90_payment_gap_amount",
        "avg_max_payment_installments",
        "avg_item_total_value_amount",
        "sum_item_total_value_amount",
        "payment_consistency_risk_flag",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de kpi_payment_consistency montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_kpi_payment_consistency_df.count()
months_count = gold_kpi_payment_consistency_df.select("order_purchase_month").distinct().count()
seller_states_count = gold_kpi_payment_consistency_df.select("seller_state").distinct().count()
categories_count = gold_kpi_payment_consistency_df.select("product_category_name").distinct().count()
risk_flag_count = gold_kpi_payment_consistency_df.filter(F.col("payment_consistency_risk_flag") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct months count: {months_count}")
log_step("gold_metrics", "OK", f"Distinct seller_state count: {seller_states_count}")
log_step("gold_metrics", "OK", f"Distinct product_category_name count: {categories_count}")
log_step("gold_metrics", "OK", f"Rows with payment_consistency_risk_flag: {risk_flag_count}")

print("Schema da Gold:")
gold_kpi_payment_consistency_df.printSchema()

display(gold_kpi_payment_consistency_df.orderBy(F.col("payment_gap_rate_orders").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_kpi_payment_consistency_df.write
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
display(persisted_df.orderBy(F.col("payment_gap_rate_orders").desc_nulls_last()).limit(20))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

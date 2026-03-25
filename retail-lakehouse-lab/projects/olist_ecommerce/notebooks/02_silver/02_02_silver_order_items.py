# Databricks notebook source
# MAGIC %md
# MAGIC # 02_02_silver_order_items
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_order_items`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas numéricas e de data/hora
# MAGIC - Criar variáveis iniciais no nível do item do pedido
# MAGIC - Persistir a tabela `silver_order_items` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_order_items")
dbutils.widgets.text("target_table_name", "silver_order_items")
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


def sql_try_to_timestamp(column_name: str):
    return F.expr(f"try_to_timestamp({column_name}, 'yyyy-MM-dd HH:mm:ss')")


def sql_try_cast(column_name: str, target_type: str):
    return F.expr(f"try_cast({column_name} as {target_type})")


def null_if_blank(column_name: str):
    return F.when(
        F.trim(F.coalesce(F.col(column_name), F.lit(""))) == "",
        F.lit(None)
    ).otherwise(F.trim(F.col(column_name)))


# COMMAND ----------

# DBTITLE 1,Uso do catálogo e schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

log_step("context_setup", "OK", f"Contexto definido para {FULL_SCHEMA_NAME}")

# COMMAND ----------

# DBTITLE 1,Validação da tabela Bronze
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de order_items")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
order_items_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
order_items_silver_base_df = (
    order_items_bronze_df
    .withColumn("order_id", null_if_blank("order_id"))
    .withColumn("product_id", null_if_blank("product_id"))
    .withColumn("seller_id", null_if_blank("seller_id"))
    .withColumn("order_item_id_int", sql_try_cast("order_item_id", "int"))
    .withColumn("shipping_limit_date_ts", sql_try_to_timestamp("shipping_limit_date"))
    .withColumn("price_amount", sql_try_cast("price", "double"))
    .withColumn("freight_value_amount", sql_try_cast("freight_value", "double"))
)

order_items_silver_df = (
    order_items_silver_base_df
    .withColumn("shipping_limit_date", F.to_date("shipping_limit_date_ts"))
    .withColumn(
        "item_total_value_amount",
        F.when(
            F.col("price_amount").isNotNull() & F.col("freight_value_amount").isNotNull(),
            F.round(F.col("price_amount") + F.col("freight_value_amount"), 2)
        )
    )
    .withColumn(
        "is_free_shipping",
        F.when(F.col("freight_value_amount").isNull(), F.lit(None)).otherwise(F.col("freight_value_amount") == F.lit(0))
    )
    .withColumn(
        "has_missing_product_id",
        F.col("product_id").isNull()
    )
    .withColumn(
        "has_missing_seller_id",
        F.col("seller_id").isNull()
    )
    .withColumn(
        "has_invalid_order_item_id",
        F.col("order_item_id_int").isNull()
    )
    .withColumn(
        "has_non_positive_price",
        F.when(F.col("price_amount").isNull(), F.lit(None)).otherwise(F.col("price_amount") <= F.lit(0))
    )
    .withColumn(
        "has_negative_freight_value",
        F.when(F.col("freight_value_amount").isNull(), F.lit(None)).otherwise(F.col("freight_value_amount") < F.lit(0))
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "order_id",
        "order_item_id_int",
        "product_id",
        "seller_id",
        "shipping_limit_date_ts",
        "shipping_limit_date",
        "price_amount",
        "freight_value_amount",
        "item_total_value_amount",
        "is_free_shipping",
        "has_missing_product_id",
        "has_missing_seller_id",
        "has_invalid_order_item_id",
        "has_non_positive_price",
        "has_negative_freight_value",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de order_items montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = order_items_silver_df.count()
null_order_id_count = order_items_silver_df.filter(F.col("order_id").isNull()).count()
null_order_item_id_count = order_items_silver_df.filter(F.col("order_item_id_int").isNull()).count()
null_product_id_count = order_items_silver_df.filter(F.col("product_id").isNull()).count()
null_seller_id_count = order_items_silver_df.filter(F.col("seller_id").isNull()).count()
null_price_amount_count = order_items_silver_df.filter(F.col("price_amount").isNull()).count()
null_freight_value_amount_count = order_items_silver_df.filter(F.col("freight_value_amount").isNull()).count()
free_shipping_count = order_items_silver_df.filter(F.col("is_free_shipping") == True).count()
invalid_price_count = order_items_silver_df.filter(F.col("has_non_positive_price") == True).count()
negative_freight_count = order_items_silver_df.filter(F.col("has_negative_freight_value") == True).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null order_id count: {null_order_id_count}")
log_step("silver_metrics", "OK", f"Null order_item_id_int count: {null_order_item_id_count}")
log_step("silver_metrics", "OK", f"Null product_id count: {null_product_id_count}")
log_step("silver_metrics", "OK", f"Null seller_id count: {null_seller_id_count}")
log_step("silver_metrics", "OK", f"Null price_amount count: {null_price_amount_count}")
log_step("silver_metrics", "OK", f"Null freight_value_amount count: {null_freight_value_amount_count}")
log_step("silver_metrics", "OK", f"Free shipping count: {free_shipping_count}")
log_step("silver_metrics", "OK", f"Non-positive price count: {invalid_price_count}")
log_step("silver_metrics", "OK", f"Negative freight value count: {negative_freight_count}")

print("Schema da Silver:")
order_items_silver_df.printSchema()

display(order_items_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    order_items_silver_df.write
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
display(persisted_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

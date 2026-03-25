# Databricks notebook source
# MAGIC %md
# MAGIC # 02_01_silver_orders
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_orders`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas de data/hora para timestamp
# MAGIC - Criar variáveis iniciais da jornada do pedido
# MAGIC - Persistir a tabela `silver_orders` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_orders")
dbutils.widgets.text("target_table_name", "silver_orders")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de orders")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
orders_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
orders_silver_base_df = (
    orders_bronze_df
    .withColumn("order_id", null_if_blank("order_id"))
    .withColumn("customer_id", null_if_blank("customer_id"))
    .withColumn("order_status", null_if_blank("order_status"))
    .withColumn("order_purchase_timestamp_ts", sql_try_to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at_ts", sql_try_to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date_ts", sql_try_to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date_ts", sql_try_to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date_ts", sql_try_to_timestamp("order_estimated_delivery_date"))
)

orders_silver_df = (
    orders_silver_base_df
    .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp_ts"))
    .withColumn("order_approved_date", F.to_date("order_approved_at_ts"))
    .withColumn("order_delivered_carrier_date", F.to_date("order_delivered_carrier_date_ts"))
    .withColumn("order_delivered_customer_date", F.to_date("order_delivered_customer_date_ts"))
    .withColumn("order_estimated_delivery_date", F.to_date("order_estimated_delivery_date_ts"))
    .withColumn(
        "purchase_to_approval_hours",
        F.when(
            F.col("order_purchase_timestamp_ts").isNotNull() & F.col("order_approved_at_ts").isNotNull(),
            F.round(
                (F.col("order_approved_at_ts").cast("long") - F.col("order_purchase_timestamp_ts").cast("long")) / F.lit(3600.0),
                2
            )
        )
    )
    .withColumn(
        "approval_to_carrier_hours",
        F.when(
            F.col("order_approved_at_ts").isNotNull() & F.col("order_delivered_carrier_date_ts").isNotNull(),
            F.round(
                (F.col("order_delivered_carrier_date_ts").cast("long") - F.col("order_approved_at_ts").cast("long")) / F.lit(3600.0),
                2
            )
        )
    )
    .withColumn(
        "carrier_to_customer_hours",
        F.when(
            F.col("order_delivered_carrier_date_ts").isNotNull() & F.col("order_delivered_customer_date_ts").isNotNull(),
            F.round(
                (F.col("order_delivered_customer_date_ts").cast("long") - F.col("order_delivered_carrier_date_ts").cast("long")) / F.lit(3600.0),
                2
            )
        )
    )
    .withColumn(
        "purchase_to_delivery_hours",
        F.when(
            F.col("order_purchase_timestamp_ts").isNotNull() & F.col("order_delivered_customer_date_ts").isNotNull(),
            F.round(
                (F.col("order_delivered_customer_date_ts").cast("long") - F.col("order_purchase_timestamp_ts").cast("long")) / F.lit(3600.0),
                2
            )
        )
    )
    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull() & F.col("order_estimated_delivery_date").isNotNull(),
            F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date"))
        )
    )
    .withColumn(
        "is_delivered",
        F.col("order_status") == F.lit("delivered")
    )
    .withColumn(
        "is_canceled",
        F.col("order_status") == F.lit("canceled")
    )
    .withColumn(
        "is_delayed",
        F.when(F.col("delivery_delay_days").isNull(), F.lit(None)).otherwise(F.col("delivery_delay_days") > F.lit(0))
    )
    .withColumn(
        "has_purchase_approval_inconsistency",
        F.when(
            F.col("order_purchase_timestamp_ts").isNotNull() & F.col("order_approved_at_ts").isNotNull(),
            F.col("order_approved_at_ts") < F.col("order_purchase_timestamp_ts")
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "has_approval_carrier_inconsistency",
        F.when(
            F.col("order_approved_at_ts").isNotNull() & F.col("order_delivered_carrier_date_ts").isNotNull(),
            F.col("order_delivered_carrier_date_ts") < F.col("order_approved_at_ts")
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "has_carrier_customer_inconsistency",
        F.when(
            F.col("order_delivered_carrier_date_ts").isNotNull() & F.col("order_delivered_customer_date_ts").isNotNull(),
            F.col("order_delivered_customer_date_ts") < F.col("order_delivered_carrier_date_ts")
        ).otherwise(F.lit(False))
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp_ts",
        "order_purchase_date",
        "order_approved_at_ts",
        "order_approved_date",
        "order_delivered_carrier_date_ts",
        "order_delivered_carrier_date",
        "order_delivered_customer_date_ts",
        "order_delivered_customer_date",
        "order_estimated_delivery_date_ts",
        "order_estimated_delivery_date",
        "purchase_to_approval_hours",
        "approval_to_carrier_hours",
        "carrier_to_customer_hours",
        "purchase_to_delivery_hours",
        "delivery_delay_days",
        "is_delivered",
        "is_canceled",
        "is_delayed",
        "has_purchase_approval_inconsistency",
        "has_approval_carrier_inconsistency",
        "has_carrier_customer_inconsistency",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de orders montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = orders_silver_df.count()
null_order_id_count = orders_silver_df.filter(F.col("order_id").isNull()).count()
null_customer_id_count = orders_silver_df.filter(F.col("customer_id").isNull()).count()
null_purchase_ts_count = orders_silver_df.filter(F.col("order_purchase_timestamp_ts").isNull()).count()
delivered_order_count = orders_silver_df.filter(F.col("is_delivered") == True).count()
delayed_order_count = orders_silver_df.filter(F.col("is_delayed") == True).count()
purchase_approval_inconsistency_count = orders_silver_df.filter(F.col("has_purchase_approval_inconsistency") == True).count()
approval_carrier_inconsistency_count = orders_silver_df.filter(F.col("has_approval_carrier_inconsistency") == True).count()
carrier_customer_inconsistency_count = orders_silver_df.filter(F.col("has_carrier_customer_inconsistency") == True).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null order_id count: {null_order_id_count}")
log_step("silver_metrics", "OK", f"Null customer_id count: {null_customer_id_count}")
log_step("silver_metrics", "OK", f"Null order_purchase_timestamp_ts count: {null_purchase_ts_count}")
log_step("silver_metrics", "OK", f"Delivered order count: {delivered_order_count}")
log_step("silver_metrics", "OK", f"Delayed order count: {delayed_order_count}")
log_step("silver_metrics", "OK", f"Purchase -> approval inconsistencies: {purchase_approval_inconsistency_count}")
log_step("silver_metrics", "OK", f"Approval -> carrier inconsistencies: {approval_carrier_inconsistency_count}")
log_step("silver_metrics", "OK", f"Carrier -> customer inconsistencies: {carrier_customer_inconsistency_count}")

print("Schema da Silver:")
orders_silver_df.printSchema()

display(orders_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    orders_silver_df.write
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

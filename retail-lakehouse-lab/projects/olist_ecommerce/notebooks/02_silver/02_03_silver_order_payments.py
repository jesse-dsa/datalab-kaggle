# Databricks notebook source
# MAGIC %md
# MAGIC # 02_03_silver_order_payments
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_order_payments`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas numéricas
# MAGIC - Criar variáveis iniciais no nível de pagamento do pedido
# MAGIC - Persistir a tabela `silver_order_payments` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_order_payments")
dbutils.widgets.text("target_table_name", "silver_order_payments")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de order_payments")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
payments_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
payments_silver_base_df = (
    payments_bronze_df
    .withColumn("order_id", null_if_blank("order_id"))
    .withColumn("payment_type", F.lower(null_if_blank("payment_type")))
    .withColumn("payment_sequential_int", sql_try_cast("payment_sequential", "int"))
    .withColumn("payment_installments_int", sql_try_cast("payment_installments", "int"))
    .withColumn("payment_value_amount", sql_try_cast("payment_value", "double"))
)

payments_silver_df = (
    payments_silver_base_df
    .withColumn(
        "is_credit_card",
        F.when(F.col("payment_type").isNull(), F.lit(None)).otherwise(F.col("payment_type") == F.lit("credit_card"))
    )
    .withColumn(
        "is_voucher",
        F.when(F.col("payment_type").isNull(), F.lit(None)).otherwise(F.col("payment_type") == F.lit("voucher"))
    )
    .withColumn(
        "is_boleto",
        F.when(F.col("payment_type").isNull(), F.lit(None)).otherwise(F.col("payment_type") == F.lit("boleto"))
    )
    .withColumn(
        "has_installments",
        F.when(F.col("payment_installments_int").isNull(), F.lit(None)).otherwise(F.col("payment_installments_int") > F.lit(1))
    )
    .withColumn(
        "has_invalid_payment_sequential",
        F.col("payment_sequential_int").isNull()
    )
    .withColumn(
        "has_invalid_payment_installments",
        F.col("payment_installments_int").isNull()
    )
    .withColumn(
        "has_non_positive_payment_value",
        F.when(F.col("payment_value_amount").isNull(), F.lit(None)).otherwise(F.col("payment_value_amount") <= F.lit(0))
    )
    .withColumn(
        "has_missing_payment_type",
        F.col("payment_type").isNull()
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "order_id",
        "payment_sequential_int",
        "payment_type",
        "payment_installments_int",
        "payment_value_amount",
        "is_credit_card",
        "is_voucher",
        "is_boleto",
        "has_installments",
        "has_invalid_payment_sequential",
        "has_invalid_payment_installments",
        "has_non_positive_payment_value",
        "has_missing_payment_type",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de order_payments montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = payments_silver_df.count()
null_order_id_count = payments_silver_df.filter(F.col("order_id").isNull()).count()
null_payment_type_count = payments_silver_df.filter(F.col("payment_type").isNull()).count()
null_payment_sequential_count = payments_silver_df.filter(F.col("payment_sequential_int").isNull()).count()
null_payment_installments_count = payments_silver_df.filter(F.col("payment_installments_int").isNull()).count()
null_payment_value_count = payments_silver_df.filter(F.col("payment_value_amount").isNull()).count()
credit_card_count = payments_silver_df.filter(F.col("is_credit_card") == True).count()
voucher_count = payments_silver_df.filter(F.col("is_voucher") == True).count()
boleto_count = payments_silver_df.filter(F.col("is_boleto") == True).count()
installments_count = payments_silver_df.filter(F.col("has_installments") == True).count()
invalid_payment_value_count = payments_silver_df.filter(F.col("has_non_positive_payment_value") == True).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null order_id count: {null_order_id_count}")
log_step("silver_metrics", "OK", f"Null payment_type count: {null_payment_type_count}")
log_step("silver_metrics", "OK", f"Null payment_sequential_int count: {null_payment_sequential_count}")
log_step("silver_metrics", "OK", f"Null payment_installments_int count: {null_payment_installments_count}")
log_step("silver_metrics", "OK", f"Null payment_value_amount count: {null_payment_value_count}")
log_step("silver_metrics", "OK", f"Credit card count: {credit_card_count}")
log_step("silver_metrics", "OK", f"Voucher count: {voucher_count}")
log_step("silver_metrics", "OK", f"Boleto count: {boleto_count}")
log_step("silver_metrics", "OK", f"Payments with installments count: {installments_count}")
log_step("silver_metrics", "OK", f"Non-positive payment value count: {invalid_payment_value_count}")

print("Schema da Silver:")
payments_silver_df.printSchema()

display(payments_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    payments_silver_df.write
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

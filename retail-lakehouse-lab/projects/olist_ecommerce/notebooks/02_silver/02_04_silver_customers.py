# Databricks notebook source
# MAGIC %md
# MAGIC # 02_04_silver_customers
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_customers`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas numéricas básicas
# MAGIC - Criar variáveis iniciais no nível do cliente
# MAGIC - Persistir a tabela `silver_customers` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_customers")
dbutils.widgets.text("target_table_name", "silver_customers")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de customers")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
customers_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
customers_silver_base_df = (
    customers_bronze_df
    .withColumn("customer_id", null_if_blank("customer_id"))
    .withColumn("customer_unique_id", null_if_blank("customer_unique_id"))
    .withColumn("customer_city", F.lower(null_if_blank("customer_city")))
    .withColumn("customer_state", F.upper(null_if_blank("customer_state")))
    .withColumn("customer_zip_code_prefix_int", sql_try_cast("customer_zip_code_prefix", "int"))
)

customers_silver_df = (
    customers_silver_base_df
    .withColumn(
        "has_missing_customer_unique_id",
        F.col("customer_unique_id").isNull()
    )
    .withColumn(
        "has_invalid_zip_code_prefix",
        F.col("customer_zip_code_prefix_int").isNull()
    )
    .withColumn(
        "has_missing_customer_city",
        F.col("customer_city").isNull()
    )
    .withColumn(
        "has_missing_customer_state",
        F.col("customer_state").isNull()
    )
    .withColumn(
        "is_valid_brazil_state_code",
        F.when(
            F.col("customer_state").isNull(),
            F.lit(None)
        ).otherwise(
            F.col("customer_state").isin(
                "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
                "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
                "SP","SE","TO"
            )
        )
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix_int",
        "customer_city",
        "customer_state",
        "has_missing_customer_unique_id",
        "has_invalid_zip_code_prefix",
        "has_missing_customer_city",
        "has_missing_customer_state",
        "is_valid_brazil_state_code",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de customers montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = customers_silver_df.count()
null_customer_id_count = customers_silver_df.filter(F.col("customer_id").isNull()).count()
null_customer_unique_id_count = customers_silver_df.filter(F.col("customer_unique_id").isNull()).count()
null_zip_code_prefix_count = customers_silver_df.filter(F.col("customer_zip_code_prefix_int").isNull()).count()
null_customer_city_count = customers_silver_df.filter(F.col("customer_city").isNull()).count()
null_customer_state_count = customers_silver_df.filter(F.col("customer_state").isNull()).count()
invalid_state_code_count = customers_silver_df.filter(F.col("is_valid_brazil_state_code") == False).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null customer_id count: {null_customer_id_count}")
log_step("silver_metrics", "OK", f"Null customer_unique_id count: {null_customer_unique_id_count}")
log_step("silver_metrics", "OK", f"Null customer_zip_code_prefix_int count: {null_zip_code_prefix_count}")
log_step("silver_metrics", "OK", f"Null customer_city count: {null_customer_city_count}")
log_step("silver_metrics", "OK", f"Null customer_state count: {null_customer_state_count}")
log_step("silver_metrics", "OK", f"Invalid Brazil state code count: {invalid_state_code_count}")

print("Schema da Silver:")
customers_silver_df.printSchema()

display(customers_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    customers_silver_df.write
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

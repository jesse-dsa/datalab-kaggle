# Databricks notebook source
# MAGIC %md
# MAGIC # 03_02_gold_dim_customers
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `silver_customers`
# MAGIC - Padronizar a dimensão de clientes para consumo analítico
# MAGIC - Criar chaves e agrupamentos úteis para análise geográfica
# MAGIC - Persistir a tabela `gold_dim_customers` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "silver_customers")
dbutils.widgets.text("target_table_name", "gold_dim_customers")
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

# DBTITLE 1,Validação da tabela Silver
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Silver de customers")

# COMMAND ----------

# DBTITLE 1,Leitura da Silver
customers_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("silver_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da dimensão Gold
gold_dim_customers_df = (
    customers_df
    .withColumn("customer_key", F.col("customer_id"))
    .withColumn(
        "customer_location_key",
        F.concat_ws(
            "_",
            F.coalesce(F.col("customer_state"), F.lit("unknown")),
            F.coalesce(F.col("customer_city"), F.lit("unknown")),
            F.coalesce(F.col("customer_zip_code_prefix_int").cast("string"), F.lit("unknown"))
        )
    )
    .withColumn(
        "customer_city_state",
        F.concat_ws(
            " - ",
            F.coalesce(F.col("customer_city"), F.lit("unknown")),
            F.coalesce(F.col("customer_state"), F.lit("unknown"))
        )
    )
    .withColumn(
        "customer_zip_code_prefix_band",
        F.when(F.col("customer_zip_code_prefix_int").isNull(), F.lit("unknown"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(10000), F.lit("00000_09999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(20000), F.lit("10000_19999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(30000), F.lit("20000_29999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(40000), F.lit("30000_39999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(50000), F.lit("40000_49999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(60000), F.lit("50000_59999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(70000), F.lit("60000_69999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(80000), F.lit("70000_79999"))
         .when(F.col("customer_zip_code_prefix_int") < F.lit(90000), F.lit("80000_89999"))
         .otherwise(F.lit("90000_plus"))
    )
    .withColumn(
        "customer_data_quality_status",
        F.when(
            (F.col("has_invalid_zip_code_prefix") == True) |
            (F.col("has_missing_customer_city") == True) |
            (F.col("has_missing_customer_state") == True) |
            (F.col("is_valid_brazil_state_code") == False),
            F.lit("needs_attention")
        ).otherwise(F.lit("ok"))
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "customer_key",
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix_int",
        "customer_zip_code_prefix_band",
        "customer_city",
        "customer_state",
        "customer_city_state",
        "customer_location_key",
        "has_missing_customer_unique_id",
        "has_invalid_zip_code_prefix",
        "has_missing_customer_city",
        "has_missing_customer_state",
        "is_valid_brazil_state_code",
        "customer_data_quality_status",
        "_ingestion_timestamp",
        "_silver_processed_timestamp",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de dim_customers montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_dim_customers_df.count()
distinct_customer_count = gold_dim_customers_df.select("customer_id").distinct().count()
distinct_unique_customer_count = gold_dim_customers_df.select("customer_unique_id").distinct().count()
needs_attention_count = gold_dim_customers_df.filter(F.col("customer_data_quality_status") == "needs_attention").count()
missing_state_count = gold_dim_customers_df.filter(F.col("has_missing_customer_state") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct customer_id count: {distinct_customer_count}")
log_step("gold_metrics", "OK", f"Distinct customer_unique_id count: {distinct_unique_customer_count}")
log_step("gold_metrics", "OK", f"Rows with needs_attention status: {needs_attention_count}")
log_step("gold_metrics", "OK", f"Rows with missing customer_state: {missing_state_count}")

print("Schema da Gold:")
gold_dim_customers_df.printSchema()

display(gold_dim_customers_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_dim_customers_df.write
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

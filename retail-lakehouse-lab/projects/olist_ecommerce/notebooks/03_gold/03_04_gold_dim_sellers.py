# Databricks notebook source
# MAGIC %md
# MAGIC # 03_04_gold_dim_sellers
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `silver_sellers`
# MAGIC - Padronizar a dimensão de sellers para consumo analítico
# MAGIC - Criar agrupamentos úteis para análise geográfica e de qualidade
# MAGIC - Persistir a tabela `gold_dim_sellers` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "silver_sellers")
dbutils.widgets.text("target_table_name", "gold_dim_sellers")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Silver de sellers")

# COMMAND ----------

# DBTITLE 1,Leitura da Silver
sellers_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("silver_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da dimensão Gold
gold_dim_sellers_df = (
    sellers_df
    .withColumn("seller_key", F.col("seller_id"))
    .withColumn(
        "seller_location_key",
        F.concat_ws(
            "_",
            F.coalesce(F.col("seller_state"), F.lit("unknown")),
            F.coalesce(F.col("seller_city"), F.lit("unknown")),
            F.coalesce(F.col("seller_zip_code_prefix_int").cast("string"), F.lit("unknown"))
        )
    )
    .withColumn(
        "seller_city_state",
        F.concat_ws(
            " - ",
            F.coalesce(F.col("seller_city"), F.lit("unknown")),
            F.coalesce(F.col("seller_state"), F.lit("unknown"))
        )
    )
    .withColumn(
        "seller_zip_code_prefix_band",
        F.when(F.col("seller_zip_code_prefix_int").isNull(), F.lit("unknown"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(10000), F.lit("00000_09999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(20000), F.lit("10000_19999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(30000), F.lit("20000_29999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(40000), F.lit("30000_39999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(50000), F.lit("40000_49999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(60000), F.lit("50000_59999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(70000), F.lit("60000_69999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(80000), F.lit("70000_79999"))
         .when(F.col("seller_zip_code_prefix_int") < F.lit(90000), F.lit("80000_89999"))
         .otherwise(F.lit("90000_plus"))
    )
    .withColumn(
        "seller_data_quality_status",
        F.when(
            (F.col("has_invalid_zip_code_prefix") == True) |
            (F.col("has_missing_seller_city") == True) |
            (F.col("has_missing_seller_state") == True) |
            (F.col("is_valid_brazil_state_code") == False),
            F.lit("needs_attention")
        ).otherwise(F.lit("ok"))
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "seller_key",
        "seller_id",
        "seller_zip_code_prefix_int",
        "seller_zip_code_prefix_band",
        "seller_city",
        "seller_state",
        "seller_city_state",
        "seller_location_key",
        "has_invalid_zip_code_prefix",
        "has_missing_seller_city",
        "has_missing_seller_state",
        "is_valid_brazil_state_code",
        "seller_data_quality_status",
        "_ingestion_timestamp",
        "_silver_processed_timestamp",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de dim_sellers montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_dim_sellers_df.count()
distinct_seller_count = gold_dim_sellers_df.select("seller_id").distinct().count()
needs_attention_count = gold_dim_sellers_df.filter(F.col("seller_data_quality_status") == "needs_attention").count()
missing_state_count = gold_dim_sellers_df.filter(F.col("has_missing_seller_state") == True).count()
invalid_zip_count = gold_dim_sellers_df.filter(F.col("has_invalid_zip_code_prefix") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct seller_id count: {distinct_seller_count}")
log_step("gold_metrics", "OK", f"Rows with needs_attention status: {needs_attention_count}")
log_step("gold_metrics", "OK", f"Rows with missing seller_state: {missing_state_count}")
log_step("gold_metrics", "OK", f"Rows with invalid zip code prefix: {invalid_zip_count}")

print("Schema da Gold:")
gold_dim_sellers_df.printSchema()

display(gold_dim_sellers_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_dim_sellers_df.write
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

# Databricks notebook source
# MAGIC %md
# MAGIC # 03_03_gold_dim_products
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `silver_products`
# MAGIC - Padronizar a dimensão de produtos para consumo analítico
# MAGIC - Criar agrupamentos úteis para análise de categoria, volume e peso
# MAGIC - Persistir a tabela `gold_dim_products` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "silver_products")
dbutils.widgets.text("target_table_name", "gold_dim_products")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Silver de products")

# COMMAND ----------

# DBTITLE 1,Leitura da Silver
products_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("silver_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da dimensão Gold
gold_dim_products_df = (
    products_df
    .withColumn("product_key", F.col("product_id"))
    .withColumn(
        "product_size_group",
        F.when(F.col("product_volume_cm3").isNull(), F.lit("unknown"))
         .when(F.col("product_volume_cm3") < F.lit(1000), F.lit("small"))
         .when(F.col("product_volume_cm3") < F.lit(10000), F.lit("medium"))
         .when(F.col("product_volume_cm3") < F.lit(50000), F.lit("large"))
         .otherwise(F.lit("extra_large"))
    )
    .withColumn(
        "product_weight_group",
        F.when(F.col("product_weight_g").isNull(), F.lit("unknown"))
         .when(F.col("product_weight_g") < F.lit(500), F.lit("light"))
         .when(F.col("product_weight_g") < F.lit(2000), F.lit("medium"))
         .when(F.col("product_weight_g") < F.lit(5000), F.lit("heavy"))
         .otherwise(F.lit("extra_heavy"))
    )
    .withColumn(
        "product_photo_group",
        F.when(F.col("product_photos_qty").isNull(), F.lit("unknown"))
         .when(F.col("product_photos_qty") == F.lit(0), F.lit("no_photo"))
         .when(F.col("product_photos_qty") == F.lit(1), F.lit("one_photo"))
         .when(F.col("product_photos_qty") <= F.lit(3), F.lit("few_photos"))
         .otherwise(F.lit("many_photos"))
    )
    .withColumn(
        "product_data_quality_status",
        F.when(
            (F.col("has_missing_product_category_name") == True) |
            (F.col("has_invalid_dimensions") == True) |
            (F.col("has_non_positive_weight_g") == True) |
            (F.col("has_non_positive_dimensions") == True),
            F.lit("needs_attention")
        ).otherwise(F.lit("ok"))
    )
    .withColumn("_gold_processed_timestamp", F.current_timestamp())
    .select(
        "product_key",
        "product_id",
        "product_category_name",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_photo_group",
        "product_weight_g",
        "product_weight_group",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "product_size_group",
        "has_missing_product_category_name",
        "has_invalid_product_name_length",
        "has_invalid_product_description_length",
        "has_invalid_product_photos_qty",
        "has_invalid_product_weight_g",
        "has_invalid_dimensions",
        "has_non_positive_weight_g",
        "has_non_positive_dimensions",
        "product_data_quality_status",
        "_ingestion_timestamp",
        "_silver_processed_timestamp",
        "_gold_processed_timestamp"
    )
)

log_step("gold_dataframe", "OK", "DataFrame gold de dim_products montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
gold_row_count = gold_dim_products_df.count()
distinct_product_count = gold_dim_products_df.select("product_id").distinct().count()
needs_attention_count = gold_dim_products_df.filter(F.col("product_data_quality_status") == "needs_attention").count()
missing_category_count = gold_dim_products_df.filter(F.col("has_missing_product_category_name") == True).count()
invalid_dimensions_count = gold_dim_products_df.filter(F.col("has_invalid_dimensions") == True).count()

log_step("gold_metrics", "OK", f"Row count: {gold_row_count}")
log_step("gold_metrics", "OK", f"Distinct product_id count: {distinct_product_count}")
log_step("gold_metrics", "OK", f"Rows with needs_attention status: {needs_attention_count}")
log_step("gold_metrics", "OK", f"Rows with missing product_category_name: {missing_category_count}")
log_step("gold_metrics", "OK", f"Rows with invalid dimensions: {invalid_dimensions_count}")

print("Schema da Gold:")
gold_dim_products_df.printSchema()

display(gold_dim_products_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Gold em Delta
(
    gold_dim_products_df.write
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

# Databricks notebook source
# MAGIC %md
# MAGIC # 02_05_silver_products
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_products`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas numéricas
# MAGIC - Criar variáveis iniciais no nível do produto
# MAGIC - Persistir a tabela `silver_products` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_products")
dbutils.widgets.text("target_table_name", "silver_products")
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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de products")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
products_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
products_silver_base_df = (
    products_bronze_df
    .withColumn("product_id", null_if_blank("product_id"))
    .withColumn("product_category_name", F.lower(null_if_blank("product_category_name")))
    .withColumn("product_name_length", sql_try_cast("product_name_lenght", "int"))
    .withColumn("product_description_length", sql_try_cast("product_description_lenght", "int"))
    .withColumn("product_photos_qty", sql_try_cast("product_photos_qty", "int"))
    .withColumn("product_weight_g", sql_try_cast("product_weight_g", "double"))
    .withColumn("product_length_cm", sql_try_cast("product_length_cm", "double"))
    .withColumn("product_height_cm", sql_try_cast("product_height_cm", "double"))
    .withColumn("product_width_cm", sql_try_cast("product_width_cm", "double"))
)

products_silver_df = (
    products_silver_base_df
    .withColumn(
        "product_volume_cm3",
        F.when(
            F.col("product_length_cm").isNotNull() &
            F.col("product_height_cm").isNotNull() &
            F.col("product_width_cm").isNotNull(),
            F.round(
                F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"),
                2
            )
        )
    )
    .withColumn(
        "has_missing_product_category_name",
        F.col("product_category_name").isNull()
    )
    .withColumn(
        "has_invalid_product_name_length",
        F.col("product_name_length").isNull()
    )
    .withColumn(
        "has_invalid_product_description_length",
        F.col("product_description_length").isNull()
    )
    .withColumn(
        "has_invalid_product_photos_qty",
        F.col("product_photos_qty").isNull()
    )
    .withColumn(
        "has_invalid_product_weight_g",
        F.col("product_weight_g").isNull()
    )
    .withColumn(
        "has_invalid_dimensions",
        F.col("product_length_cm").isNull() |
        F.col("product_height_cm").isNull() |
        F.col("product_width_cm").isNull()
    )
    .withColumn(
        "has_non_positive_weight_g",
        F.when(F.col("product_weight_g").isNull(), F.lit(None)).otherwise(F.col("product_weight_g") <= F.lit(0))
    )
    .withColumn(
        "has_non_positive_dimensions",
        F.when(
            F.col("product_length_cm").isNull() |
            F.col("product_height_cm").isNull() |
            F.col("product_width_cm").isNull(),
            F.lit(None)
        ).otherwise(
            (F.col("product_length_cm") <= F.lit(0)) |
            (F.col("product_height_cm") <= F.lit(0)) |
            (F.col("product_width_cm") <= F.lit(0))
        )
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "product_id",
        "product_category_name",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "has_missing_product_category_name",
        "has_invalid_product_name_length",
        "has_invalid_product_description_length",
        "has_invalid_product_photos_qty",
        "has_invalid_product_weight_g",
        "has_invalid_dimensions",
        "has_non_positive_weight_g",
        "has_non_positive_dimensions",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de products montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = products_silver_df.count()
null_product_id_count = products_silver_df.filter(F.col("product_id").isNull()).count()
null_product_category_count = products_silver_df.filter(F.col("product_category_name").isNull()).count()
null_product_name_length_count = products_silver_df.filter(F.col("product_name_length").isNull()).count()
null_product_description_length_count = products_silver_df.filter(F.col("product_description_length").isNull()).count()
null_product_photos_qty_count = products_silver_df.filter(F.col("product_photos_qty").isNull()).count()
null_product_weight_count = products_silver_df.filter(F.col("product_weight_g").isNull()).count()
invalid_dimensions_count = products_silver_df.filter(F.col("has_invalid_dimensions") == True).count()
non_positive_weight_count = products_silver_df.filter(F.col("has_non_positive_weight_g") == True).count()
non_positive_dimensions_count = products_silver_df.filter(F.col("has_non_positive_dimensions") == True).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null product_id count: {null_product_id_count}")
log_step("silver_metrics", "OK", f"Null product_category_name count: {null_product_category_count}")
log_step("silver_metrics", "OK", f"Null product_name_length count: {null_product_name_length_count}")
log_step("silver_metrics", "OK", f"Null product_description_length count: {null_product_description_length_count}")
log_step("silver_metrics", "OK", f"Null product_photos_qty count: {null_product_photos_qty_count}")
log_step("silver_metrics", "OK", f"Null product_weight_g count: {null_product_weight_count}")
log_step("silver_metrics", "OK", f"Invalid dimensions count: {invalid_dimensions_count}")
log_step("silver_metrics", "OK", f"Non-positive weight count: {non_positive_weight_count}")
log_step("silver_metrics", "OK", f"Non-positive dimensions count: {non_positive_dimensions_count}")

print("Schema da Silver:")
products_silver_df.printSchema()

display(products_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    products_silver_df.write
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

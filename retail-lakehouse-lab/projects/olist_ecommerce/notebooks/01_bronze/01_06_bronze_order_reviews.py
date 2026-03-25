# Databricks notebook source
# MAGIC %md
# MAGIC # 01_06_bronze_order_reviews
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler o arquivo bruto `olist_order_reviews_dataset.csv` da raw zone
# MAGIC - Preservar a estrutura original da fonte
# MAGIC - Adicionar metadados técnicos de ingestão
# MAGIC - Persistir a tabela `bronze_order_reviews` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("volume_name", "raw_files")
dbutils.widgets.text("project_slug", "olist_ecommerce")
dbutils.widgets.text("raw_subdir", "downloads")
dbutils.widgets.text("source_file_name", "olist_order_reviews_dataset.csv")
dbutils.widgets.text("target_table_name", "bronze_order_reviews")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
VOLUME_NAME = dbutils.widgets.get("volume_name").strip()
PROJECT_SLUG = dbutils.widgets.get("project_slug").strip()
RAW_SUBDIR = dbutils.widgets.get("raw_subdir").strip()
SOURCE_FILE_NAME = dbutils.widgets.get("source_file_name").strip()
TARGET_TABLE_NAME = dbutils.widgets.get("target_table_name").strip()
WRITE_MODE = dbutils.widgets.get("write_mode").strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{TARGET_TABLE_NAME}"

RAW_PROJECT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/{PROJECT_SLUG}"
RAW_DOWNLOADS_PATH = f"{RAW_PROJECT_PATH}/{RAW_SUBDIR}"
SOURCE_FILE_PATH = f"{RAW_DOWNLOADS_PATH}/{SOURCE_FILE_NAME}"

print("Parâmetros carregados com sucesso.")
print(f"FULL_SCHEMA_NAME = {FULL_SCHEMA_NAME}")
print(f"FULL_TABLE_NAME = {FULL_TABLE_NAME}")
print(f"RAW_DOWNLOADS_PATH = {RAW_DOWNLOADS_PATH}")
print(f"SOURCE_FILE_PATH = {SOURCE_FILE_PATH}")
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


def ensure_path_exists(path: str, description: str) -> None:
    try:
        dbutils.fs.ls(path)
        log_step("path_validation", "OK", f"{description} encontrado: {path}")
    except Exception as exc:
        raise FileNotFoundError(f"{description} não encontrado em {path}") from exc


def ensure_source_file_exists(downloads_path: str, source_file_name: str, source_file_path: str) -> None:
    try:
        available_file_infos = dbutils.fs.ls(downloads_path)
    except Exception as exc:
        raise FileNotFoundError(
            f"A pasta de downloads não foi encontrada em {downloads_path}. "
            "Confirme se os notebooks 00_00_project_setup e 00_01_source_download_olist foram executados com sucesso."
        ) from exc

    available_file_names = [file_info.name.rstrip("/") for file_info in available_file_infos]
    source_exists = source_file_name in available_file_names

    if not source_exists:
        raise FileNotFoundError(
            f"Arquivo de origem não encontrado em {source_file_path}. "
            f"Arquivos encontrados em downloads: {', '.join(sorted(available_file_names)) if available_file_names else '<nenhum>'}. "
            "Confirme se o notebook 00_01_source_download_olist foi executado com sucesso."
        )

    log_step("source_validation", "OK", f"Arquivo de origem encontrado: {source_file_path}")


# COMMAND ----------

# DBTITLE 1,Validação de paths
ensure_path_exists(RAW_PROJECT_PATH, "Raw project path")
ensure_path_exists(RAW_DOWNLOADS_PATH, "Raw downloads path")
ensure_source_file_exists(RAW_DOWNLOADS_PATH, SOURCE_FILE_NAME, SOURCE_FILE_PATH)

# COMMAND ----------

# DBTITLE 1,Uso do catálogo e schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

log_step("context_setup", "OK", f"Contexto definido para {FULL_SCHEMA_NAME}")

# COMMAND ----------

# DBTITLE 1,Leitura do CSV e montagem da Bronze
reviews_raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "false")
    .load(SOURCE_FILE_PATH)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file_path"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modification_time"),
    )
)

reviews_bronze_df = (
    reviews_raw_df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

log_step("bronze_dataframe", "OK", "DataFrame bronze de order_reviews montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
bronze_row_count = reviews_bronze_df.count()
null_review_id_count = reviews_bronze_df.filter(F.col("review_id").isNull()).count()
empty_review_id_count = reviews_bronze_df.filter(F.trim(F.coalesce(F.col("review_id"), F.lit(""))) == "").count()
null_order_id_count = reviews_bronze_df.filter(F.col("order_id").isNull()).count()
empty_order_id_count = reviews_bronze_df.filter(F.trim(F.coalesce(F.col("order_id"), F.lit(""))) == "").count()

log_step("bronze_metrics", "OK", f"Row count: {bronze_row_count}")
log_step("bronze_metrics", "OK", f"Null review_id count: {null_review_id_count}")
log_step("bronze_metrics", "OK", f"Empty review_id count: {empty_review_id_count}")
log_step("bronze_metrics", "OK", f"Null order_id count: {null_order_id_count}")
log_step("bronze_metrics", "OK", f"Empty order_id count: {empty_order_id_count}")

print("Schema da Bronze:")
reviews_bronze_df.printSchema()

display(reviews_bronze_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Bronze em Delta
(
    reviews_bronze_df.write
    .format("delta")
    .mode(WRITE_MODE)
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

log_step("delta_write", "OK", f"Tabela gravada com sucesso: {FULL_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Evidências pós-gravação
persisted_df = spark.table(FULL_TABLE_NAME)
persisted_row_count = persisted_df.count()

log_step("post_write_validation", "OK", f"Persisted row count: {persisted_row_count}")

print(f"Tabela criada/atualizada: {FULL_TABLE_NAME}")
display(persisted_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TABLE_NAME}"))

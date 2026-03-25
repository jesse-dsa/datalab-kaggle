# Databricks notebook source
# MAGIC %md
# MAGIC # 01_02_bronze_order_items
# MAGIC
# MAGIC Este notebook cria a tabela Bronze `bronze_order_items` a partir do arquivo
# MAGIC `olist_order_items_dataset.csv` já copiado para a raw zone.
# MAGIC
# MAGIC ## O que ele faz
# MAGIC - Valida a presença do arquivo na pasta `downloads`
# MAGIC - Lê o CSV com Spark a partir de um Unity Catalog volume
# MAGIC - Seleciona campos específicos de `_metadata`
# MAGIC - Adiciona timestamp técnico de ingestão
# MAGIC - Persiste a tabela `bronze_order_items` em Delta
# MAGIC - Exibe evidências básicas de carga
# MAGIC
# MAGIC ## O que ele não faz
# MAGIC - Não corrige tipos analíticos nem datas
# MAGIC - Não cria colunas derivadas de negócio
# MAGIC - Não faz joins com outras entidades

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros

# COMMAND ----------

import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from pyspark.sql import Row, functions as F


def normalize_identifier(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        raise ValueError("O identificador não pode ficar vazio após normalização.")
    return value


DEFAULTS: Dict[str, Any] = {
    "catalog_name": "olist",
    "schema_name": "dev",
    "volume_name": "raw_files",
    "project_slug": "olist_ecommerce",
    "source_file_name": "olist_order_items_dataset.csv",
    "target_table_name": "bronze_order_items",
    "write_mode": "overwrite",
}

try:
    dbutils.widgets.text("catalog_name", DEFAULTS["catalog_name"])
    dbutils.widgets.text("schema_name", DEFAULTS["schema_name"])
    dbutils.widgets.text("volume_name", DEFAULTS["volume_name"])
    dbutils.widgets.text("project_slug", DEFAULTS["project_slug"])
    dbutils.widgets.text("source_file_name", DEFAULTS["source_file_name"])
    dbutils.widgets.text("target_table_name", DEFAULTS["target_table_name"])
    dbutils.widgets.dropdown("write_mode", DEFAULTS["write_mode"], ["overwrite", "append"])

    params = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "project_slug": dbutils.widgets.get("project_slug"),
        "source_file_name": dbutils.widgets.get("source_file_name"),
        "target_table_name": dbutils.widgets.get("target_table_name"),
        "write_mode": dbutils.widgets.get("write_mode"),
    }
except NameError:
    params = DEFAULTS.copy()

CATALOG_NAME = normalize_identifier(params["catalog_name"])
SCHEMA_NAME = normalize_identifier(params["schema_name"])
VOLUME_NAME = normalize_identifier(params["volume_name"])
PROJECT_SLUG = normalize_identifier(params["project_slug"])
SOURCE_FILE_NAME = str(params["source_file_name"]).strip()
TARGET_TABLE_NAME = normalize_identifier(params["target_table_name"])
WRITE_MODE = str(params["write_mode"]).strip().lower()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{TARGET_TABLE_NAME}"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
RAW_PROJECT_PATH = f"{VOLUME_PATH}/{PROJECT_SLUG}"
RAW_DOWNLOADS_PATH = f"{RAW_PROJECT_PATH}/downloads"
SOURCE_FILE_PATH = f"{RAW_DOWNLOADS_PATH}/{SOURCE_FILE_NAME}"

config_rows = [
    Row(parameter="catalog_name", value=CATALOG_NAME),
    Row(parameter="schema_name", value=SCHEMA_NAME),
    Row(parameter="volume_name", value=VOLUME_NAME),
    Row(parameter="project_slug", value=PROJECT_SLUG),
    Row(parameter="raw_downloads_path", value=RAW_DOWNLOADS_PATH),
    Row(parameter="source_file_name", value=SOURCE_FILE_NAME),
    Row(parameter="source_file_path", value=SOURCE_FILE_PATH),
    Row(parameter="target_table_name", value=TARGET_TABLE_NAME),
    Row(parameter="full_table_name", value=FULL_TABLE_NAME),
    Row(parameter="write_mode", value=WRITE_MODE),
]

display(spark.createDataFrame(config_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções auxiliares

# COMMAND ----------

execution_log: List[Row] = []


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log_step(step: str, status: str, detail: str) -> None:
    execution_log.append(
        Row(
            timestamp=utc_now_iso(),
            step=step,
            status=status,
            detail=detail,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação da origem

# COMMAND ----------

try:
    available_file_infos = dbutils.fs.ls(RAW_DOWNLOADS_PATH)
    available_file_names = sorted([f.name.rstrip("/") for f in available_file_infos])
except Exception as exc:
    raise FileNotFoundError(
        f"A pasta de downloads não foi encontrada em {RAW_DOWNLOADS_PATH}. "
        "Confirme se os notebooks 00_00_project_setup e 00_01_source_download_olist foram executados com sucesso."
    ) from exc

if SOURCE_FILE_NAME not in available_file_names:
    raise FileNotFoundError(
        f"Arquivo de origem não encontrado em {SOURCE_FILE_PATH}. "
        f"Arquivos encontrados em downloads: {', '.join(available_file_names) if available_file_names else '<nenhum>'}."
    )

log_step("source_validation", "OK", f"Arquivo encontrado: {SOURCE_FILE_PATH}")
display(spark.createDataFrame([Row(file_name=name) for name in available_file_names]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura do CSV e montagem da Bronze
# MAGIC
# MAGIC Em Unity Catalog, `input_file_name()` não é suportado. Por isso, os campos
# MAGIC de origem do arquivo são obtidos via `_metadata`.

# COMMAND ----------

order_items_raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
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

order_items_bronze_df = (
    order_items_raw_df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

log_step("read_csv", "OK", f"CSV lido com sucesso a partir de {SOURCE_FILE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidências pré-gravação

# COMMAND ----------

bronze_row_count = order_items_bronze_df.count()
null_order_id_count = order_items_bronze_df.filter(F.col("order_id").isNull()).count()
empty_order_id_count = order_items_bronze_df.filter(F.trim(F.coalesce(F.col("order_id"), F.lit(""))) == "").count()
null_order_item_id_count = order_items_bronze_df.filter(F.col("order_item_id").isNull()).count()
empty_product_id_count = order_items_bronze_df.filter(F.trim(F.coalesce(F.col("product_id"), F.lit(""))) == "").count()
empty_seller_id_count = order_items_bronze_df.filter(F.trim(F.coalesce(F.col("seller_id"), F.lit(""))) == "").count()


evidence_rows = [
    Row(metric="bronze_row_count", value=str(bronze_row_count)),
    Row(metric="null_order_id_count", value=str(null_order_id_count)),
    Row(metric="empty_order_id_count", value=str(empty_order_id_count)),
    Row(metric="null_order_item_id_count", value=str(null_order_item_id_count)),
    Row(metric="empty_product_id_count", value=str(empty_product_id_count)),
    Row(metric="empty_seller_id_count", value=str(empty_seller_id_count)),
]

display(spark.createDataFrame(evidence_rows))
display(order_items_bronze_df.limit(10))
order_items_bronze_df.printSchema()

log_step(
    "pre_write_checks",
    "OK",
    (
        f"Linhas={bronze_row_count}; order_id nulos={null_order_id_count}; "
        f"order_id vazios={empty_order_id_count}; order_item_id nulos={null_order_item_id_count}; "
        f"product_id vazios={empty_product_id_count}; seller_id vazios={empty_seller_id_count}"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persistência em Delta

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")

writer = (
    order_items_bronze_df.write
    .format("delta")
    .mode(WRITE_MODE)
)

if WRITE_MODE == "overwrite":
    writer = writer.option("overwriteSchema", "true")

writer.saveAsTable(FULL_TABLE_NAME)

log_step("write_table", "OK", f"Tabela gravada com sucesso: {FULL_TABLE_NAME} (mode={WRITE_MODE})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Evidências pós-gravação

# COMMAND ----------

persisted_df = spark.table(FULL_TABLE_NAME)
persisted_row_count = persisted_df.count()

post_rows = [
    Row(metric="persisted_row_count", value=str(persisted_row_count)),
    Row(metric="table_name", value=FULL_TABLE_NAME),
    Row(metric="source_file_name", value=SOURCE_FILE_NAME),
]

display(spark.createDataFrame(post_rows))
display(persisted_df.limit(10))

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {FULL_TABLE_NAME}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Log de execução

# COMMAND ----------

if execution_log:
    display(spark.createDataFrame(execution_log))
else:
    display(spark.createDataFrame([Row(timestamp=utc_now_iso(), step="log", status="INFO", detail="Nenhum evento registrado")]))

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # 00_00_project_setup
# MAGIC
# MAGIC Este notebook prepara a fundação do projeto no Databricks.
# MAGIC
# MAGIC ## O que ele faz
# MAGIC - Define parâmetros base do projeto
# MAGIC - Cria ou utiliza catálogo
# MAGIC - Cria schema
# MAGIC - Cria volume gerenciado no Unity Catalog
# MAGIC - Cria a raw zone oficial do projeto dentro de `/Volumes/...`
# MAGIC - Exibe evidências do ambiente preparado
# MAGIC
# MAGIC ## O que ele não faz
# MAGIC - Não baixa dados do Kaggle
# MAGIC - Não cria tabelas Bronze
# MAGIC - Não faz limpeza ou transformação de dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros do projeto

# COMMAND ----------

import re
from datetime import datetime
from typing import Dict, Any, List

from pyspark.sql import Row


def normalize_identifier(value: str) -> str:
    """Normaliza nomes para padrões estáveis em snake_case."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        raise ValueError("O identificador não pode ficar vazio após normalização.")
    return value


DEFAULTS: Dict[str, Any] = {
    "catalog_name": "olist",
    "schema_name": "dev",
    "volume_name": "raw_files",
    "project_slug": "olist_ecommerce",
    "create_catalog_if_missing": "true",
    "volume_comment": "Managed volume for raw files used by the Olist e-commerce lakehouse project",
}

# Widgets facilitam ajustes sem editar o código no Databricks.
try:
    dbutils.widgets.text("catalog_name", DEFAULTS["catalog_name"])
    dbutils.widgets.text("schema_name", DEFAULTS["schema_name"])
    dbutils.widgets.text("volume_name", DEFAULTS["volume_name"])
    dbutils.widgets.text("project_slug", DEFAULTS["project_slug"])
    dbutils.widgets.dropdown(
        "create_catalog_if_missing",
        DEFAULTS["create_catalog_if_missing"],
        ["true", "false"],
    )
    dbutils.widgets.text("volume_comment", DEFAULTS["volume_comment"])

    params = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "project_slug": dbutils.widgets.get("project_slug"),
        "create_catalog_if_missing": dbutils.widgets.get("create_catalog_if_missing"),
        "volume_comment": dbutils.widgets.get("volume_comment"),
    }
except NameError:
    # Fallback caso o código seja lido fora do Databricks.
    params = DEFAULTS.copy()

CATALOG_NAME = normalize_identifier(params["catalog_name"])
SCHEMA_NAME = normalize_identifier(params["schema_name"])
VOLUME_NAME = normalize_identifier(params["volume_name"])
PROJECT_SLUG = normalize_identifier(params["project_slug"])
CREATE_CATALOG_IF_MISSING = str(params["create_catalog_if_missing"]).lower() == "true"
VOLUME_COMMENT = str(params["volume_comment"]).strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_VOLUME_NAME = f"{FULL_SCHEMA_NAME}.{VOLUME_NAME}"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
RAW_PROJECT_PATH = f"{VOLUME_PATH}/{PROJECT_SLUG}"
RAW_DOWNLOADS_PATH = f"{RAW_PROJECT_PATH}/downloads"
RAW_ARCHIVE_PATH = f"{RAW_PROJECT_PATH}/archive"
RAW_LOGS_PATH = f"{RAW_PROJECT_PATH}/logs"

config_rows = [
    Row(parameter="catalog_name", value=CATALOG_NAME),
    Row(parameter="schema_name", value=SCHEMA_NAME),
    Row(parameter="volume_name", value=VOLUME_NAME),
    Row(parameter="project_slug", value=PROJECT_SLUG),
    Row(parameter="full_schema_name", value=FULL_SCHEMA_NAME),
    Row(parameter="full_volume_name", value=FULL_VOLUME_NAME),
    Row(parameter="volume_path", value=VOLUME_PATH),
    Row(parameter="raw_project_path", value=RAW_PROJECT_PATH),
    Row(parameter="raw_downloads_path", value=RAW_DOWNLOADS_PATH),
    Row(parameter="raw_archive_path", value=RAW_ARCHIVE_PATH),
    Row(parameter="raw_logs_path", value=RAW_LOGS_PATH),
    Row(parameter="create_catalog_if_missing", value=str(CREATE_CATALOG_IF_MISSING)),
]

display(spark.createDataFrame(config_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções auxiliares

# COMMAND ----------

execution_log: List[Row] = []


def log_step(step: str, status: str, detail: str, statement: str = "") -> None:
    execution_log.append(
        Row(
            timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            step=step,
            status=status,
            detail=detail,
            statement=statement,
        )
    )



def execute_sql(step: str, statement: str, ignore_error: bool = False) -> None:
    try:
        spark.sql(statement)
        log_step(step, "OK", "SQL executado com sucesso", statement)
    except Exception as exc:
        message = str(exc)
        status = "WARN" if ignore_error else "ERROR"
        log_step(step, status, message, statement)
        if not ignore_error:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparação do catálogo, schema e volume

# COMMAND ----------

if CREATE_CATALOG_IF_MISSING:
    try:
        execute_sql(
            step="create_catalog",
            statement=f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}",
            ignore_error=False,
        )
    except Exception:
        # Em alguns ambientes você pode não ter privilégio CREATE CATALOG.
        # Nesse caso, tentamos seguir usando um catálogo já existente.
        log_step(
            step="create_catalog",
            status="WARN",
            detail=(
                "Falha ao criar catálogo. Tentando continuar com catálogo existente. "
                "Verifique permissões se o catálogo ainda não existir."
            ),
            statement=f"USE CATALOG {CATALOG_NAME}",
        )

execute_sql(step="use_catalog", statement=f"USE CATALOG {CATALOG_NAME}")
execute_sql(step="create_schema", statement=f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")
execute_sql(step="use_schema", statement=f"USE SCHEMA {SCHEMA_NAME}")
escaped_volume_comment = VOLUME_COMMENT.replace("'", "''")

execute_sql(
    step="create_volume",
    statement=(
        f"CREATE VOLUME IF NOT EXISTS {FULL_VOLUME_NAME} "
        f"COMMENT '{escaped_volume_comment}'"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criação da raw zone do projeto

# COMMAND ----------

paths_to_create = [RAW_PROJECT_PATH, RAW_DOWNLOADS_PATH, RAW_ARCHIVE_PATH, RAW_LOGS_PATH]

for path in paths_to_create:
    dbutils.fs.mkdirs(path)
    log_step(step="mkdirs", status="OK", detail=f"Diretório garantido: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidências do ambiente preparado

# COMMAND ----------

# Metadados do volume
volume_description = spark.sql(f"DESCRIBE VOLUME {FULL_VOLUME_NAME}")
display(volume_description)

# COMMAND ----------

# Estrutura criada na raw zone
for path in paths_to_create:
    print(f"Conteúdo de: {path}")
    display(dbutils.fs.ls(path))

# COMMAND ----------

# Log de execução
execution_log_df = spark.createDataFrame(execution_log)
display(execution_log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Saídas esperadas deste notebook
# MAGIC
# MAGIC Ao final desta execução, você deve ter:
# MAGIC - Catálogo ativo
# MAGIC - Schema criado
# MAGIC - Volume criado em Unity Catalog
# MAGIC - Pasta raiz do projeto em `/Volumes/...`
# MAGIC - Subpastas `downloads`, `archive` e `logs`
# MAGIC
# MAGIC O próximo notebook natural é o `00_01_source_download_olist`, responsável por baixar a base da Olist no Kaggle e copiar os CSVs para `downloads`.

# Databricks notebook source
# MAGIC %md
# MAGIC # 00_01_source_download_olist
# MAGIC
# MAGIC Este notebook baixa a base pública da Olist no Kaggle e copia os arquivos CSV esperados para a raw zone oficial do projeto.
# MAGIC
# MAGIC ## O que ele faz
# MAGIC - Define parâmetros do download
# MAGIC - Opcionalmente configura autenticação com token do Kaggle via Databricks Secrets
# MAGIC - Baixa o dataset da Olist com `kagglehub`
# MAGIC - Valida os arquivos esperados
# MAGIC - Copia os CSVs para a pasta `downloads` da raw zone
# MAGIC - Gera evidências e um manifesto JSON da extração
# MAGIC
# MAGIC ## O que ele não faz
# MAGIC - Não cria tabelas Bronze
# MAGIC - Não trata schema analítico
# MAGIC - Não limpa colunas nem deriva métricas de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Dependências
# MAGIC
# MAGIC O Databricks recomenda `%pip` para bibliotecas escopadas ao notebook.
# MAGIC Se esta for a primeira execução da sessão, talvez seja necessário rodar o notebook novamente após a instalação.

# COMMAND ----------

# MAGIC %pip install -q kagglehub kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parâmetros do projeto e do download

# COMMAND ----------

import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from pyspark.sql import Row


def normalize_identifier(value: str) -> str:
    """Normaliza identificadores para snake_case simples."""
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
    "kaggle_dataset": "olistbr/brazilian-ecommerce",
    "force_download": "true",
    "clean_download_folder_before_copy": "true",
    "local_download_dir": "/tmp/olist_ecommerce/kaggle_download",
    "kaggle_api_token_scope": "",
    "kaggle_api_token_key": "",
}

EXPECTED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]


try:
    dbutils.widgets.text("catalog_name", DEFAULTS["catalog_name"])
    dbutils.widgets.text("schema_name", DEFAULTS["schema_name"])
    dbutils.widgets.text("volume_name", DEFAULTS["volume_name"])
    dbutils.widgets.text("project_slug", DEFAULTS["project_slug"])
    dbutils.widgets.text("kaggle_dataset", DEFAULTS["kaggle_dataset"])
    dbutils.widgets.dropdown("force_download", DEFAULTS["force_download"], ["true", "false"])
    dbutils.widgets.dropdown(
        "clean_download_folder_before_copy",
        DEFAULTS["clean_download_folder_before_copy"],
        ["true", "false"],
    )
    dbutils.widgets.text("local_download_dir", DEFAULTS["local_download_dir"])
    dbutils.widgets.text("kaggle_api_token_scope", DEFAULTS["kaggle_api_token_scope"])
    dbutils.widgets.text("kaggle_api_token_key", DEFAULTS["kaggle_api_token_key"])

    params = {
        "catalog_name": dbutils.widgets.get("catalog_name"),
        "schema_name": dbutils.widgets.get("schema_name"),
        "volume_name": dbutils.widgets.get("volume_name"),
        "project_slug": dbutils.widgets.get("project_slug"),
        "kaggle_dataset": dbutils.widgets.get("kaggle_dataset"),
        "force_download": dbutils.widgets.get("force_download"),
        "clean_download_folder_before_copy": dbutils.widgets.get("clean_download_folder_before_copy"),
        "local_download_dir": dbutils.widgets.get("local_download_dir"),
        "kaggle_api_token_scope": dbutils.widgets.get("kaggle_api_token_scope"),
        "kaggle_api_token_key": dbutils.widgets.get("kaggle_api_token_key"),
    }
except NameError:
    params = DEFAULTS.copy()

CATALOG_NAME = normalize_identifier(params["catalog_name"])
SCHEMA_NAME = normalize_identifier(params["schema_name"])
VOLUME_NAME = normalize_identifier(params["volume_name"])
PROJECT_SLUG = normalize_identifier(params["project_slug"])
KAGGLE_DATASET = str(params["kaggle_dataset"]).strip()
FORCE_DOWNLOAD = str(params["force_download"]).lower() == "true"
CLEAN_DOWNLOAD_FOLDER_BEFORE_COPY = str(params["clean_download_folder_before_copy"]).lower() == "true"
LOCAL_DOWNLOAD_DIR = str(params["local_download_dir"]).strip()
KAGGLE_API_TOKEN_SCOPE = str(params["kaggle_api_token_scope"]).strip()
KAGGLE_API_TOKEN_KEY = str(params["kaggle_api_token_key"]).strip()

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
    Row(parameter="kaggle_dataset", value=KAGGLE_DATASET),
    Row(parameter="force_download", value=str(FORCE_DOWNLOAD)),
    Row(parameter="clean_download_folder_before_copy", value=str(CLEAN_DOWNLOAD_FOLDER_BEFORE_COPY)),
    Row(parameter="local_download_dir", value=LOCAL_DOWNLOAD_DIR),
    Row(parameter="raw_downloads_path", value=RAW_DOWNLOADS_PATH),
    Row(parameter="raw_archive_path", value=RAW_ARCHIVE_PATH),
    Row(parameter="raw_logs_path", value=RAW_LOGS_PATH),
    Row(parameter="kaggle_api_token_scope", value=KAGGLE_API_TOKEN_SCOPE or "<empty>"),
    Row(parameter="kaggle_api_token_key", value=KAGGLE_API_TOKEN_KEY or "<empty>"),
]

display(spark.createDataFrame(config_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções auxiliares

# COMMAND ----------

execution_log: List[Row] = []


def log_step(step: str, status: str, detail: str) -> None:
    execution_log.append(
        Row(
            timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            step=step,
            status=status,
            detail=detail,
        )
    )


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
    log_step("mkdirs", "OK", f"Diretório garantido: {path}")


def find_files_recursively(root: str, allowed_names: List[str]) -> Dict[str, str]:
    allowed = set(allowed_names)
    found: Dict[str, str] = {}
    for file_path in Path(root).rglob("*"):
        if file_path.is_file() and file_path.name in allowed:
            found[file_path.name] = str(file_path)
    return found


def file_metadata(path: str) -> Row:
    p = Path(path)
    stat = p.stat()
    return Row(
        file_name=p.name,
        path=str(p),
        size_bytes=int(stat.st_size),
        modified_at=datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparação da raw zone e autenticação opcional

# COMMAND ----------

for path in [RAW_PROJECT_PATH, RAW_DOWNLOADS_PATH, RAW_ARCHIVE_PATH, RAW_LOGS_PATH, LOCAL_DOWNLOAD_DIR]:
    ensure_dir(path)

# Autenticação opcional via Databricks Secrets.
# Se scope e key forem informados, o notebook exporta o token para KAGGLE_API_TOKEN.
# Caso contrário, assume que a autenticação já está configurada no ambiente.
if KAGGLE_API_TOKEN_SCOPE and KAGGLE_API_TOKEN_KEY:
    kaggle_api_token = dbutils.secrets.get(KAGGLE_API_TOKEN_SCOPE, KAGGLE_API_TOKEN_KEY)
    os.environ["KAGGLE_API_TOKEN"] = kaggle_api_token
    log_step("auth", "OK", "KAGGLE_API_TOKEN configurado a partir de Databricks Secrets")
else:
    log_step(
        "auth",
        "WARN",
        (
            "Nenhum secret de token foi informado. O notebook vai tentar usar a autenticação já disponível "
            "no ambiente Kaggle/Kaggle API/KaggleHub."
        ),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download do dataset com kagglehub

# COMMAND ----------

import kagglehub

local_download_path = Path(LOCAL_DOWNLOAD_DIR)
if CLEAN_DOWNLOAD_FOLDER_BEFORE_COPY and local_download_path.exists():
    shutil.rmtree(local_download_path)
    log_step("clean_local_download_dir", "OK", f"Diretório limpo: {LOCAL_DOWNLOAD_DIR}")

ensure_dir(LOCAL_DOWNLOAD_DIR)

resolved_download_root = kagglehub.dataset_download(
    KAGGLE_DATASET,
    force_download=FORCE_DOWNLOAD,
    output_dir=LOCAL_DOWNLOAD_DIR,
)

resolved_download_root = str(Path(resolved_download_root).resolve())
log_step("dataset_download", "OK", f"Dataset baixado para: {resolved_download_root}")
print(f"Dataset resolvido em: {resolved_download_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação dos arquivos esperados

# COMMAND ----------

found_files = find_files_recursively(resolved_download_root, EXPECTED_FILES)
found_names = sorted(found_files.keys())
missing_files = sorted(set(EXPECTED_FILES) - set(found_names))
extra_files = sorted(set(found_names) - set(EXPECTED_FILES))

validation_rows = [
    Row(check="expected_files_count", result=str(len(EXPECTED_FILES))),
    Row(check="found_expected_files_count", result=str(len(found_names))),
    Row(check="missing_files", result=", ".join(missing_files) if missing_files else "<none>"),
    Row(check="extra_files", result=", ".join(extra_files) if extra_files else "<none>"),
]

display(spark.createDataFrame(validation_rows))

if missing_files:
    log_step("validate_expected_files", "ERROR", f"Arquivos ausentes: {', '.join(missing_files)}")
    raise FileNotFoundError(
        "Os seguintes arquivos esperados não foram encontrados no download: "
        + ", ".join(missing_files)
    )

log_step("validate_expected_files", "OK", "Todos os arquivos esperados foram encontrados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cópia controlada dos arquivos para a raw zone

# COMMAND ----------

if CLEAN_DOWNLOAD_FOLDER_BEFORE_COPY:
    for existing_item in Path(RAW_DOWNLOADS_PATH).glob("*.csv"):
        existing_item.unlink()
    log_step("clean_raw_downloads", "OK", f"Arquivos CSV antigos removidos de: {RAW_DOWNLOADS_PATH}")

copied_rows: List[Row] = []

for file_name in EXPECTED_FILES:
    src = Path(found_files[file_name])
    dst = Path(RAW_DOWNLOADS_PATH) / file_name
    shutil.copy2(src, dst)
    copied_rows.append(file_metadata(str(dst)))
    log_step("copy_to_raw_zone", "OK", f"Arquivo copiado: {src} -> {dst}")

copied_df = spark.createDataFrame(copied_rows)
display(copied_df.orderBy("file_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Manifesto JSON da extração

# COMMAND ----------

manifest = {
    "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "dataset_handle": KAGGLE_DATASET,
    "force_download": FORCE_DOWNLOAD,
    "local_download_dir": resolved_download_root,
    "raw_downloads_path": RAW_DOWNLOADS_PATH,
    "expected_files": EXPECTED_FILES,
    "copied_files": [row.asDict() for row in copied_rows],
}

manifest_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
manifest_path = Path(RAW_LOGS_PATH) / f"kaggle_download_manifest_{manifest_ts}.json"
with open(manifest_path, "w", encoding="utf-8") as fp:
    json.dump(manifest, fp, ensure_ascii=False, indent=2)

print(f"Manifesto salvo em: {manifest_path}")
log_step("write_manifest", "OK", f"Manifesto salvo em: {manifest_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Evidências finais

# COMMAND ----------

print("Conteúdo final da pasta downloads:")
display(dbutils.fs.ls(RAW_DOWNLOADS_PATH))

# COMMAND ----------

print("Conteúdo final da pasta logs:")
display(dbutils.fs.ls(RAW_LOGS_PATH))

# COMMAND ----------

execution_log_df = spark.createDataFrame(execution_log)
display(execution_log_df)

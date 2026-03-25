# Databricks notebook source
# MAGIC %md
# MAGIC # 02_07_silver_order_reviews
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler a tabela `bronze_order_reviews`
# MAGIC - Padronizar campos textuais essenciais
# MAGIC - Converter colunas numéricas e de data/hora
# MAGIC - Criar variáveis iniciais no nível de review do pedido
# MAGIC - Persistir a tabela `silver_order_reviews` em Delta

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "bronze_order_reviews")
dbutils.widgets.text("target_table_name", "silver_order_reviews")
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


def sql_try_to_timestamp(column_name: str):
    return F.expr(f"try_to_timestamp({column_name}, 'yyyy-MM-dd HH:mm:ss')")


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
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Bronze de order_reviews")

# COMMAND ----------

# DBTITLE 1,Leitura da Bronze
reviews_bronze_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("bronze_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Montagem da Silver
reviews_silver_base_df = (
    reviews_bronze_df
    .withColumn("review_id", null_if_blank("review_id"))
    .withColumn("order_id", null_if_blank("order_id"))
    .withColumn("review_score_int", sql_try_cast("review_score", "int"))
    .withColumn("review_comment_title", null_if_blank("review_comment_title"))
    .withColumn("review_comment_message", null_if_blank("review_comment_message"))
    .withColumn("review_creation_date_ts", sql_try_to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp_ts", sql_try_to_timestamp("review_answer_timestamp"))
)

reviews_silver_df = (
    reviews_silver_base_df
    .withColumn("review_creation_date", F.to_date("review_creation_date_ts"))
    .withColumn("review_answer_date", F.to_date("review_answer_timestamp_ts"))
    .withColumn(
        "review_response_hours",
        F.when(
            F.col("review_creation_date_ts").isNotNull() & F.col("review_answer_timestamp_ts").isNotNull(),
            F.round(
                (F.col("review_answer_timestamp_ts").cast("long") - F.col("review_creation_date_ts").cast("long")) / F.lit(3600.0),
                2
            )
        )
    )
    .withColumn(
        "is_positive_review",
        F.when(F.col("review_score_int").isNull(), F.lit(None)).otherwise(F.col("review_score_int") >= F.lit(4))
    )
    .withColumn(
        "is_neutral_review",
        F.when(F.col("review_score_int").isNull(), F.lit(None)).otherwise(F.col("review_score_int") == F.lit(3))
    )
    .withColumn(
        "is_negative_review",
        F.when(F.col("review_score_int").isNull(), F.lit(None)).otherwise(F.col("review_score_int") <= F.lit(2))
    )
    .withColumn(
        "has_comment_title",
        F.col("review_comment_title").isNotNull()
    )
    .withColumn(
        "has_comment_message",
        F.col("review_comment_message").isNotNull()
    )
    .withColumn(
        "has_invalid_review_score",
        F.when(F.col("review_score_int").isNull(), F.lit(True)).otherwise(~F.col("review_score_int").between(1, 5))
    )
    .withColumn(
        "has_creation_answer_inconsistency",
        F.when(
            F.col("review_creation_date_ts").isNotNull() & F.col("review_answer_timestamp_ts").isNotNull(),
            F.col("review_answer_timestamp_ts") < F.col("review_creation_date_ts")
        ).otherwise(F.lit(False))
    )
    .withColumn("_silver_processed_timestamp", F.current_timestamp())
    .select(
        "review_id",
        "order_id",
        "review_score_int",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date_ts",
        "review_creation_date",
        "review_answer_timestamp_ts",
        "review_answer_date",
        "review_response_hours",
        "is_positive_review",
        "is_neutral_review",
        "is_negative_review",
        "has_comment_title",
        "has_comment_message",
        "has_invalid_review_score",
        "has_creation_answer_inconsistency",
        "_ingestion_timestamp",
        "_silver_processed_timestamp"
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de order_reviews montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = reviews_silver_df.count()
null_review_id_count = reviews_silver_df.filter(F.col("review_id").isNull()).count()
null_order_id_count = reviews_silver_df.filter(F.col("order_id").isNull()).count()
null_review_score_count = reviews_silver_df.filter(F.col("review_score_int").isNull()).count()
positive_review_count = reviews_silver_df.filter(F.col("is_positive_review") == True).count()
neutral_review_count = reviews_silver_df.filter(F.col("is_neutral_review") == True).count()
negative_review_count = reviews_silver_df.filter(F.col("is_negative_review") == True).count()
invalid_review_score_count = reviews_silver_df.filter(F.col("has_invalid_review_score") == True).count()
creation_answer_inconsistency_count = reviews_silver_df.filter(F.col("has_creation_answer_inconsistency") == True).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Null review_id count: {null_review_id_count}")
log_step("silver_metrics", "OK", f"Null order_id count: {null_order_id_count}")
log_step("silver_metrics", "OK", f"Null review_score_int count: {null_review_score_count}")
log_step("silver_metrics", "OK", f"Positive review count: {positive_review_count}")
log_step("silver_metrics", "OK", f"Neutral review count: {neutral_review_count}")
log_step("silver_metrics", "OK", f"Negative review count: {negative_review_count}")
log_step("silver_metrics", "OK", f"Invalid review score count: {invalid_review_score_count}")
log_step("silver_metrics", "OK", f"Creation -> answer inconsistencies: {creation_answer_inconsistency_count}")

print("Schema da Silver:")
reviews_silver_df.printSchema()

display(reviews_silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    reviews_silver_df.write
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

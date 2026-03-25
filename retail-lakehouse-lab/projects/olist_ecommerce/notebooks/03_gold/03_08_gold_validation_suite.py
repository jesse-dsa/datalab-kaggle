# Databricks notebook source
# MAGIC %md
# MAGIC # 03_08_gold_validation_suite
# MAGIC
# MAGIC Objetivo:
# MAGIC - Validar a camada Gold do projeto Olist
# MAGIC - Confirmar existência das tabelas esperadas
# MAGIC - Verificar contagens básicas, unicidade e nulos críticos
# MAGIC - Avaliar cobertura entre fato, dimensões e tabelas KPI
# MAGIC - Produzir evidências rápidas de qualidade da Gold

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("fact_orders_table_name", "gold_fact_orders")
dbutils.widgets.text("dim_customers_table_name", "gold_dim_customers")
dbutils.widgets.text("dim_products_table_name", "gold_dim_products")
dbutils.widgets.text("dim_sellers_table_name", "gold_dim_sellers")
dbutils.widgets.text("kpi_delivery_table_name", "gold_kpi_delivery_performance")
dbutils.widgets.text("kpi_review_table_name", "gold_kpi_review_logistics")
dbutils.widgets.text("kpi_payment_table_name", "gold_kpi_payment_consistency")

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
FACT_ORDERS_TABLE_NAME = dbutils.widgets.get("fact_orders_table_name").strip()
DIM_CUSTOMERS_TABLE_NAME = dbutils.widgets.get("dim_customers_table_name").strip()
DIM_PRODUCTS_TABLE_NAME = dbutils.widgets.get("dim_products_table_name").strip()
DIM_SELLERS_TABLE_NAME = dbutils.widgets.get("dim_sellers_table_name").strip()
KPI_DELIVERY_TABLE_NAME = dbutils.widgets.get("kpi_delivery_table_name").strip()
KPI_REVIEW_TABLE_NAME = dbutils.widgets.get("kpi_review_table_name").strip()
KPI_PAYMENT_TABLE_NAME = dbutils.widgets.get("kpi_payment_table_name").strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_FACT_ORDERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{FACT_ORDERS_TABLE_NAME}"
FULL_DIM_CUSTOMERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{DIM_CUSTOMERS_TABLE_NAME}"
FULL_DIM_PRODUCTS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{DIM_PRODUCTS_TABLE_NAME}"
FULL_DIM_SELLERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{DIM_SELLERS_TABLE_NAME}"
FULL_KPI_DELIVERY_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{KPI_DELIVERY_TABLE_NAME}"
FULL_KPI_REVIEW_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{KPI_REVIEW_TABLE_NAME}"
FULL_KPI_PAYMENT_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{KPI_PAYMENT_TABLE_NAME}"

print("Parâmetros carregados com sucesso.")
print(f"FULL_SCHEMA_NAME = {FULL_SCHEMA_NAME}")
print(f"FULL_FACT_ORDERS_TABLE_NAME = {FULL_FACT_ORDERS_TABLE_NAME}")
print(f"FULL_DIM_CUSTOMERS_TABLE_NAME = {FULL_DIM_CUSTOMERS_TABLE_NAME}")
print(f"FULL_DIM_PRODUCTS_TABLE_NAME = {FULL_DIM_PRODUCTS_TABLE_NAME}")
print(f"FULL_DIM_SELLERS_TABLE_NAME = {FULL_DIM_SELLERS_TABLE_NAME}")
print(f"FULL_KPI_DELIVERY_TABLE_NAME = {FULL_KPI_DELIVERY_TABLE_NAME}")
print(f"FULL_KPI_REVIEW_TABLE_NAME = {FULL_KPI_REVIEW_TABLE_NAME}")
print(f"FULL_KPI_PAYMENT_TABLE_NAME = {FULL_KPI_PAYMENT_TABLE_NAME}")

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


def make_check(check_name: str, metric_name: str, metric_value, status: str, details: str):
    return (check_name, metric_name, str(metric_value), status, details)


# COMMAND ----------

# DBTITLE 1,Uso do catálogo e schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

log_step("context_setup", "OK", f"Contexto definido para {FULL_SCHEMA_NAME}")

# COMMAND ----------

# DBTITLE 1,Validação de existência das tabelas Gold
ensure_table_exists(FULL_FACT_ORDERS_TABLE_NAME, "Tabela Gold fact_orders")
ensure_table_exists(FULL_DIM_CUSTOMERS_TABLE_NAME, "Tabela Gold dim_customers")
ensure_table_exists(FULL_DIM_PRODUCTS_TABLE_NAME, "Tabela Gold dim_products")
ensure_table_exists(FULL_DIM_SELLERS_TABLE_NAME, "Tabela Gold dim_sellers")
ensure_table_exists(FULL_KPI_DELIVERY_TABLE_NAME, "Tabela Gold kpi_delivery_performance")
ensure_table_exists(FULL_KPI_REVIEW_TABLE_NAME, "Tabela Gold kpi_review_logistics")
ensure_table_exists(FULL_KPI_PAYMENT_TABLE_NAME, "Tabela Gold kpi_payment_consistency")

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas Gold
fact_orders_df = spark.table(FULL_FACT_ORDERS_TABLE_NAME)
dim_customers_df = spark.table(FULL_DIM_CUSTOMERS_TABLE_NAME)
dim_products_df = spark.table(FULL_DIM_PRODUCTS_TABLE_NAME)
dim_sellers_df = spark.table(FULL_DIM_SELLERS_TABLE_NAME)
kpi_delivery_df = spark.table(FULL_KPI_DELIVERY_TABLE_NAME)
kpi_review_df = spark.table(FULL_KPI_REVIEW_TABLE_NAME)
kpi_payment_df = spark.table(FULL_KPI_PAYMENT_TABLE_NAME)

log_step("gold_read", "OK", "Todas as tabelas Gold foram lidas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Contagens principais
fact_row_count = fact_orders_df.count()
fact_distinct_orders = fact_orders_df.select("order_id").distinct().count()
fact_distinct_order_item_keys = fact_orders_df.select("order_item_key").distinct().count()

dim_customers_count = dim_customers_df.count()
dim_products_count = dim_products_df.count()
dim_sellers_count = dim_sellers_df.count()

kpi_delivery_count = kpi_delivery_df.count()
kpi_review_count = kpi_review_df.count()
kpi_payment_count = kpi_payment_df.count()

summary_counts_df = spark.createDataFrame(
    [
        ("gold_fact_orders", fact_row_count, fact_distinct_orders, fact_distinct_order_item_keys),
        ("gold_dim_customers", dim_customers_count, None, None),
        ("gold_dim_products", dim_products_count, None, None),
        ("gold_dim_sellers", dim_sellers_count, None, None),
        ("gold_kpi_delivery_performance", kpi_delivery_count, None, None),
        ("gold_kpi_review_logistics", kpi_review_count, None, None),
        ("gold_kpi_payment_consistency", kpi_payment_count, None, None),
    ],
    ["table_name", "row_count", "distinct_orders", "distinct_order_item_keys"]
)

display(summary_counts_df)

# COMMAND ----------

# DBTITLE 1,Validação do fato principal
fact_order_item_key_duplicates = (
    fact_orders_df.groupBy("order_item_key").count().filter(F.col("count") > 1).count()
)
fact_null_order_id = fact_orders_df.filter(F.col("order_id").isNull()).count()
fact_null_order_item_key = fact_orders_df.filter(F.col("order_item_key").isNull()).count()
fact_null_customer_id = fact_orders_df.filter(F.col("customer_id").isNull()).count()
fact_null_product_id = fact_orders_df.filter(F.col("product_id").isNull()).count()
fact_null_seller_id = fact_orders_df.filter(F.col("seller_id").isNull()).count()

# COMMAND ----------

# DBTITLE 1,Validação das dimensões
dim_customers_duplicate_keys = (
    dim_customers_df.groupBy("customer_key").count().filter(F.col("count") > 1).count()
)
dim_products_duplicate_keys = (
    dim_products_df.groupBy("product_key").count().filter(F.col("count") > 1).count()
)
dim_sellers_duplicate_keys = (
    dim_sellers_df.groupBy("seller_key").count().filter(F.col("count") > 1).count()
)

dim_customers_null_key = dim_customers_df.filter(F.col("customer_key").isNull()).count()
dim_products_null_key = dim_products_df.filter(F.col("product_key").isNull()).count()
dim_sellers_null_key = dim_sellers_df.filter(F.col("seller_key").isNull()).count()

# COMMAND ----------

# DBTITLE 1,Cobertura entre fato e dimensões
fact_customers_missing_in_dim = (
    fact_orders_df
    .select("customer_id").distinct()
    .join(dim_customers_df.select(F.col("customer_key").alias("customer_id_dim")).distinct(),
          fact_orders_df.customer_id == F.col("customer_id_dim"),
          "left_anti")
    .count()
)

fact_products_missing_in_dim = (
    fact_orders_df
    .select("product_id").distinct()
    .join(dim_products_df.select(F.col("product_key").alias("product_id_dim")).distinct(),
          fact_orders_df.product_id == F.col("product_id_dim"),
          "left_anti")
    .count()
)

fact_sellers_missing_in_dim = (
    fact_orders_df
    .select("seller_id").distinct()
    .join(dim_sellers_df.select(F.col("seller_key").alias("seller_id_dim")).distinct(),
          fact_orders_df.seller_id == F.col("seller_id_dim"),
          "left_anti")
    .count()
)

# COMMAND ----------

# DBTITLE 1,Validação das tabelas KPI
kpi_delivery_null_month = kpi_delivery_df.filter(F.col("order_purchase_month").isNull()).count()
kpi_review_null_month = kpi_review_df.filter(F.col("order_purchase_month").isNull()).count()
kpi_payment_null_month = kpi_payment_df.filter(F.col("order_purchase_month").isNull()).count()

kpi_delivery_negative_total_orders = kpi_delivery_df.filter(F.col("total_orders") < 0).count()
kpi_review_negative_total_orders = kpi_review_df.filter(F.col("total_orders") < 0).count()
kpi_payment_negative_total_orders = kpi_payment_df.filter(F.col("total_orders") < 0).count()

# COMMAND ----------

# DBTITLE 1,Painel consolidado de checagens
checks = [
    make_check("fact_orders", "row_count", fact_row_count, "ok" if fact_row_count > 0 else "error", "A tabela fato precisa ter linhas."),
    make_check("fact_orders", "distinct_order_item_key_duplicates", fact_order_item_key_duplicates, "ok" if fact_order_item_key_duplicates == 0 else "warning", "A chave do item do pedido deve ser única."),
    make_check("fact_orders", "null_order_id", fact_null_order_id, "ok" if fact_null_order_id == 0 else "warning", "order_id não deveria ser nulo no fato."),
    make_check("fact_orders", "null_order_item_key", fact_null_order_item_key, "ok" if fact_null_order_item_key == 0 else "warning", "order_item_key não deveria ser nulo."),
    make_check("fact_orders", "null_customer_id", fact_null_customer_id, "ok" if fact_null_customer_id == 0 else "warning", "customer_id idealmente não deve ser nulo."),
    make_check("fact_orders", "null_product_id", fact_null_product_id, "ok" if fact_null_product_id == 0 else "warning", "product_id idealmente não deve ser nulo."),
    make_check("fact_orders", "null_seller_id", fact_null_seller_id, "ok" if fact_null_seller_id == 0 else "warning", "seller_id idealmente não deve ser nulo."),
    make_check("dim_customers", "duplicate_customer_key", dim_customers_duplicate_keys, "ok" if dim_customers_duplicate_keys == 0 else "warning", "A chave da dimensão de clientes deve ser única."),
    make_check("dim_products", "duplicate_product_key", dim_products_duplicate_keys, "ok" if dim_products_duplicate_keys == 0 else "warning", "A chave da dimensão de produtos deve ser única."),
    make_check("dim_sellers", "duplicate_seller_key", dim_sellers_duplicate_keys, "ok" if dim_sellers_duplicate_keys == 0 else "warning", "A chave da dimensão de sellers deve ser única."),
    make_check("dim_customers", "null_customer_key", dim_customers_null_key, "ok" if dim_customers_null_key == 0 else "warning", "customer_key não deveria ser nulo."),
    make_check("dim_products", "null_product_key", dim_products_null_key, "ok" if dim_products_null_key == 0 else "warning", "product_key não deveria ser nulo."),
    make_check("dim_sellers", "null_seller_key", dim_sellers_null_key, "ok" if dim_sellers_null_key == 0 else "warning", "seller_key não deveria ser nulo."),
    make_check("coverage", "fact_customers_missing_in_dim", fact_customers_missing_in_dim, "ok" if fact_customers_missing_in_dim == 0 else "warning", "Clientes do fato deveriam existir na dimensão."),
    make_check("coverage", "fact_products_missing_in_dim", fact_products_missing_in_dim, "ok" if fact_products_missing_in_dim == 0 else "warning", "Produtos do fato deveriam existir na dimensão."),
    make_check("coverage", "fact_sellers_missing_in_dim", fact_sellers_missing_in_dim, "ok" if fact_sellers_missing_in_dim == 0 else "warning", "Sellers do fato deveriam existir na dimensão."),
    make_check("kpi_delivery", "null_order_purchase_month", kpi_delivery_null_month, "ok" if kpi_delivery_null_month == 0 else "warning", "A KPI de entrega deve ter mês preenchido."),
    make_check("kpi_review", "null_order_purchase_month", kpi_review_null_month, "ok" if kpi_review_null_month == 0 else "warning", "A KPI de review/logística deve ter mês preenchido."),
    make_check("kpi_payment", "null_order_purchase_month", kpi_payment_null_month, "ok" if kpi_payment_null_month == 0 else "warning", "A KPI de pagamento deve ter mês preenchido."),
    make_check("kpi_delivery", "negative_total_orders", kpi_delivery_negative_total_orders, "ok" if kpi_delivery_negative_total_orders == 0 else "error", "total_orders não pode ser negativo."),
    make_check("kpi_review", "negative_total_orders", kpi_review_negative_total_orders, "ok" if kpi_review_negative_total_orders == 0 else "error", "total_orders não pode ser negativo."),
    make_check("kpi_payment", "negative_total_orders", kpi_payment_negative_total_orders, "ok" if kpi_payment_negative_total_orders == 0 else "error", "total_orders não pode ser negativo."),
]

validation_results_df = spark.createDataFrame(
    checks,
    ["check_name", "metric_name", "metric_value", "status", "details"]
)

display(validation_results_df)

# COMMAND ----------

# DBTITLE 1,Resumo dos status
validation_summary_df = (
    validation_results_df
    .groupBy("status")
    .agg(F.count("*").alias("checks"))
    .orderBy("status")
)

display(validation_summary_df)

# COMMAND ----------

log_step("gold_validation", "OK", "Validação da Gold concluída com sucesso.")

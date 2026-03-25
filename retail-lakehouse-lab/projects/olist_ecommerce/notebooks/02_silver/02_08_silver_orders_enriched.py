# Databricks notebook source
# MAGIC %md
# MAGIC # 02_08_silver_orders_enriched
# MAGIC
# MAGIC Objetivo:
# MAGIC - Ler as tabelas Silver necessárias para enriquecer a jornada do pedido
# MAGIC - Consolidar pagamentos e reviews no nível do pedido
# MAGIC - Integrar pedidos, itens, clientes, produtos e sellers
# MAGIC - Persistir a tabela `silver_orders_enriched` em Delta
# MAGIC
# MAGIC Observação:
# MAGIC - A granularidade final desta tabela é de item do pedido
# MAGIC - Um mesmo `order_id` pode aparecer em múltiplas linhas

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("orders_table_name", "silver_orders")
dbutils.widgets.text("order_items_table_name", "silver_order_items")
dbutils.widgets.text("payments_table_name", "silver_order_payments")
dbutils.widgets.text("customers_table_name", "silver_customers")
dbutils.widgets.text("products_table_name", "silver_products")
dbutils.widgets.text("sellers_table_name", "silver_sellers")
dbutils.widgets.text("reviews_table_name", "silver_order_reviews")
dbutils.widgets.text("target_table_name", "silver_orders_enriched")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
ORDERS_TABLE_NAME = dbutils.widgets.get("orders_table_name").strip()
ORDER_ITEMS_TABLE_NAME = dbutils.widgets.get("order_items_table_name").strip()
PAYMENTS_TABLE_NAME = dbutils.widgets.get("payments_table_name").strip()
CUSTOMERS_TABLE_NAME = dbutils.widgets.get("customers_table_name").strip()
PRODUCTS_TABLE_NAME = dbutils.widgets.get("products_table_name").strip()
SELLERS_TABLE_NAME = dbutils.widgets.get("sellers_table_name").strip()
REVIEWS_TABLE_NAME = dbutils.widgets.get("reviews_table_name").strip()
TARGET_TABLE_NAME = dbutils.widgets.get("target_table_name").strip()
WRITE_MODE = dbutils.widgets.get("write_mode").strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_ORDERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{ORDERS_TABLE_NAME}"
FULL_ORDER_ITEMS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{ORDER_ITEMS_TABLE_NAME}"
FULL_PAYMENTS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{PAYMENTS_TABLE_NAME}"
FULL_CUSTOMERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{CUSTOMERS_TABLE_NAME}"
FULL_PRODUCTS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{PRODUCTS_TABLE_NAME}"
FULL_SELLERS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{SELLERS_TABLE_NAME}"
FULL_REVIEWS_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{REVIEWS_TABLE_NAME}"
FULL_TARGET_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{TARGET_TABLE_NAME}"

print("Parâmetros carregados com sucesso.")
print(f"FULL_SCHEMA_NAME = {FULL_SCHEMA_NAME}")
print(f"FULL_ORDERS_TABLE_NAME = {FULL_ORDERS_TABLE_NAME}")
print(f"FULL_ORDER_ITEMS_TABLE_NAME = {FULL_ORDER_ITEMS_TABLE_NAME}")
print(f"FULL_PAYMENTS_TABLE_NAME = {FULL_PAYMENTS_TABLE_NAME}")
print(f"FULL_CUSTOMERS_TABLE_NAME = {FULL_CUSTOMERS_TABLE_NAME}")
print(f"FULL_PRODUCTS_TABLE_NAME = {FULL_PRODUCTS_TABLE_NAME}")
print(f"FULL_SELLERS_TABLE_NAME = {FULL_SELLERS_TABLE_NAME}")
print(f"FULL_REVIEWS_TABLE_NAME = {FULL_REVIEWS_TABLE_NAME}")
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

# DBTITLE 1,Validação das tabelas Silver de origem
ensure_table_exists(FULL_ORDERS_TABLE_NAME, "Tabela Silver de orders")
ensure_table_exists(FULL_ORDER_ITEMS_TABLE_NAME, "Tabela Silver de order_items")
ensure_table_exists(FULL_PAYMENTS_TABLE_NAME, "Tabela Silver de order_payments")
ensure_table_exists(FULL_CUSTOMERS_TABLE_NAME, "Tabela Silver de customers")
ensure_table_exists(FULL_PRODUCTS_TABLE_NAME, "Tabela Silver de products")
ensure_table_exists(FULL_SELLERS_TABLE_NAME, "Tabela Silver de sellers")
ensure_table_exists(FULL_REVIEWS_TABLE_NAME, "Tabela Silver de order_reviews")

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas Silver
orders_df = spark.table(FULL_ORDERS_TABLE_NAME)
order_items_df = spark.table(FULL_ORDER_ITEMS_TABLE_NAME)
payments_df = spark.table(FULL_PAYMENTS_TABLE_NAME)
customers_df = spark.table(FULL_CUSTOMERS_TABLE_NAME)
products_df = spark.table(FULL_PRODUCTS_TABLE_NAME)
sellers_df = spark.table(FULL_SELLERS_TABLE_NAME)
reviews_df = spark.table(FULL_REVIEWS_TABLE_NAME)

log_step("silver_read", "OK", "Todas as tabelas Silver foram lidas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Agregação de pagamentos por pedido
payments_agg_df = (
    payments_df
    .groupBy("order_id")
    .agg(
        F.count(F.lit(1)).alias("payment_records_count"),
        F.sum("payment_value_amount").alias("order_total_payment_value_amount"),
        F.max("payment_installments_int").alias("max_payment_installments"),
        F.max(F.when(F.col("is_credit_card") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_credit_card_payment_int"),
        F.max(F.when(F.col("is_boleto") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_boleto_payment_int"),
        F.max(F.when(F.col("is_voucher") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_voucher_payment_int"),
        F.max(F.when(F.col("has_non_positive_payment_value") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_invalid_payment_value_int")
    )
    .withColumn("has_credit_card_payment", F.col("has_credit_card_payment_int") == F.lit(1))
    .withColumn("has_boleto_payment", F.col("has_boleto_payment_int") == F.lit(1))
    .withColumn("has_voucher_payment", F.col("has_voucher_payment_int") == F.lit(1))
    .withColumn("has_invalid_payment_value", F.col("has_invalid_payment_value_int") == F.lit(1))
    .drop(
        "has_credit_card_payment_int",
        "has_boleto_payment_int",
        "has_voucher_payment_int",
        "has_invalid_payment_value_int"
    )
)

log_step("payments_aggregation", "OK", "Pagamentos agregados por pedido com sucesso.")

# COMMAND ----------

# DBTITLE 1,Agregação de reviews por pedido
reviews_agg_df = (
    reviews_df
    .groupBy("order_id")
    .agg(
        F.count(F.lit(1)).alias("review_records_count"),
        F.avg("review_score_int").alias("avg_review_score"),
        F.max("review_score_int").alias("max_review_score"),
        F.min("review_score_int").alias("min_review_score"),
        F.max(F.when(F.col("is_positive_review") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_positive_review_int"),
        F.max(F.when(F.col("is_negative_review") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_negative_review_int"),
        F.max(F.when(F.col("has_comment_message") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_review_comment_int"),
        F.avg("review_response_hours").alias("avg_review_response_hours"),
        F.max(F.when(F.col("has_invalid_review_score") == True, F.lit(1)).otherwise(F.lit(0))).alias("has_invalid_review_score_int")
    )
    .withColumn("avg_review_score", F.round(F.col("avg_review_score"), 2))
    .withColumn("avg_review_response_hours", F.round(F.col("avg_review_response_hours"), 2))
    .withColumn("has_positive_review", F.col("has_positive_review_int") == F.lit(1))
    .withColumn("has_negative_review", F.col("has_negative_review_int") == F.lit(1))
    .withColumn("has_review_comment", F.col("has_review_comment_int") == F.lit(1))
    .withColumn("has_invalid_review_score", F.col("has_invalid_review_score_int") == F.lit(1))
    .drop(
        "has_positive_review_int",
        "has_negative_review_int",
        "has_review_comment_int",
        "has_invalid_review_score_int"
    )
)

log_step("reviews_aggregation", "OK", "Reviews agregados por pedido com sucesso.")

# COMMAND ----------

# DBTITLE 1,Integração das tabelas Silver
orders_alias = orders_df.alias("o")
order_items_alias = order_items_df.alias("oi")
payments_alias = payments_agg_df.alias("p")
customers_alias = customers_df.alias("c")
products_alias = products_df.alias("pr")
sellers_alias = sellers_df.alias("s")
reviews_alias = reviews_agg_df.alias("r")

orders_enriched_base_df = (
    orders_alias
    .join(order_items_alias, on="order_id", how="left")
    .join(customers_alias, on="customer_id", how="left")
    .join(products_alias, on="product_id", how="left")
    .join(sellers_alias, on="seller_id", how="left")
    .join(payments_alias, on="order_id", how="left")
    .join(reviews_alias, on="order_id", how="left")
)

log_step("silver_join", "OK", "Integração das tabelas Silver concluída com sucesso.")

# COMMAND ----------

# DBTITLE 1,Métricas derivadas na granularidade de item do pedido
item_window = Window.partitionBy("order_id")

orders_enriched_df = (
    orders_enriched_base_df
    .withColumn("order_items_count", F.count("order_item_id_int").over(item_window))
    .withColumn("order_total_items_value_amount", F.round(F.sum("item_total_value_amount").over(item_window), 2))
    .withColumn(
        "item_value_share_in_order",
        F.when(
            F.col("item_total_value_amount").isNotNull() & (F.col("order_total_items_value_amount") > F.lit(0)),
            F.round(F.col("item_total_value_amount") / F.col("order_total_items_value_amount"), 4)
        )
    )
    .withColumn(
        "payment_to_items_value_gap_amount",
        F.when(
            F.col("order_total_payment_value_amount").isNotNull() & F.col("order_total_items_value_amount").isNotNull(),
            F.round(F.col("order_total_payment_value_amount") - F.col("order_total_items_value_amount"), 2)
        )
    )
    .withColumn("has_order_item", F.col("order_item_id_int").isNotNull())
    .withColumn("has_payment_data", F.col("payment_records_count").isNotNull())
    .withColumn("has_review_data", F.col("review_records_count").isNotNull())
    .withColumn("has_customer_data", F.col("customer_unique_id").isNotNull())
    .withColumn("has_product_data", F.col("product_id").isNotNull() & F.col("product_category_name").isNotNull())
    .withColumn("has_seller_data", F.col("seller_id").isNotNull() & F.col("seller_state").isNotNull())
    .withColumn(
        "is_order_payment_value_consistent",
        F.when(
            F.col("payment_to_items_value_gap_amount").isNull(),
            F.lit(None)
        ).otherwise(F.abs(F.col("payment_to_items_value_gap_amount")) <= F.lit(0.01))
    )
    .select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix_int",
        "customer_city",
        "customer_state",
        F.col("c.has_invalid_zip_code_prefix").alias("customer_has_invalid_zip_code_prefix"),
        F.col("c.has_missing_customer_city").alias("customer_has_missing_city"),
        F.col("c.has_missing_customer_state").alias("customer_has_missing_state"),
        F.col("c.is_valid_brazil_state_code").alias("customer_is_valid_brazil_state_code"),
        "order_status",
        "order_purchase_timestamp_ts",
        "order_purchase_date",
        "order_approved_at_ts",
        "order_approved_date",
        "order_delivered_carrier_date_ts",
        "order_delivered_carrier_date",
        "order_delivered_customer_date_ts",
        "order_delivered_customer_date",
        "order_estimated_delivery_date_ts",
        "order_estimated_delivery_date",
        "purchase_to_approval_hours",
        "approval_to_carrier_hours",
        "carrier_to_customer_hours",
        "purchase_to_delivery_hours",
        "delivery_delay_days",
        "is_delivered",
        "is_canceled",
        "is_delayed",
        "has_purchase_approval_inconsistency",
        "has_approval_carrier_inconsistency",
        "has_carrier_customer_inconsistency",
        "order_item_id_int",
        "product_id",
        "seller_id",
        "shipping_limit_date_ts",
        "shipping_limit_date",
        "price_amount",
        "freight_value_amount",
        "item_total_value_amount",
        "is_free_shipping",
        "has_missing_product_id",
        "has_missing_seller_id",
        "has_invalid_order_item_id",
        "has_non_positive_price",
        "has_negative_freight_value",
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
        "seller_zip_code_prefix_int",
        "seller_city",
        "seller_state",
        F.col("s.has_invalid_zip_code_prefix").alias("seller_has_invalid_zip_code_prefix"),
        F.col("s.has_missing_seller_city").alias("seller_has_missing_city"),
        F.col("s.has_missing_seller_state").alias("seller_has_missing_state"),
        F.col("s.is_valid_brazil_state_code").alias("seller_is_valid_brazil_state_code"),
        "payment_records_count",
        "order_total_payment_value_amount",
        "max_payment_installments",
        "has_credit_card_payment",
        "has_boleto_payment",
        "has_voucher_payment",
        "has_invalid_payment_value",
        "review_records_count",
        "avg_review_score",
        "max_review_score",
        "min_review_score",
        "has_positive_review",
        "has_negative_review",
        "has_review_comment",
        "avg_review_response_hours",
        "has_invalid_review_score",
        "order_items_count",
        "order_total_items_value_amount",
        "item_value_share_in_order",
        "payment_to_items_value_gap_amount",
        "is_order_payment_value_consistent",
        "has_order_item",
        "has_payment_data",
        "has_review_data",
        "has_customer_data",
        "has_product_data",
        "has_seller_data",
        F.col("o._ingestion_timestamp").alias("_ingestion_timestamp"),
        F.current_timestamp().alias("_silver_processed_timestamp")
    )
)

log_step("silver_dataframe", "OK", "DataFrame silver de orders_enriched montado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Evidências pré-gravação
silver_row_count = orders_enriched_df.count()
distinct_order_count = orders_enriched_df.select("order_id").distinct().count()
rows_without_order_item_count = orders_enriched_df.filter(F.col("has_order_item") == False).count()
rows_without_payment_data_count = orders_enriched_df.filter(F.col("has_payment_data") == False).count()
rows_without_review_data_count = orders_enriched_df.filter(F.col("has_review_data") == False).count()
delayed_rows_count = orders_enriched_df.filter(F.col("is_delayed") == True).count()
negative_review_rows_count = orders_enriched_df.filter(F.col("has_negative_review") == True).count()
inconsistent_payment_rows_count = orders_enriched_df.filter(F.col("is_order_payment_value_consistent") == False).count()

log_step("silver_metrics", "OK", f"Row count: {silver_row_count}")
log_step("silver_metrics", "OK", f"Distinct order count: {distinct_order_count}")
log_step("silver_metrics", "OK", f"Rows without order item: {rows_without_order_item_count}")
log_step("silver_metrics", "OK", f"Rows without payment data: {rows_without_payment_data_count}")
log_step("silver_metrics", "OK", f"Rows without review data: {rows_without_review_data_count}")
log_step("silver_metrics", "OK", f"Delayed rows count: {delayed_rows_count}")
log_step("silver_metrics", "OK", f"Rows with negative review: {negative_review_rows_count}")
log_step("silver_metrics", "OK", f"Rows with payment inconsistency: {inconsistent_payment_rows_count}")

print("Schema da Silver:")
orders_enriched_df.printSchema()

display(orders_enriched_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gravação da tabela Silver em Delta
(
    orders_enriched_df.write
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
persisted_distinct_order_count = persisted_df.select("order_id").distinct().count()

log_step("post_write_validation", "OK", f"Persisted row count: {persisted_row_count}")
log_step("post_write_validation", "OK", f"Persisted distinct order count: {persisted_distinct_order_count}")

print(f"Tabela criada/atualizada: {FULL_TARGET_TABLE_NAME}")
display(persisted_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Describe detail
display(spark.sql(f"DESCRIBE DETAIL {FULL_TARGET_TABLE_NAME}"))

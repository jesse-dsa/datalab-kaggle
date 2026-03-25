# Databricks notebook source
# MAGIC %md
# MAGIC # 04_01_exploratory_analysis_orders_enriched
# MAGIC
# MAGIC Objetivo:
# MAGIC - Explorar a tabela `silver_orders_enriched`
# MAGIC - Responder perguntas centrais do problema de negócio
# MAGIC - Identificar padrões de atraso, fricção e experiência do cliente
# MAGIC - Preparar a definição da camada Gold

# COMMAND ----------

from datetime import datetime, timezone
import json

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widgets de configuração
dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("schema_name", "dev")
dbutils.widgets.text("source_table_name", "silver_orders_enriched")

CATALOG_NAME = dbutils.widgets.get("catalog_name").strip()
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip()
SOURCE_TABLE_NAME = dbutils.widgets.get("source_table_name").strip()

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"
FULL_SOURCE_TABLE_NAME = f"{FULL_SCHEMA_NAME}.{SOURCE_TABLE_NAME}"

print("Parâmetros carregados com sucesso.")
print(f"FULL_SCHEMA_NAME = {FULL_SCHEMA_NAME}")
print(f"FULL_SOURCE_TABLE_NAME = {FULL_SOURCE_TABLE_NAME}")

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

# DBTITLE 1,Validação da tabela Silver integrada
ensure_table_exists(FULL_SOURCE_TABLE_NAME, "Tabela Silver integrada")

# COMMAND ----------

# DBTITLE 1,Leitura da Silver integrada
orders_enriched_df = spark.table(FULL_SOURCE_TABLE_NAME)

log_step("silver_read", "OK", f"Tabela lida com sucesso: {FULL_SOURCE_TABLE_NAME}")

# COMMAND ----------

# DBTITLE 1,Visão geral executiva
overview_df = (
    orders_enriched_df
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("order_id").alias("distinct_orders"),
        F.countDistinct("seller_id").alias("distinct_sellers"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.sum(F.when(F.col("is_delayed") == True, F.lit(1)).otherwise(F.lit(0))).alias("delayed_rows"),
        F.sum(F.when(F.col("has_negative_review") == True, F.lit(1)).otherwise(F.lit(0))).alias("rows_with_negative_review"),
        F.sum(F.when(F.col("is_order_payment_value_consistent") == False, F.lit(1)).otherwise(F.lit(0))).alias("rows_with_payment_gap"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
        F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
        F.round(F.avg("order_total_items_value_amount"), 2).alias("avg_order_items_value_amount")
    )
)

display(overview_df)

# COMMAND ----------

# DBTITLE 1,Quanto tempo o pedido leva entre compra, aprovação, expedição e entrega
journey_metrics_df = (
    orders_enriched_df
    .select("order_id", "purchase_to_approval_hours", "approval_to_carrier_hours", "carrier_to_customer_hours", "purchase_to_delivery_hours")
    .distinct()
    .agg(
        F.round(F.avg("purchase_to_approval_hours"), 2).alias("avg_purchase_to_approval_hours"),
        F.round(F.avg("approval_to_carrier_hours"), 2).alias("avg_approval_to_carrier_hours"),
        F.round(F.avg("carrier_to_customer_hours"), 2).alias("avg_carrier_to_customer_hours"),
        F.round(F.avg("purchase_to_delivery_hours"), 2).alias("avg_purchase_to_delivery_hours"),
        F.round(F.expr("percentile_approx(purchase_to_delivery_hours, 0.5)"), 2).alias("p50_purchase_to_delivery_hours"),
        F.round(F.expr("percentile_approx(purchase_to_delivery_hours, 0.9)"), 2).alias("p90_purchase_to_delivery_hours")
    )
)

display(journey_metrics_df)

# COMMAND ----------

# DBTITLE 1,Quais pedidos atrasaram
delayed_orders_df = (
    orders_enriched_df
    .select(
        "order_id",
        "order_status",
        "order_purchase_date",
        "order_estimated_delivery_date",
        "order_delivered_customer_date",
        "delivery_delay_days",
        "avg_review_score",
        "has_negative_review",
        "is_delayed"
    )
    .distinct()
    .filter(F.col("is_delayed") == True)
    .orderBy(F.col("delivery_delay_days").desc_nulls_last())
)

display(delayed_orders_df.limit(100))

# COMMAND ----------

# DBTITLE 1,Como o atraso se distribui por mês
delay_by_month_df = (
    orders_enriched_df
    .select("order_id", "order_purchase_date", "is_delayed", "delivery_delay_days")
    .distinct()
    .withColumn("order_purchase_month", F.date_trunc("month", F.col("order_purchase_date")))
    .groupBy("order_purchase_month")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum(F.when(F.col("is_delayed") == True, F.lit(1)).otherwise(F.lit(0))).alias("delayed_orders"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days")
    )
    .withColumn(
        "delay_rate",
        F.round(F.col("delayed_orders") / F.col("total_orders"), 4)
    )
    .orderBy("order_purchase_month")
)

display(delay_by_month_df)

# COMMAND ----------

# DBTITLE 1,Como o atraso se distribui no nível do item
delay_by_item_df = (
    orders_enriched_df
    .groupBy("product_category_name")
    .agg(
        F.count("*").alias("total_items"),
        F.sum(F.when(F.col("is_delayed") == True, F.lit(1)).otherwise(F.lit(0))).alias("delayed_items"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
        F.round(F.avg("item_total_value_amount"), 2).alias("avg_item_total_value_amount")
    )
    .withColumn(
        "delay_rate",
        F.round(F.col("delayed_items") / F.col("total_items"), 4)
    )
    .orderBy(F.col("delayed_items").desc(), F.col("avg_delay_days").desc_nulls_last())
)

display(delay_by_item_df.limit(50))

# COMMAND ----------

# DBTITLE 1,Quais sellers aparecem associados a pedidos com mais fricção
seller_friction_df = (
    orders_enriched_df
    .groupBy("seller_id", "seller_state")
    .agg(
        F.count("*").alias("total_items"),
        F.sum(F.when(F.col("is_delayed") == True, F.lit(1)).otherwise(F.lit(0))).alias("delayed_items"),
        F.sum(F.when(F.col("has_negative_review") == True, F.lit(1)).otherwise(F.lit(0))).alias("negative_review_items"),
        F.sum(F.when(F.col("is_order_payment_value_consistent") == False, F.lit(1)).otherwise(F.lit(0))).alias("payment_gap_items"),
        F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
        F.round(F.avg("avg_review_score"), 2).alias("avg_review_score")
    )
    .withColumn(
        "friction_score",
        F.col("delayed_items") + F.col("negative_review_items") + F.col("payment_gap_items")
    )
    .withColumn(
        "delay_rate",
        F.round(F.col("delayed_items") / F.col("total_items"), 4)
    )
    .withColumn(
        "negative_review_rate",
        F.round(F.col("negative_review_items") / F.col("total_items"), 4)
    )
    .orderBy(F.col("friction_score").desc(), F.col("avg_delay_days").desc_nulls_last())
)

display(seller_friction_df.limit(50))

# COMMAND ----------

# DBTITLE 1,Quais categorias têm produtos com características físicas mais problemáticas
problematic_physical_categories_df = (
    orders_enriched_df
    .groupBy("product_category_name")
    .agg(
        F.count("*").alias("total_items"),
        F.sum(F.when(F.col("has_invalid_dimensions") == True, F.lit(1)).otherwise(F.lit(0))).alias("invalid_dimension_items"),
        F.sum(F.when(F.col("has_non_positive_weight_g") == True, F.lit(1)).otherwise(F.lit(0))).alias("non_positive_weight_items"),
        F.sum(F.when(F.col("has_non_positive_dimensions") == True, F.lit(1)).otherwise(F.lit(0))).alias("non_positive_dimension_items"),
        F.round(F.avg("product_weight_g"), 2).alias("avg_weight_g"),
        F.round(F.avg("product_volume_cm3"), 2).alias("avg_volume_cm3"),
        F.sum(F.when(F.col("is_delayed") == True, F.lit(1)).otherwise(F.lit(0))).alias("delayed_items")
    )
    .orderBy(
        F.col("invalid_dimension_items").desc(),
        F.col("non_positive_weight_items").desc(),
        F.col("delayed_items").desc()
    )
)

display(problematic_physical_categories_df.limit(50))

# COMMAND ----------

# DBTITLE 1,Quais pedidos tiveram gap entre valor pago e soma dos itens
payment_gap_orders_df = (
    orders_enriched_df
    .select(
        "order_id",
        "order_total_payment_value_amount",
        "order_total_items_value_amount",
        "payment_to_items_value_gap_amount",
        "is_order_payment_value_consistent",
        "has_credit_card_payment",
        "has_boleto_payment",
        "has_voucher_payment"
    )
    .distinct()
    .filter(F.col("is_order_payment_value_consistent") == False)
    .orderBy(F.abs(F.col("payment_to_items_value_gap_amount")).desc_nulls_last())
)

display(payment_gap_orders_df.limit(100))

# COMMAND ----------

# DBTITLE 1,Quais pedidos têm review negativo
negative_review_orders_df = (
    orders_enriched_df
    .select(
        "order_id",
        "is_delayed",
        "delivery_delay_days",
        "avg_review_score",
        "has_negative_review",
        "has_review_comment",
        "seller_id",
        "product_category_name"
    )
    .distinct()
    .filter(F.col("has_negative_review") == True)
    .orderBy(F.col("avg_review_score").asc_nulls_last(), F.col("delivery_delay_days").desc_nulls_last())
)

display(negative_review_orders_df.limit(100))

# COMMAND ----------

# DBTITLE 1,Como pagamento, item, seller e experiência já podem ser observados juntos
integrated_view_df = (
    orders_enriched_df
    .select(
        "order_id",
        "order_item_id_int",
        "seller_id",
        "seller_state",
        "product_id",
        "product_category_name",
        "item_total_value_amount",
        "order_total_payment_value_amount",
        "max_payment_installments",
        "has_credit_card_payment",
        "has_boleto_payment",
        "has_voucher_payment",
        "avg_review_score",
        "has_negative_review",
        "is_delayed",
        "delivery_delay_days"
    )
    .orderBy(F.col("delivery_delay_days").desc_nulls_last(), F.col("avg_review_score").asc_nulls_last())
)

display(integrated_view_df.limit(100))

# COMMAND ----------

# DBTITLE 1,Diagnóstico cruzado: atraso x review negativo
delay_vs_review_df = (
    orders_enriched_df
    .select("order_id", "is_delayed", "has_negative_review")
    .distinct()
    .groupBy("is_delayed", "has_negative_review")
    .agg(F.count("*").alias("total_orders"))
    .orderBy("is_delayed", "has_negative_review")
)

display(delay_vs_review_df)

# COMMAND ----------

# DBTITLE 1,Resumo dos principais candidatos à Gold
gold_candidates_df = spark.createDataFrame(
    [
        ("gold_fact_orders", "Fato principal por pedido/item para análise integrada de jornada, pagamento, seller, produto e experiência"),
        ("gold_dim_customers", "Dimensão de clientes com atributos geográficos padronizados"),
        ("gold_dim_products", "Dimensão de produtos com categoria, peso, dimensões e volume"),
        ("gold_dim_sellers", "Dimensão de sellers com atributos geográficos padronizados"),
        ("gold_kpi_delivery_performance", "KPIs de atraso, tempo de entrega e distribuição temporal"),
        ("gold_kpi_review_logistics", "KPIs que relacionam atraso com score e review negativo"),
        ("gold_kpi_payment_consistency", "KPIs de gap entre valor pago e soma dos itens")
    ],
    ["gold_candidate", "business_purpose"]
)

display(gold_candidates_df)

# COMMAND ----------

log_step("exploratory_analysis", "OK", "Análise exploratória concluída com sucesso.")

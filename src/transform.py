from pyspark.sql.functions import col, when, round, trim, year, month, dayofmonth, quarter, date_format
from pyspark.sql.types import IntegerType, DoubleType, DateType

from config import (
    VALID_PAYMENT_METHODS,
    VALID_ORDER_STATUSES,
    AGE_HARD_MIN,
    AGE_HARD_MAX,
    MIN_DISCOUNT,
    MAX_DISCOUNT
)

def normalize_string_columns(df, columns):
    """
    标准化字符串字段：
    1. 去除前后空格。
    2. 将空字符串转换为 None。
    """
    for column in columns:
        df = df.withColumn(column, trim(col(column)))
        df = df.withColumn(
            column, 
            when(col(column) == "", None).otherwise(col(column))
        )
    return df

def clean_orders(orders_df):
    """
    清洗订单数据。

    规则：
    1. 订单主键、外键、日期、数量、价格、折扣进行类型转换。
    2. 过滤关键字段缺失数据。
    3. 过滤数量、单价、折扣异常数据。
    4. 过滤非法支付方式和非法订单状态。
    5. 根据order_id去重。
    6. 生成成交单价deal_unit_price和销售金额sales_amount。
    """
    df = orders_df \
            .withColumn("order_id", col("order_id").cast(IntegerType())) \
            .withColumn("customer_id", col("customer_id").cast(IntegerType())) \
            .withColumn("product_id", col("product_id").cast(IntegerType())) \
            .withColumn("order_date", col("order_date").cast(DateType())) \
            .withColumn("quantity", col("quantity").cast(IntegerType())) \
            .withColumn("unit_price", col("unit_price").cast(DoubleType())) \
            .withColumn("discount", col("discount").cast(DoubleType()))
    
    df = normalize_string_columns(df, ["payment_method", "order_status"])

    df = df.dropna(subset=[
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "discount",
        "payment_method",
        "order_status"
    ])

    df = df.filter(col("quantity") > 0)
    df = df.filter(col("unit_price") >= 0)
    df = df.filter(col("discount").between(MIN_DISCOUNT, MAX_DISCOUNT))

    df = df.filter(col("payment_method").isin(VALID_PAYMENT_METHODS))
    df = df.filter(col("order_status").isin(VALID_ORDER_STATUSES))

    df = df.dropDuplicates(["order_id"])

    df = df.withColumn(
        "deal_unit_price",
        round(col("unit_price") * (1 - col("discount")), 2)
    )

    df = df.withColumn(
        "sales_amount",
        round(col("quantity") * col("deal_unit_price"), 2)
    )

    df = df.withColumn(
        "is_valid_sales",
        when(col("order_status") == "已完成", 1).otherwise(0)
    )

    return df

def clean_customers(customers_df):
    """
    清洗客户数据。
    """
    df = customers_df \
            .withColumn("customer_id", col("customer_id").cast(IntegerType())) \
            .withColumn("age", col("age").cast(IntegerType())) \
            .withColumn("region_id", col("region_id").cast(IntegerType())) \
            .withColumn("register_date", col("register_date").cast(DateType()))
    
    df = normalize_string_columns(df, ["customer_name", "gender"])

    df = df.dropna(subset=[
        "customer_id",
        "customer_name",
        "gender",
        "age",
        "region_id",
        "register_date"
    ])

    df = df.filter(col("gender").isin(["男", "女"]))
    df = df.filter(col("age").between(AGE_HARD_MIN, AGE_HARD_MAX))
    df = df.dropDuplicates(["customer_id"])

    return df

def clean_products(products_df):
    """
    清洗商品数据。
    """

    df = products_df \
        .withColumn("product_id", col("product_id").cast(IntegerType())) \
        .withColumn("cost_price", col("cost_price").cast(DoubleType())) \
        .withColumn("sale_price", col("sale_price").cast(DoubleType()))

    df = normalize_string_columns(df, [
        "product_name",
        "category",
        "brand",
        "repurchase_level",
        "product_type"
    ])

    df = df.dropna(subset=[
        "product_id",
        "product_name",
        "category",
        "brand",
        "cost_price",
        "sale_price",
        "repurchase_level",
        "product_type"
    ])

    df = df.filter(col("repurchase_level").isin(["低", "中", "高"]))

    df = df.filter(
        col("product_type").isin([
            "普通商品",
            "赠品",
            "试用品",
            "积分兑换"
        ])
    )

    df = df.filter(
        (
            (col("product_type") == "普通商品")
            & (col("cost_price") > 0)
            & (col("sale_price") > 0)
        )
        |
        (
            (col("product_type").isin(["赠品", "试用品", "积分兑换"]))
            & (col("cost_price") >= 0)
            & (col("sale_price") >= 0)
        )
    )

    df = df.dropDuplicates(["product_id"])

    return df

def clean_regions(regions_df):
    """
    清洗地区数据。
    """
    df = regions_df.withColumn(
        "region_id", 
        col("region_id").cast(IntegerType())
    )

    df = normalize_string_columns(df, ["country", "province", "city"])

    df = df.dropna(subset=[
        "region_id",
        "country",
        "province",
        "city"
    ])

    df = df.dropDuplicates(["region_id"])

    return df

def build_enriched_orders(clean_orders_df, clean_customers_df, clean_products_df, clean_regions_df):
    """
    构建订单宽表。

    订单宽表=清洗后订单表+客户信息+商品信息+地区信息。
    """
    enriched_df = clean_orders_df \
                    .join(clean_customers_df, on="customer_id", how="left") \
                    .join(clean_products_df, on="product_id", how="left") \
                    .join(clean_regions_df, on="region_id", how="left")
    
    enriched_df = enriched_df.withColumn(
        "cost_amount",
        round(col("quantity") * col("cost_price"), 2)
    )

    enriched_df = enriched_df.withColumn(
        "profit_amount",
        round(col("sales_amount") - col("cost_amount"), 2)
    )

    # 普通销售额：只统计已完成 + 普通商品
    enriched_df = enriched_df.withColumn(
        "valid_sales_amount",
        when(
            (col("is_valid_sales") == 1) &
            (col("product_type") == "普通商品"),
            col("sales_amount")
        ).otherwise(0)
    )

    # 普通销售利润：只统计已完成 + 普通商品
    enriched_df = enriched_df.withColumn(
        "valid_profit_amount",
        when(
            (col("is_valid_sales") == 1) &
            (col("product_type") == "普通商品"),
            round(col("sales_amount") - col("cost_amount"), 2)
        ).otherwise(0)
    )

    # 非现金商品成本：统计赠品 / 试用品 / 积分兑换带来的成本
    enriched_df = enriched_df.withColumn(
        "non_cash_cost_amount",
        when(
            (col("is_valid_sales") == 1) &
            (col("product_type") != "普通商品"),
            col("cost_amount")
        ).otherwise(0)
    )

    enriched_df = enriched_df.select(
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "gender",
        "age",
        "register_date",
        "region_id",
        "country",
        "province",
        "city",
        "product_id",
        "product_name",
        "category",
        "brand",
        "repurchase_level",
        "product_type",
        "quantity",
        "unit_price",
        "discount",
        "deal_unit_price",
        "cost_price",
        "sale_price",
        "payment_method",
        "order_status",
        "sales_amount",
        "cost_amount",
        "profit_amount",
        "is_valid_sales",
        "valid_sales_amount",
        "valid_profit_amount",
        "non_cash_cost_amount"
    )

    return enriched_df

def build_fact_orders(enriched_orders_df):
    """
    构建订单事实表。
    """
    fact_orders_df = enriched_orders_df.withColumn(
        "date_id", 
        date_format(col("order_date"), "yyyyMMdd").cast(IntegerType())
    )

    fact_orders_df = fact_orders_df.select(
        "order_id",
        "date_id",
        "order_date",
        "customer_id",
        "product_id",
        "region_id",
        "quantity",
        "unit_price",
        "discount",
        "deal_unit_price",
        "cost_price",
        "sales_amount",
        "cost_amount",
        "profit_amount",
        "is_valid_sales",
        "valid_sales_amount",
        "valid_profit_amount",
        "non_cash_cost_amount",
        "payment_method",
        "order_status"
    )

    return fact_orders_df

def build_dim_customers(clean_customers_df):
    """
    构建客户维度表。
    """
    dim_customers_df = clean_customers_df.select(
        "customer_id",
        "customer_name",
        "gender",
        "age", 
        "register_date",
        "region_id"
    ).dropDuplicates(["customer_id"])

    return dim_customers_df

def build_dim_products(clean_products_df):
    """
    构建商品维度表。
    """
    dim_products_df = clean_products_df.select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "repurchase_level",
        "product_type",
        "cost_price",
        "sale_price"
    ).dropDuplicates(["product_id"])

    return dim_products_df

def build_dim_regions(clean_regions_df):
    """
    构建地区维度表。
    """
    dim_regions_df = clean_regions_df.select(
        "region_id",
        "country",
        "province",
        "city"
    ).dropDuplicates(["region_id"])

    return dim_regions_df

def build_dim_date(enriched_orders_df):
    """
    构建日期维度表。
    """
    dim_date_df = enriched_orders_df.select(
        "order_date"
    ).dropDuplicates(["order_date"])

    dim_date_df = dim_date_df \
        .withColumn(
            "date_id",
            date_format(col("order_date"), "yyyyMMdd").cast(IntegerType())
        ) \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("day", dayofmonth(col("order_date"))) \
        .withColumn("quarter", quarter(col("order_date")))

    dim_date_df = dim_date_df.select(
        "date_id",
        "order_date",
        "year",
        "month",
        "day",
        "quarter"
    )
    
    return dim_date_df
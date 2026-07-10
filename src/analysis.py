from pyspark.sql.functions import col, sum, count, round, desc, first, when
from pyspark.sql.window import Window

def get_valid_normal_sales_df(analysis_df):
    """
    获取有效普通商品销售数据。

    只保留：
    1. 已完成订单
    2. 普通商品
    """
    return analysis_df.filter(
        (col("is_valid_sales") == 1) &
        (col("product_type") == "普通商品")
    )

def build_analysis_base_table(
        fact_orders_df,
        dim_customers_df,
        dim_products_df,
        dim_regions_df,
        dim_date_df
):
    """
    构建销售分析基础表。
    """
    dim_customers_for_join = dim_customers_df.drop("region_id")
    dim_products_for_join = dim_products_df.drop("cost_price")
    dim_date_for_join = dim_date_df.drop("order_date")

    analysis_df = fact_orders_df \
        .join(dim_customers_for_join, on="customer_id", how="left") \
        .join(dim_products_for_join, on="product_id", how="left") \
        .join(dim_regions_df, on="region_id", how="left") \
        .join(dim_date_for_join, on="date_id", how="left")

    return analysis_df

def analyze_sales_by_product(analysis_df):
    """
    按商品分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计product_type为“普通商品”的商品。
    3. 不统计赠品、试用品、积分兑换。
    4. 销售额和利润基于valid_sales_amount / valid_profit_amount。

    返回结果：
    - product_id：商品ID
    - product_name：商品名称
    - category：商品类别
    - brand：品牌
    - repurchase_level：复购等级
    - product_type：商品类型
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("product_id") \
        .agg(
            first("product_name", ignorenulls=True).alias("product_name"),
            first("category", ignorenulls=True).alias("category"),
            first("brand", ignorenulls=True).alias("brand"),
            first("repurchase_level", ignorenulls=True).alias("repurchase_level"),
            first("product_type", ignorenulls=True).alias("product_type"),
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )

    result_df = result_df \
        .withColumn(
            "avg_order_value",
            when(
                col("valid_order_count") > 0,
                round(col("total_sales_amount") / col("valid_order_count"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_unit_revenue",
            when(
                col("total_quantity") > 0,
                round(col("total_sales_amount") / col("total_quantity"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "profit_margin",
            when(
                col("total_sales_amount") > 0,
                round(col("total_profit_amount") / col("total_sales_amount"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc("total_sales_amount"))

    return result_df

def analyze_sales_by_category(analysis_df):
    """
    按商品类别分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按category聚合销售额、利润、数量和订单数。
    4. 计算不同类别的订单占比和销售额占比。

    返回结果：
    - category：商品类别
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - order_count_ratio：订单数占比
    - sales_amount_ratio：销售额占比
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("category") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
    
    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        
    result_df = result_df \
        .withColumn(
            "all_valid_order_count",
            sum("valid_order_count").over(total_window)
        ) \
        .withColumn(
            "all_valid_sales_amount",
            sum("total_sales_amount").over(total_window)
        ) \
        .withColumn(
            "order_count_ratio",
            round(col("valid_order_count") / col("all_valid_order_count"), 4)
        ) \
        .withColumn(
            "sales_amount_ratio",
            round(col("total_sales_amount") / col("all_valid_sales_amount"), 4)
        ) \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "avg_unit_revenue",
            round(col("total_sales_amount") / col("total_quantity"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .drop("all_valid_order_count", "all_valid_sales_amount") \
        .orderBy(desc("total_sales_amount"))
    
    return result_df

def analyze_sales_by_region(analysis_df):
    """
    按地区分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按country / province / city 聚合。
    4. 用于观察不同地区的销售额、利润和客单价差异。

    返回结果：
    - country：国家
    - province：省份或州
    - city：城市
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - avg_order_value：客单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy(
            "country",
            "province",
            "city"
        ) \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
        
    result_df = result_df \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc("total_sales_amount"))
    
    return result_df

def analyze_monthly_sales(analysis_df):
    """
    按年月分析普通商品销售趋势。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按year / month聚合。
    4. 用于观察月度销售额、利润、销量和客单价变化。

    返回结果：
    - year：年份
    - month：月份
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("year", "month") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
        
    result_df = result_df \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "avg_unit_revenue",
            round(col("total_sales_amount") / col("total_quantity"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc("year"), desc("month"))
    
    return result_df

def analyze_yearly_sales(analysis_df):
    """
    按年份分析普通商品销售趋势。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按year聚合。
    4. 用于观察年度销售额、利润、销量和客单价变化。

    返回结果：
    - year：年份
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("year") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
        
    result_df = result_df \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "avg_unit_revenue",
            round(col("total_sales_amount") / col("total_quantity"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc("year"))
    
    return result_df

def analyze_discount_level(analysis_df):
    """
    按折扣区间分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按discount划分为无折扣、低折扣、中折扣、高折扣。
    4. 用于验证折扣生成规则是否合理，以及折扣对利润率的影响。

    返回结果：
    - discount_level：折扣区间
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - order_count_ratio：订单数占比
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """

    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    df = normal_sales_df.withColumn(
        "discount_level",
        when(col("discount") == 0, "无折扣")
        .when(col("discount") <= 0.05, "低折扣")
        .when(col("discount") <= 0.10, "中折扣")
        .otherwise("高折扣")
    )

    result_df = df \
        .groupBy("discount_level") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )

    total_window = Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    result_df = result_df \
        .withColumn(
            "all_valid_order_count",
            sum("valid_order_count").over(total_window)
        ) \
        .withColumn(
            "order_count_ratio",
            when(
                col("all_valid_order_count") > 0,
                round(col("valid_order_count") / col("all_valid_order_count"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_order_value",
            when(
                col("valid_order_count") > 0,
                round(col("total_sales_amount") / col("valid_order_count"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_unit_revenue",
            when(
                col("total_quantity") > 0,
                round(col("total_sales_amount") / col("total_quantity"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "profit_margin",
            when(
                col("total_sales_amount") > 0,
                round(col("total_profit_amount") / col("total_sales_amount"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .drop("all_valid_order_count") \
        .orderBy(desc("valid_order_count"))

    return result_df

def analyze_top_customers(analysis_df, top_n=10):
    """
    分析普通商品消费金额最高的客户。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按 customer_id 聚合。
    4. 默认返回销售额最高的前top_n名客户。

    参数：
    - top_n：返回客户数量，默认10。

    返回结果：
    - customer_id：客户ID
    - customer_name：客户姓名
    - gender：性别
    - age：年龄
    - country / province / city：客户所在地区
    - valid_order_count：有效订单数
    - total_quantity：购买数量
    - total_sales_amount：有效消费金额
    - total_profit_amount：贡献利润
    - avg_order_value：客单价
    - profit_margin：利润率
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("customer_id") \
        .agg(
            first("customer_name", ignorenulls=True).alias("customer_name"),
            first("gender", ignorenulls=True).alias("gender"),
            first("age", ignorenulls=True).alias("age"),
            first("country", ignorenulls=True).alias("country"),
            first("province", ignorenulls=True).alias("province"),
            first("city", ignorenulls=True).alias("city"),
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
        
    result_df = result_df \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc("total_sales_amount")) \
        .limit(top_n)
    
    return result_df

def analyze_payment_method(analysis_df):
    """
    按支付方式分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 不统计赠品、试用品、积分兑换。
    4. 用于观察不同支付方式下的订单数、销售额和利润表现。

    返回结果：
    - payment_method：支付方式
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - avg_order_value：客单价
    - profit_margin：利润率
    - order_count_ratio：订单数占比
    - sales_amount_ratio：销售额占比
    """
    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("payment_method") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )

    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    result_df = result_df \
        .withColumn(
            "all_valid_order_count",
            sum("valid_order_count").over(total_window)
        ) \
        .withColumn(
            "all_valid_sales_amount",
            sum("total_sales_amount").over(total_window)
        ) \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "order_count_ratio",
            round(col("valid_order_count") / col("all_valid_order_count"), 4)
        ) \
        .withColumn(
            "sales_amount_ratio",
            round(col("total_sales_amount") / col("all_valid_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount", 
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount", 
            round(col("total_profit_amount"), 2)
        ) \
        .drop("all_valid_order_count", "all_valid_sales_amount") \
        .orderBy(desc("total_sales_amount"))
    
    return result_df

def analyze_order_status(analysis_df):
    """
    分析不同订单状态下的订单分布和金额影响。

    分析口径：
    1. 不过滤is_valid_sales，因为该分析需要覆盖所有订单状态。
    2. 不过滤product_type，因为赠品、试用品、积分兑换也可能被取消或退款。
    3. valid_sales_amount只代表普通商品的有效销售额。
    4. non_cash_cost_amount代表赠品、试用品、积分兑换带来的非现金成本。

    返回结果：
    - order_status：订单状态
    - order_count：该状态下的订单数量
    - ordinary_order_count：普通商品订单数量
    - non_cash_order_count：非现金商品订单数量
    - total_quantity：商品数量
    - raw_sales_amount：原始销售金额
    - valid_sales_amount：普通商品有效销售额
    - non_cash_cost_amount：非现金商品成本
    - order_count_ratio：该状态订单数量占比
    """
    result_df = analysis_df \
        .groupBy("order_status") \
        .agg(
            count("*").alias("order_count"),
            sum(
                when(col("product_type") == "普通商品", 1).otherwise(0)
            ).alias("ordinary_order_count"),
            sum(
                when(col("product_type") != "普通商品", 1).otherwise(0)
            ).alias("non_cash_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("sales_amount").alias("raw_sales_amount"),
            sum("valid_sales_amount").alias("valid_sales_amount"),
            sum("non_cash_cost_amount").alias("non_cash_cost_amount")
        )

    total_window = Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    result_df = result_df \
        .withColumn(
            "all_order_count",
            sum("order_count").over(total_window)
        ) \
        .withColumn(
            "order_count_ratio",
            when(
                col("all_order_count") > 0,
                round(col("order_count") / col("all_order_count"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "raw_sales_amount",
            round(col("raw_sales_amount"), 2)
        ) \
        .withColumn(
            "valid_sales_amount",
            round(col("valid_sales_amount"), 2)
        ) \
        .withColumn(
            "non_cash_cost_amount",
            round(col("non_cash_cost_amount"), 2)
        ) \
        .drop("all_order_count") \
        .orderBy(desc("order_count"))

    return result_df

def analyze_sales_by_repurchase_level(analysis_df):
    """
    按复购等级分析普通商品销售表现。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 按repurchase_level聚合。
    4. 用于验证高复购商品是否订单量更高，以及不同复购等级的客单价和利润率差异。

    返回结果：
    - repurchase_level：复购等级
    - valid_order_count：有效订单数
    - total_quantity：销售数量
    - total_sales_amount：有效销售额
    - total_profit_amount：有效利润
    - order_count_ratio：订单数占比
    - sales_amount_ratio：销售额占比
    - avg_order_value：客单价
    - avg_unit_revenue：件单价
    - profit_margin：利润率
    """

    normal_sales_df = get_valid_normal_sales_df(analysis_df)

    result_df = normal_sales_df \
        .groupBy("repurchase_level") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )

    total_window = Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    result_df = result_df \
        .withColumn(
            "all_valid_order_count",
            sum("valid_order_count").over(total_window)
        ) \
        .withColumn(
            "all_valid_sales_amount",
            sum("total_sales_amount").over(total_window)
        ) \
        .withColumn(
            "order_count_ratio",
            when(
                col("all_valid_order_count") > 0,
                round(col("valid_order_count") / col("all_valid_order_count"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "sales_amount_ratio",
            when(
                col("all_valid_sales_amount") > 0,
                round(col("total_sales_amount") / col("all_valid_sales_amount"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_order_value",
            when(
                col("valid_order_count") > 0,
                round(col("total_sales_amount") / col("valid_order_count"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_unit_revenue",
            when(
                col("total_quantity") > 0,
                round(col("total_sales_amount") / col("total_quantity"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "profit_margin",
            when(
                col("total_sales_amount") > 0,
                round(col("total_profit_amount") / col("total_sales_amount"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .drop("all_valid_order_count", "all_valid_sales_amount") \
        .orderBy(desc("total_sales_amount"))

    return result_df

def analyze_sales_by_product_type(analysis_df):
    """
    按商品类型分析订单表现。

    分析口径：
    1. 只统计已完成订单。
    2. 不过滤product_type。
    3. 普通商品通过valid_sales_amount / valid_profit_amount统计销售表现。
    4. 赠品、试用品、积分兑换通过non_cash_cost_amount统计非现金成本。
    5. 用于观察普通销售与非现金商品成本之间的结构关系。

    返回结果：
    - product_type：商品类型
    - valid_order_count：有效订单数
    - total_quantity：商品数量
    - total_sales_amount：普通商品有效销售额
    - total_profit_amount：普通商品有效利润
    - total_non_cash_cost_amount：非现金商品成本
    - order_count_ratio：订单数占比
    - avg_order_value：客单价
    - profit_margin：利润率
    """
    result_df = analysis_df \
        .filter(col("is_valid_sales") == 1) \
        .groupBy("product_type") \
        .agg(
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount"),
            sum("non_cash_cost_amount").alias("total_non_cash_cost_amount")
        )

    total_window = Window.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    result_df = result_df \
        .withColumn(
            "all_valid_order_count",
            sum("valid_order_count").over(total_window)
        ) \
        .withColumn(
            "order_count_ratio",
            when(
                col("all_valid_order_count") > 0,
                round(col("valid_order_count") / col("all_valid_order_count"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_order_value",
            when(
                col("valid_order_count") > 0,
                round(col("total_sales_amount") / col("valid_order_count"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "profit_margin",
            when(
                col("total_sales_amount") > 0,
                round(col("total_profit_amount") / col("total_sales_amount"), 4)
            ).otherwise(0)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .withColumn(
            "total_non_cash_cost_amount",
            round(col("total_non_cash_cost_amount"), 2)
        ) \
        .drop("all_valid_order_count") \
        .orderBy(desc("valid_order_count"))

    return result_df

def analyze_non_cash_cost(analysis_df):
    """
    分析非现金商品成本。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计product_type不等于“普通商品”的记录。
    3. 覆盖赠品、试用品、积分兑换。
    4. 不计算销售额和普通利润，只统计non_cash_cost_amount。
    5. 用于观察营销赠品、试用品和积分兑换商品带来的成本。

    返回结果：
    - product_type：商品类型
    - category：商品类别
    - order_count：订单数量
    - total_quantity：商品数量
    - total_non_cash_cost_amount：非现金商品总成本
    - avg_non_cash_cost_per_order：单订单平均非现金成本
    - avg_non_cash_cost_per_unit：单件平均非现金成本
    """
    result_df = analysis_df \
        .filter(
            (col("is_valid_sales") == 1) &
            (col("product_type") != "普通商品")
        ) \
        .groupBy("product_type", "category") \
        .agg(
            count("*").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            sum("non_cash_cost_amount").alias("total_non_cash_cost_amount")
        )

    result_df = result_df \
        .withColumn(
            "avg_non_cash_cost_per_order",
            when(
                col("order_count") > 0,
                round(col("total_non_cash_cost_amount") / col("order_count"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "avg_non_cash_cost_per_unit",
            when(
                col("total_quantity") > 0,
                round(col("total_non_cash_cost_amount") / col("total_quantity"), 2)
            ).otherwise(0)
        ) \
        .withColumn(
            "total_non_cash_cost_amount",
            round(col("total_non_cash_cost_amount"), 2)
        ) \
        .orderBy(desc("total_non_cash_cost_amount"))

    return result_df

def analyze_top_customers_dynamic(
        analysis_df,
        country=None,
        province=None,
        city=None,
        year=None,
        month=None,
        category=None,
        payment_method=None,
        top_n=10,
        sort_by="total_sales_amount"
):
    """
    动态筛选普通商品消费最高的客户。

    分析口径：
    1. 只统计已完成订单。
    2. 只统计普通商品。
    3. 支持按国家、省份、城市、年份、月份、商品类别、支付方式筛选。
    4. 支持按销售额、利润、订单数、客单价、利润率排序。
    5. 用于模拟前端筛选查询，例如“上海市消费前100客户”。

    参数：
    - country：国家筛选条件。
    - province：省份或州筛选条件。
    - city：城市筛选条件。
    - year：年份筛选条件。
    - month：月份筛选条件。
    - category：商品类别筛选条件。
    - payment_method：支付方式筛选条件。
    - top_n：返回客户数量。
    - sort_by：排序字段。

    返回结果：
    - customer_id：客户ID
    - customer_name：客户姓名
    - gender：性别
    - age：年龄
    - country / province / city：客户所在地区
    - valid_order_count：有效订单数
    - total_quantity：购买数量
    - total_sales_amount：有效消费金额
    - total_profit_amount：贡献利润
    - avg_order_value：客单价
    - profit_margin：利润率
    """
    allowed_sort_fields = [
        "total_sales_amount",
        "total_profit_amount",
        "valid_order_count",
        "avg_order_value",
        "profit_margin"
    ]

    if sort_by not in allowed_sort_fields:
        raise ValueError(f"非法的排序字段: {sort_by}. 允许的字段包括: {allowed_sort_fields}")
    
    df = get_valid_normal_sales_df(analysis_df)

    if country:
        df = df.filter(col("country") == country)

    if province:
        df = df.filter(col("province") == province)
    
    if city:
        df = df.filter(col("city") == city)

    if year:
        df = df.filter(col("year") == year)

    if month:
        df = df.filter(col("month") == month)

    if category:
        df = df.filter(col("category") == category)

    if payment_method:
        df = df.filter(col("payment_method") == payment_method)

    result_df = df \
        .groupBy("customer_id") \
        .agg(
            first("customer_name", ignorenulls=True).alias("customer_name"),
            first("gender", ignorenulls=True).alias("gender"),
            first("age", ignorenulls=True).alias("age"),
            first("country", ignorenulls=True).alias("country"),
            first("province", ignorenulls=True).alias("province"),
            first("city", ignorenulls=True).alias("city"),
            count("*").alias("valid_order_count"),
            sum("quantity").alias("total_quantity"),
            sum("valid_sales_amount").alias("total_sales_amount"),
            sum("valid_profit_amount").alias("total_profit_amount")
        )
    
    result_df = result_df \
        .withColumn(
            "avg_order_value",
            round(col("total_sales_amount") / col("valid_order_count"), 2)
        ) \
        .withColumn(
            "profit_margin",
            round(col("total_profit_amount") / col("total_sales_amount"), 4)
        ) \
        .withColumn(
            "total_sales_amount",
            round(col("total_sales_amount"), 2)
        ) \
        .withColumn(
            "total_profit_amount",
            round(col("total_profit_amount"), 2)
        ) \
        .orderBy(desc(sort_by)) \
        .limit(top_n)
    
    return result_df
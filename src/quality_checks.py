import os
import csv
from datetime import datetime
from pyspark.sql.functions import (
    col,
    current_date,
    count as spark_count,
    sum as spark_sum
)

from config import (
    QUALITY_SUMMARY_REPORT_FILE,
    QUALITY_FULL_REPORT_FILE,
    QUALITY_ISSUES_FILE,
    NORMAL_AGE_MIN,
    NORMAL_AGE_MAX,
    AGE_HARD_MIN,
    AGE_HARD_MAX,
    MIN_DISCOUNT,
    MAX_DISCOUNT,
    VALID_PAYMENT_METHODS,
    VALID_ORDER_STATUSES
)

QUALITY_ISSUE_COLUMN_NAME_MAP = {
    "issue_id": "风险ID",
    "section": "检查模块",
    "check_name": "检查项",
    "risk_level": "风险等级",
    "value": "检查结果",
    "suggestion": "处理建议"
}

RISK_LEVEL_NAME_MAP = {
    "HIGH": "高",
    "MEDIUM": "中",
    "LOW": "低",
    "INFO": "信息"
}

def safe_count(df):
    """
    统计DataFrame行数。
    """
    return df.count()

def has_column(df, column_name):
    """
    判断DataFrame中是否存在某个字段。
    """
    return column_name in df.columns


def is_positive_number(value):
    """
    判断value是否为大于0的数字。

    用于判断风险是否存在。
    """
    return isinstance(value, (int, float)) and value > 0

def calculate_ratio(part_count, total_count):
    """
    计算比例。

    如果total_count为 0，则返回 0，避免除零错误。
    """
    if total_count == 0:
        return 0

    return round(part_count / total_count, 4)


def get_ratio_risk_level(value, low_threshold, medium_threshold, high_threshold):
    """
    根据比例返回风险等级。

    返回：
    - HIGH
    - MEDIUM
    - LOW
    - None
    """

    if value > high_threshold:
        return "HIGH"

    if value > medium_threshold:
        return "MEDIUM"

    if value > low_threshold:
        return "LOW"

    return None

def count_missing_values(df, column_name):
    """
    统计某一列为空的行数。

    如果字段不存在，返回字符串提示。
    """
    if not has_column(df, column_name):
        return "字段不存在"

    return df.filter(col(column_name).isNull()).count()

def get_duplicate_key_stats(df, key_column):
    """
    一次聚合统计重复主键数量和涉及重复主键的总行数。
    """
    if not has_column(df, key_column):
        return "字段不存在", "字段不存在"

    stats = df.groupBy(key_column) \
        .count() \
        .filter(col("count") > 1) \
        .agg(
            spark_count("*").alias("duplicated_key_count"),
            spark_sum("count").alias("duplicated_row_count")
        ) \
        .first()

    duplicated_key_count = stats["duplicated_key_count"]
    duplicated_row_count = stats["duplicated_row_count"] or 0

    return duplicated_key_count, duplicated_row_count

def add_section(lines, title):
    """
    给报告增加章节标题。
    """
    lines.append("")
    lines.append("=" * 100)
    lines.append(title)
    lines.append("=" * 100)

def add_full_check(full_report_lines, name, value, level="INFO", suggestion=None):
    """
    写入完整检查报告。

    无论是否存在风险，都会写入full_report_lines。
    用途：
    - 证明该检查项已经执行
    - 展示检查结果
    """
    full_report_lines.append(f"[{level}] {name}：{value}")

    if suggestion:
        full_report_lines.append(f"    说明：{suggestion}")

def add_summary_risk(summary_lines, section, name, value, level, suggestion=None):
    """
    写入风险摘要报告。

    只有真正存在风险时才调用这个函数。
    用途：
    - 快速查看问题
    """
    summary_lines.append(f"[{level}] {section} - {name}：{value}")

    if suggestion:
        summary_lines.append(f"    建议：{suggestion}")

def add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        name,
        value,
        normal_level="INFO",
        risk_level=None,
        has_risk=False,
        suggestion=None
):
    """
    统一处理质量检查结果。

    规则：
    1. 完整报告一定写入。
    2. 风险摘要txt和csv只在has_risk=True时写入。
    """
    if has_risk and risk_level is None:
        raise ValueError(f"检查项 {name} 已触发风险，但没有提供risk_level")
    
    level = risk_level if has_risk else normal_level

    add_full_check(
        full_report_lines=full_report_lines,
        name=name,
        value=value,
        level=level,
        suggestion=suggestion if has_risk else None
    )

    if has_risk:
        add_summary_risk(
            summary_lines=summary_lines,
            section=section,
            name=name,
            value=value,
            level=risk_level,
            suggestion=suggestion
        )

        issue_rows.append({
            "issue_id": len(issue_rows) + 1,
            "section": section,
            "check_name": name,
            "risk_level": risk_level,
            "value": value,
            "suggestion": suggestion if suggestion else ""
        })

        return 1
    
    return 0

def write_report_to_txt(report_lines, output_file):
    """
    将质量报告写入本地txt文件。
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        for line in report_lines:
            f.write(line + "\n")

def write_issues_to_csv(issue_rows, output_file):
    """
    将风险明细写入本地 CSV 文件。

    说明：
    1. issue_rows内部仍然使用英文key。
    2. 导出CSV时将字段名转换为中文。
    3. 风险等级也会从HIGH/MEDIUM/LOW转换为中文。
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    source_fieldnames = [
        "issue_id",
        "section",
        "check_name",
        "risk_level",
        "value",
        "suggestion"
    ]

    target_fieldnames = [
        QUALITY_ISSUE_COLUMN_NAME_MAP[field_name]
        for field_name in source_fieldnames
    ]

    converted_rows = []

    for row in issue_rows:
        converted_row = {}

        for field_name in source_fieldnames:
            chinese_field_name = QUALITY_ISSUE_COLUMN_NAME_MAP[field_name]
            value = row.get(field_name, "")

            if field_name == "risk_level":
                value = RISK_LEVEL_NAME_MAP.get(value, value)

            converted_row[chinese_field_name] = value

        converted_rows.append(converted_row)

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=target_fieldnames)
        writer.writeheader()
        writer.writerows(converted_rows)

def generate_data_quality_reports(
    raw_orders_df,
    raw_customers_df,
    raw_products_df,
    raw_regions_df,
    clean_orders_df,
    clean_customers_df,
    clean_products_df,
    clean_regions_df,
    enriched_orders_df,
    fact_orders_df,
    dim_customers_df,
    dim_products_df,
    dim_regions_df,
    dim_date_df,
    summary_output_file=QUALITY_SUMMARY_REPORT_FILE,
    full_output_file=QUALITY_FULL_REPORT_FILE,
    issues_output_file=QUALITY_ISSUES_FILE
):
    """
    生成数据质量报告。

    报告内容包括：
    1. 数据规模检查
    2. 主键唯一性检查
    3. 关键字段缺失检查
    4. 年龄风险检查
    5. 时间风险检查
    6. 商品价格风险检查
    7. 折扣与成交价格检查
    8. 多表关联结果检查
    9. 仓库表检查
    """
    summary_lines = []
    full_report_lines = []
    issue_rows = []
    risk_count = 0

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary_lines.append("电商数据管道质量风险摘要")
    summary_lines.append(f"生成时间：{now_text}")
    summary_lines.append(f"风险摘要文件：{summary_output_file}")
    summary_lines.append(f"完整检查文件：{full_output_file}")
    summary_lines.append(f"风险明细文件：{issues_output_file}")
    summary_lines.append("")

    full_report_lines.append("电商数据管道完整质量检查报告")
    full_report_lines.append(f"生成时间：{now_text}")
    full_report_lines.append(f"完整检查文件：{full_output_file}")
    full_report_lines.append(f"风险摘要文件：{summary_output_file}")
    full_report_lines.append(f"风险明细文件：{issues_output_file}")
    full_report_lines.append("")

    # 一、数据规模检查
    section = "数据规模检查"
    add_section(full_report_lines, f"一、{section}")

    raw_orders_count = safe_count(raw_orders_df)
    clean_orders_count = safe_count(clean_orders_df)
    removed_orders_count = raw_orders_count - clean_orders_count

    raw_customers_count = safe_count(raw_customers_df)
    clean_customers_count = safe_count(clean_customers_df)

    raw_products_count = safe_count(raw_products_df)
    clean_products_count = safe_count(clean_products_df)

    raw_regions_count = safe_count(raw_regions_df)
    clean_regions_count = safe_count(clean_regions_df)

    enriched_orders_count = safe_count(enriched_orders_df)

    add_full_check(full_report_lines, "原始订单数量", raw_orders_count, "INFO")
    add_full_check(full_report_lines, "清洗后订单数量", clean_orders_count, "INFO")
    add_full_check(full_report_lines, "被过滤订单数量", removed_orders_count, "INFO")

    removed_ratio = calculate_ratio(
        removed_orders_count,
        raw_orders_count
    )

    removed_ratio_level = get_ratio_risk_level(
        value=removed_ratio,
        low_threshold=0.01,
        medium_threshold=0.03,
        high_threshold=0.10
    )

    risk_count += add_quality_check(
        full_report_lines=full_report_lines,
        summary_lines=summary_lines,
        issue_rows=issue_rows,
        section=section,
        name="订单过滤比例",
        value=removed_ratio,
        risk_level=removed_ratio_level,
        has_risk=removed_ratio_level is not None,
        suggestion="订单过滤比例偏高，建议检查可能的风险字段。"
    )

    add_full_check(full_report_lines, "原始客户数量", raw_customers_count)
    add_full_check(full_report_lines, "清洗后客户数量", clean_customers_count)
    add_full_check(full_report_lines, "原始商品数量", raw_products_count)
    add_full_check(full_report_lines, "清洗后商品数量", clean_products_count)
    add_full_check(full_report_lines, "原始地区数量", raw_regions_count)
    add_full_check(full_report_lines, "清洗后地区数量", clean_regions_count)
    add_full_check(full_report_lines, "订单宽表数量", enriched_orders_count)

    risk_count += add_quality_check(
        full_report_lines=full_report_lines,
        summary_lines=summary_lines,
        issue_rows=issue_rows,
        section=section,
        name="订单宽表数量变化风险",
        value=f"clean_orders={clean_orders_count}, enriched_orders={enriched_orders_count}",
        risk_level="HIGH",
        has_risk=clean_orders_count != enriched_orders_count,
        suggestion="订单关联后数量发生变化，可能存在关联方式错误或维度表主键重复。"
    )
    
    # 二、主键唯一性检查
    section = "主键唯一性检查"
    add_section(full_report_lines, f"二、{section}")

    key_checks = [
        ("raw_orders.order_id", raw_orders_df, "order_id"),
        ("raw_customers.customer_id", raw_customers_df, "customer_id"),
        ("raw_products.product_id", raw_products_df, "product_id"),
        ("raw_regions.region_id", raw_regions_df, "region_id"),
        ("clean_orders.order_id", clean_orders_df, "order_id"),
        ("clean_customers.customer_id", clean_customers_df, "customer_id"),
        ("clean_products.product_id", clean_products_df, "product_id"),
        ("clean_regions.region_id", clean_regions_df, "region_id"),
        ("dim_customers.customer_id", dim_customers_df, "customer_id"),
        ("dim_products.product_id", dim_products_df, "product_id"),
        ("dim_regions.region_id", dim_regions_df, "region_id"),
        ("dim_date.date_id", dim_date_df, "date_id"),
    ]

    for label, df, key_column in key_checks:
        duplicated_key_count, duplicated_row_count = get_duplicate_key_stats(df, key_column)

        has_risk = (
            duplicated_key_count == "字段不存在"
            or is_positive_number(duplicated_key_count)
        )

        suggestion = (
            f"{key_column} 存在重复或字段不存在，会影响去重、关联和统计结果。"
            f"涉及重复主键的行数：{duplicated_row_count}"
        )

        risk_count += add_quality_check(
            full_report_lines=full_report_lines,
            summary_lines=summary_lines,
            issue_rows=issue_rows,
            section=section,
            name=f"{label} 重复主键数量",
            value=duplicated_key_count,
            risk_level="HIGH",
            has_risk=has_risk,
            suggestion=suggestion
        )

    # 三、关键字段缺失检查
    section = "关键字段缺失检查"
    add_section(full_report_lines, f"三、{section}")

    missing_checks = [
        ("raw_orders.order_id", raw_orders_df, "order_id"),
        ("raw_orders.customer_id", raw_orders_df, "customer_id"),
        ("raw_orders.product_id", raw_orders_df, "product_id"),
        ("raw_orders.order_date", raw_orders_df, "order_date"),
        ("raw_orders.quantity", raw_orders_df, "quantity"),
        ("raw_orders.unit_price", raw_orders_df, "unit_price"),
        ("raw_orders.discount", raw_orders_df, "discount"),
        ("raw_orders.payment_method", raw_orders_df, "payment_method"),
        ("raw_orders.order_status", raw_orders_df, "order_status"),

        ("raw_customers.customer_id", raw_customers_df, "customer_id"),
        ("raw_customers.customer_name", raw_customers_df, "customer_name"),
        ("raw_customers.gender", raw_customers_df, "gender"),
        ("raw_customers.age", raw_customers_df, "age"),
        ("raw_customers.region_id", raw_customers_df, "region_id"),
        ("raw_customers.register_date", raw_customers_df, "register_date"),

        ("raw_products.product_id", raw_products_df, "product_id"),
        ("raw_products.product_name", raw_products_df, "product_name"),
        ("raw_products.category", raw_products_df, "category"),
        ("raw_products.brand", raw_products_df, "brand"),
        ("raw_products.cost_price", raw_products_df, "cost_price"),
        ("raw_products.sale_price", raw_products_df, "sale_price"),
        ("raw_products.repurchase_level", raw_products_df, "repurchase_level"),
        ("raw_products.product_type", raw_products_df, "product_type"),

        ("raw_regions.region_id", raw_regions_df, "region_id"),
        ("raw_regions.country", raw_regions_df, "country"),
        ("raw_regions.province", raw_regions_df, "province"),
        ("raw_regions.city", raw_regions_df, "city"),
    ]

    for label, df, column_name in missing_checks:
        missing_count = count_missing_values(df, column_name)

        has_risk = (
            missing_count == "字段不存在"
            or is_positive_number(missing_count)
        )

        risk_count += add_quality_check(
            full_report_lines=full_report_lines,
            summary_lines=summary_lines,
            issue_rows=issue_rows,
            section=section,
            name=f"{label} 缺失数量",
            value=missing_count,
            risk_level="HIGH",
            has_risk=has_risk,
            suggestion="关键字段缺失会导致清洗过滤、关联失败或统计错误。"
        )

    # 四、客户年龄风险检查
    section = "客户年龄风险检查"
    add_section(full_report_lines, f"四、{section}")

    hard_age_risk_count = raw_customers_df.filter(
        (col("age") < AGE_HARD_MIN) | (col("age") > AGE_HARD_MAX)
    ).count()

    under_normal_age_count = clean_customers_df.filter(
        col("age") < NORMAL_AGE_MIN
    ).count()

    over_normal_age_count = clean_customers_df.filter(
        col("age") > NORMAL_AGE_MAX
    ).count()

    age_under_10_count = clean_customers_df.filter(
        col("age") < 10
    ).count()

    age_over_100_count = clean_customers_df.filter(
        col("age") > 100
    ).count()

    age_zero_count = clean_customers_df.filter(
        col("age") == 0
    ).count()

    age_150_count = clean_customers_df.filter(
        col("age") == 150
    ).count()

    under_normal_age_ratio = calculate_ratio(
        under_normal_age_count,
        clean_customers_count
    )

    over_normal_age_ratio = calculate_ratio(
        over_normal_age_count,
        clean_customers_count
    )

    age_under_10_ratio = calculate_ratio(
        age_under_10_count,
        clean_customers_count
    )

    age_over_100_ratio = calculate_ratio(
        age_over_100_count,
        clean_customers_count
    )

    age_zero_ratio = calculate_ratio(
        age_zero_count,
        clean_customers_count
    )

    age_150_ratio = calculate_ratio(
        age_150_count,
        clean_customers_count
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        f"超出年龄硬边界数量（{AGE_HARD_MIN}-{AGE_HARD_MAX}）",
        hard_age_risk_count,
        risk_level="HIGH",
        has_risk=hard_age_risk_count > 0,
        suggestion="超出硬边界的年龄应在清洗阶段过滤。"
    )

    under_normal_age_level = get_ratio_risk_level(
        value=under_normal_age_ratio,
        low_threshold=0,
        medium_threshold=0.05,
        high_threshold=0.10
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        f"低于正常年龄范围比例（age < {NORMAL_AGE_MIN}）",
        under_normal_age_ratio,
        risk_level=under_normal_age_level,
        has_risk=under_normal_age_level is not None,
        suggestion="未成年客户属于业务风险数据。如果数量过多，说明年龄生成或录入规则可能异常。"
    )

    over_normal_age_level = get_ratio_risk_level(
        value=over_normal_age_ratio,
        low_threshold=0,
        medium_threshold=0.03,
        high_threshold=0.08
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        f"高于正常年龄范围比例（age > {NORMAL_AGE_MAX}）",
        over_normal_age_ratio,
        risk_level=over_normal_age_level,
        has_risk=over_normal_age_level is not None,
        suggestion="高龄客户属于业务风险数据。如果数量过多，说明年龄生成或录入规则可能异常。"
    )

    age_under_10_level = get_ratio_risk_level(
        value=age_under_10_ratio,
        low_threshold=0,
        medium_threshold=0.01,
        high_threshold=0.03
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "10岁以下客户比例",
        age_under_10_ratio,
        risk_level=age_under_10_level,
        has_risk=age_under_10_level is not None,
        suggestion="10岁以下客户属于业务风险数据，可能是家长代购、账号异常或模拟风险数据。"
    )

    age_over_100_level = get_ratio_risk_level(
        value=age_over_100_ratio,
        low_threshold=0,
        medium_threshold=0.005,
        high_threshold=0.01
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "100岁以上客户比例",
        age_over_100_ratio,
        risk_level=age_over_100_level,
        has_risk=age_over_100_level is not None,
        suggestion="100岁以上客户属于业务风险数据，可能是家人代购、账号异常或模拟风险数据。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "年龄为0的客户比例",
        age_zero_ratio,
        risk_level="LOW",
        has_risk=age_zero_count > 0,
        suggestion="年龄为0通常是极端风险数据，应保持极低比例。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "年龄为150的客户比例",
        age_150_ratio,
        risk_level="LOW",
        has_risk=age_150_count > 0,
        suggestion="年龄为150通常是极端风险数据，应保持极低比例。"
    )

    # 五、时间风险检查
    section = "时间风险检查"
    add_section(full_report_lines, f"五、{section}")

    future_register_count = clean_customers_df.filter(
        col("register_date") > current_date()
    ).count()

    future_order_count = clean_orders_df.filter(
        col("order_date") > current_date()
    ).count()

    if has_column(enriched_orders_df, "register_date"):
        order_before_register_count = enriched_orders_df.filter(
            col("order_date") < col("register_date")
        ).count()
    else:
        order_before_register_count = "字段不存在"

    future_register_ratio = calculate_ratio(
        future_register_count,
        clean_customers_count
    )

    future_order_ratio = calculate_ratio(
        future_order_count,
        clean_orders_count
    )

    order_before_register_ratio = (
        calculate_ratio(order_before_register_count, enriched_orders_count)
        if isinstance(order_before_register_count, int)
        else "字段不存在"
    )

    future_register_level = get_ratio_risk_level(
        value=future_register_ratio,
        low_threshold=0,
        medium_threshold=0.01,
        high_threshold=0.03
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "未来注册日期客户比例",
        future_register_ratio,
        risk_level=future_register_level,
        has_risk=future_register_level is not None,
        suggestion="注册日期晚于当前日期，通常是系统时间、测试数据或生成范围问题。"
    )

    future_order_level = get_ratio_risk_level(
        value=future_order_ratio,
        low_threshold=0,
        medium_threshold=0.01,
        high_threshold=0.03
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "未来订单日期比例",
        future_order_ratio,
        risk_level=future_order_level,
        has_risk=future_order_level is not None,
        suggestion="订单日期晚于当前日期，通常需要检查订单生成时间范围。"
    )

    if order_before_register_ratio == "字段不存在":
        order_before_register_level = "HIGH"
        order_before_register_has_risk = True
    else:
        order_before_register_level = get_ratio_risk_level(
            value=order_before_register_ratio,
            low_threshold=0,
            medium_threshold=0.01,
            high_threshold=0.03
        )
        order_before_register_has_risk = order_before_register_level is not None

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "订单日期早于客户注册日期比例",
        order_before_register_ratio,
        risk_level=order_before_register_level,
        has_risk=order_before_register_has_risk,
        suggestion="订单早于注册日期通常不合理，可能表示客户注册时间或订单时间异常。"
    )

    # 六、商品价格、复购等级与商品类型风险检查
    section = "商品价格、复购等级与商品类型风险检查"
    add_section(full_report_lines, f"六、{section}")

    invalid_product_type_count = raw_products_df.filter(
        ~col("product_type").isin(["普通商品", "赠品", "试用品", "积分兑换"])
    ).count()

    invalid_repurchase_level_count = raw_products_df.filter(
        ~col("repurchase_level").isin(["低", "中", "高"])
    ).count()

    normal_product_price_error_count = raw_products_df.filter(
        (col("product_type") == "普通商品")
        & (
            (col("cost_price") <= 0)
            | (col("sale_price") <= 0)
        )
    ).count()

    special_product_negative_price_count = raw_products_df.filter(
        col("product_type").isin(["赠品", "试用品", "积分兑换"])
        & (
            (col("cost_price") < 0)
            | (col("sale_price") < 0)
        )
    ).count()

    special_product_zero_price_count = raw_products_df.filter(
        col("product_type").isin(["赠品", "试用品", "积分兑换"])
        & (
            (col("cost_price") == 0)
            | (col("sale_price") == 0)
        )
    ).count()

    normal_sale_less_equal_cost_count = clean_products_df.filter(
        (col("product_type") == "普通商品")
        & (col("sale_price") <= col("cost_price"))
    ).count()

    special_product_count = clean_products_df.filter(
        col("product_type").isin(["赠品", "试用品", "积分兑换"])
    ).count()

    special_product_ratio = calculate_ratio(
        special_product_count,
        clean_products_count
    )

    normal_sale_less_equal_cost_ratio = calculate_ratio(
        normal_sale_less_equal_cost_count,
        clean_products_count
    )

    normal_sale_less_equal_cost_level = get_ratio_risk_level(
        value=normal_sale_less_equal_cost_ratio,
        low_threshold=0,
        medium_threshold=0.03,
        high_threshold=0.10
    )

    special_product_ratio_level = get_ratio_risk_level(
        value=special_product_ratio,
        low_threshold=0.25,
        medium_threshold=0.35,
        high_threshold=0.50
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "非法商品类型数量",
        invalid_product_type_count,
        risk_level="HIGH",
        has_risk=invalid_product_type_count > 0,
        suggestion="product_type只允许 普通商品 / 赠品 / 试用品 / 积分兑换。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "非法复购等级数量",
        invalid_repurchase_level_count,
        risk_level="HIGH",
        has_risk=invalid_repurchase_level_count > 0,
        suggestion="repurchase_level只允许 低 / 中 / 高。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "普通商品价格异常数量",
        normal_product_price_error_count,
        risk_level="HIGH",
        has_risk=normal_product_price_error_count > 0,
        suggestion="普通商品的cost_price和sale_price都应大于0。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "特殊商品负价格数量",
        special_product_negative_price_count,
        risk_level="HIGH",
        has_risk=special_product_negative_price_count > 0,
        suggestion="赠品、试用品和积分兑换商品可以为0，但不应为负数。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "特殊商品零价格数量",
        special_product_zero_price_count,
        risk_level="LOW",
        has_risk=special_product_zero_price_count > 0,
        suggestion="赠品、试用品和积分兑换商品允许出现零价格，但需要在商品类型中明确标识。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "普通商品售价小于等于成本价比例",
        normal_sale_less_equal_cost_ratio,
        risk_level=normal_sale_less_equal_cost_level,
        has_risk=normal_sale_less_equal_cost_level is not None,
        suggestion="普通商品售价小于等于成本价可能是促销、清仓或价格数据异常，需要结合业务场景判断。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "特殊商品比例提醒",
        special_product_ratio,
        risk_level=special_product_ratio_level,
        has_risk=special_product_ratio_level is not None,
        suggestion="赠品、试用品和积分兑换商品比例过高时，可能影响普通销售分析。"
    ) 

    # 七、订单金额、折扣与利润风险检查
    section = "订单金额、折扣与利润风险检查"
    add_section(full_report_lines, f"七、{section}")

    invalid_quantity_count = raw_orders_df.filter(
        col("quantity") <= 0
    ).count()

    invalid_unit_price_count = raw_orders_df.filter(
        col("unit_price") < 0
    ).count()

    invalid_discount_count = raw_orders_df.filter(
        (col("discount") < MIN_DISCOUNT)
        | (col("discount") > MAX_DISCOUNT)
    ).count()

    negative_deal_unit_price_count = clean_orders_df.filter(
        col("deal_unit_price") < 0
    ).count()

    ordinary_zero_deal_unit_price_count = enriched_orders_df.filter(
        (col("product_type") == "普通商品") &
        (col("deal_unit_price") <= 0)
    ).count()

    negative_sales_amount_count = clean_orders_df.filter(
        col("sales_amount") < 0
    ).count()

    negative_profit_count = enriched_orders_df.filter(
        (col("product_type") == "普通商品") &
        (col("profit_amount") < 0)
    ).count()

    zero_discount_count = clean_orders_df.filter(
        col("discount") == 0
    ).count()

    low_discount_count = clean_orders_df.filter(
        (col("discount") > 0) & (col("discount") <= 0.05)
    ).count()

    high_discount_count = clean_orders_df.filter(
        col("discount") >= 0.15
    ).count()

    invalid_quantity_ratio = calculate_ratio(
        invalid_quantity_count,
        raw_orders_count
    )

    invalid_unit_price_ratio = calculate_ratio(
        invalid_unit_price_count,
        raw_orders_count
    )

    negative_profit_ratio = calculate_ratio(
        negative_profit_count,
        enriched_orders_count
    )

    zero_discount_ratio = calculate_ratio(
        zero_discount_count,
        clean_orders_count
    )

    low_discount_ratio = calculate_ratio(
        low_discount_count,
        clean_orders_count
    )

    high_discount_ratio = calculate_ratio(
        high_discount_count,
        clean_orders_count
    )

    invalid_quantity_level = get_ratio_risk_level(
        value=invalid_quantity_ratio,
        low_threshold=0,
        medium_threshold=0.01,
        high_threshold=0.03
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "原始订单中quantity小于等于0的比例",
        invalid_quantity_ratio,
        risk_level=invalid_quantity_level,
        has_risk=invalid_quantity_level is not None,
        suggestion="购买数量小于等于0会影响销售金额，应在清洗阶段过滤。"
    )

    invalid_unit_price_level = get_ratio_risk_level(
        value=invalid_unit_price_ratio,
        low_threshold=0,
        medium_threshold=0.01,
        high_threshold=0.03
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "原始订单中unit_price小于0的比例",
        invalid_unit_price_ratio,
        risk_level=invalid_unit_price_level,
        has_risk=invalid_unit_price_level is not None,
        suggestion="订单单价小于0会导致销售金额异常，应在清洗阶段过滤。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "原始订单中非法discount的数量",
        invalid_discount_count,
        risk_level="HIGH",
        has_risk=invalid_discount_count > 0,
        suggestion="折扣必须位于配置范围内。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "清洗后成交单价小于0的数量",
        negative_deal_unit_price_count,
        risk_level="HIGH",
        has_risk=negative_deal_unit_price_count > 0,
        suggestion="成交单价为负通常表示单价或折扣规则异常。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "普通商品成交单价小于等于0的数量",
        ordinary_zero_deal_unit_price_count,
        risk_level="HIGH",
        has_risk=ordinary_zero_deal_unit_price_count > 0,
        suggestion="普通商品成交单价应大于0。如果为0，通常说明商品类型、价格或折扣逻辑异常。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "清洗后销售金额小于0的数量",
        negative_sales_amount_count,
        risk_level="HIGH",
        has_risk=negative_sales_amount_count > 0,
        suggestion="销售金额为负通常表示单价、数量或折扣异常。"
    )

    negative_profit_level = get_ratio_risk_level(
        value=negative_profit_ratio,
        low_threshold=0,
        medium_threshold=0.03,
        high_threshold=0.10
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "负利润订单比例",
        negative_profit_ratio,
        risk_level=negative_profit_level,
        has_risk=negative_profit_level is not None,
        suggestion="负利润可能来自促销、亏本清仓或价格数据异常。"
    )

    high_discount_level = get_ratio_risk_level(
        value=high_discount_ratio,
        low_threshold=0.02,
        medium_threshold=0.05,
        high_threshold=0.10
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "高折扣订单比例（discount >= 0.15）",
        high_discount_ratio,
        risk_level=high_discount_level,
        has_risk=high_discount_level is not None,
        suggestion="高折扣订单允许少量存在；如果比例过高，可能为风险数据。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "无折扣订单比例偏低风险",
        zero_discount_ratio,
        risk_level="LOW",
        has_risk=zero_discount_ratio < 0.40,
        suggestion="原价订单较多通常可以接受；如果无折扣订单比例过低，可能说明促销策略过于激进。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "低折扣订单比例",
        low_discount_ratio,
        risk_level="LOW",
        has_risk=low_discount_ratio > 0.30,
        suggestion="低折扣订单较多通常可以接受，但如果比例过高，可能说明促销策略过于密集。"
    )

    # 八、业务枚举值检查
    section = "业务枚举值检查"
    add_section(full_report_lines, f"八、{section}")

    raw_invalid_payment_count = raw_orders_df.filter(
        ~col("payment_method").isin(VALID_PAYMENT_METHODS)
    ).count()

    raw_invalid_status_count = raw_orders_df.filter(
        ~col("order_status").isin(VALID_ORDER_STATUSES)
    ).count()

    clean_invalid_payment_count = clean_orders_df.filter(
        ~col("payment_method").isin(VALID_PAYMENT_METHODS)
    ).count()

    clean_invalid_status_count = clean_orders_df.filter(
        ~col("order_status").isin(VALID_ORDER_STATUSES)
    ).count()

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "原始订单非法支付方式数量",
        raw_invalid_payment_count,
        risk_level="MEDIUM",
        has_risk=raw_invalid_payment_count > 0,
        suggestion="原始数据中存在非法支付方式，清洗阶段应过滤。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "原始订单非法订单状态数量",
        raw_invalid_status_count,
        risk_level="MEDIUM",
        has_risk=raw_invalid_status_count > 0,
        suggestion="原始数据中存在非法订单状态，清洗阶段应过滤。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "清洗后非法支付方式数量",
        clean_invalid_payment_count,
        risk_level="HIGH",
        has_risk=clean_invalid_payment_count > 0,
        suggestion="清洗后仍存在非法支付方式，说明清洗规则未生效。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "清洗后非法订单状态数量",
        clean_invalid_status_count,
        risk_level="HIGH",
        has_risk=clean_invalid_status_count > 0,
        suggestion="清洗后仍存在非法订单状态，说明清洗规则未生效。"
    )

    # 九、多表关联质量检查
    section = "多表关联质量检查"
    add_section(full_report_lines, f"九、{section}")

    missing_customer_info_count = enriched_orders_df.filter(
        col("customer_name").isNull()
    ).count()

    missing_product_info_count = enriched_orders_df.filter(
        col("product_name").isNull()
    ).count()

    missing_region_info_count = enriched_orders_df.filter(
        col("city").isNull()
    ).count()

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "关联后缺失客户信息订单数量",
        missing_customer_info_count,
        risk_level="HIGH",
        has_risk=missing_customer_info_count > 0,
        suggestion="订单customer_id无法关联到客户维度。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "关联后缺失商品信息订单数量",
        missing_product_info_count,
        risk_level="HIGH",
        has_risk=missing_product_info_count > 0,
        suggestion="订单product_id无法关联到商品维度。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "关联后缺失地区信息订单数量",
        missing_region_info_count,
        risk_level="HIGH",
        has_risk=missing_region_info_count > 0,
        suggestion="客户region_id无法关联到地区维度。"
    )

    # 十、数据分布提醒
    section = "数据分布提醒"
    add_section(full_report_lines, f"十、{section}")

    completed_order_count = clean_orders_df.filter(
        col("order_status") == "已完成"
    ).count()

    completed_order_ratio = calculate_ratio(
        completed_order_count,
        clean_orders_count
    )

    refund_order_count = clean_orders_df.filter(
        col("order_status") == "已退款"
    ).count()

    refund_order_ratio = calculate_ratio(
        refund_order_count,
        clean_orders_count
    )

    cancelled_order_count = clean_orders_df.filter(
        col("order_status") == "已取消"
    ).count()

    cancelled_order_ratio = calculate_ratio(
        cancelled_order_count,
        clean_orders_count
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "已完成订单比例偏低提醒",
        completed_order_ratio,
        risk_level="LOW",
        has_risk=completed_order_ratio < 0.50,
        suggestion="已完成订单比例偏低不一定是错误，但会影响销售分析样本量。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "退款订单比例偏高提醒",
        refund_order_ratio,
        risk_level="LOW",
        has_risk=refund_order_ratio > 0.20,
        suggestion="退款订单比例偏高不一定是错误，但可能影响利润分析。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "取消订单比例偏高提醒",
        cancelled_order_ratio,
        risk_level="LOW",
        has_risk=cancelled_order_ratio > 0.20,
        suggestion="取消订单比例偏高不一定是错误，但可能影响订单转化分析。"
    )

    # 十一、仓库表检查
    section = "仓库表检查"
    add_section(full_report_lines, f"十一、{section}")

    fact_orders_count = safe_count(fact_orders_df)
    dim_customers_count = safe_count(dim_customers_df)
    dim_products_count = safe_count(dim_products_df)
    dim_regions_count = safe_count(dim_regions_df)
    dim_date_count = safe_count(dim_date_df)

    add_full_check(full_report_lines, "fact_orders数量", fact_orders_count)
    add_full_check(full_report_lines, "dim_customers数量", dim_customers_count)
    add_full_check(full_report_lines, "dim_products数量", dim_products_count)
    add_full_check(full_report_lines, "dim_regions数量", dim_regions_count)
    add_full_check(full_report_lines, "dim_date_count数量", dim_date_count)

    fact_date_missing_count = count_missing_values(fact_orders_df, "date_id")

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "fact_orders.date_id缺失数量",
        fact_date_missing_count,
        risk_level="HIGH",
        has_risk=(
            fact_date_missing_count == "字段不存在"
            or is_positive_number(fact_date_missing_count)
        ),
        suggestion="事实表应包含date_id，用于关联日期维度。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "dim_customers 与 clean_customers 数量一致性",
        value=f"clean_customers={clean_customers_count}, dim_customers={dim_customers_count}",
        risk_level="MEDIUM",
        has_risk=dim_customers_count != clean_customers_count,
        suggestion="客户维度表应来自清洗后的客户主数据，正常情况下数量应与 clean_customers 一致。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "dim_products 与 clean_products 数量一致性",
        value=f"clean_products={clean_products_count}, dim_products={dim_products_count}",
        risk_level="MEDIUM",
        has_risk=dim_products_count != clean_products_count,
        suggestion="商品维度表应来自清洗后的商品主数据，正常情况下数量应与 clean_products 一致。"
    )

    risk_count += add_quality_check(
        full_report_lines,
        summary_lines,
        issue_rows,
        section,
        "dim_regions 与 clean_regions 数量一致性",
        value=f"clean_regions={clean_regions_count}, dim_regions={dim_regions_count}",
        risk_level="MEDIUM",
        has_risk=dim_regions_count != clean_regions_count,
        suggestion="地区维度表应来自清洗后的地区主数据，正常情况下数量应与 clean_regions 一致。"
    )

    # 十二、报告生成结果
    add_section(full_report_lines, "十二、报告生成结果")

    add_full_check(full_report_lines, "风险项总数", risk_count)
    add_full_check(full_report_lines, "风险摘要报告路径", summary_output_file)
    add_full_check(full_report_lines, "完整质量检查报告路径", full_output_file)
    add_full_check(full_report_lines, "风险明细CSV路径", issues_output_file)

    if risk_count == 0:
        summary_lines.append("[INFO] 未发现需要重点关注的数据质量风险。")
    else:
        summary_lines.append("")
        summary_lines.append(f"风险项总数：{risk_count}")

    write_report_to_txt(summary_lines, summary_output_file)
    write_report_to_txt(full_report_lines, full_output_file)
    write_issues_to_csv(issue_rows, issues_output_file)

    return{
        "summary_report": summary_output_file,
        "full_report": full_output_file,
        "issues_file": issues_output_file,
        "risk_count": risk_count
    }
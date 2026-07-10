import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(BASE_DIR)
from config import (
    ORDERS_FILE, 
    CUSTOMERS_FILE, 
    PRODUCTS_FILE, 
    REGIONS_FILE, 

    CLEAN_ORDERS_DIR,
    CLEAN_ORDERS_FILE,
    ENRICHED_ORDERS_DIR,
    ENRICHED_ORDERS_FILE,

    FACT_ORDERS_DIR,
    FACT_ORDERS_FILE,
    DIM_CUSTOMERS_DIR,
    DIM_CUSTOMERS_FILE,
    DIM_PRODUCTS_DIR,
    DIM_PRODUCTS_FILE,
    DIM_REGIONS_DIR,
    DIM_REGIONS_FILE,
    DIM_DATE_DIR,
    DIM_DATE_FILE,

    SALES_BY_PRODUCT_DIR,
    SALES_BY_PRODUCT_FILE,
    SALES_BY_CATEGORY_DIR,
    SALES_BY_CATEGORY_FILE,
    SALES_BY_REGION_DIR,
    SALES_BY_REGION_FILE,

    MONTHLY_SALES_DIR,
    MONTHLY_SALES_FILE,
    YEARLY_SALES_DIR,
    YEARLY_SALES_FILE,

    DISCOUNT_LEVEL_ANALYSIS_DIR,
    DISCOUNT_LEVEL_ANALYSIS_FILE,
    TOP_CUSTOMERS_DIR,
    TOP_CUSTOMERS_FILE,

    PAYMENT_METHOD_ANALYSIS_DIR,
    PAYMENT_METHOD_ANALYSIS_FILE,
    ORDER_STATUS_ANALYSIS_DIR,
    ORDER_STATUS_ANALYSIS_FILE,

    SALES_BY_REPURCHASE_LEVEL_DIR,
    SALES_BY_REPURCHASE_LEVEL_FILE,
    DYNAMIC_TOP_CUSTOMERS_DIR,
    DYNAMIC_TOP_CUSTOMERS_FILE,

    SALES_BY_PRODUCT_TYPE_DIR,
    SALES_BY_PRODUCT_TYPE_FILE,
    NON_CASH_COST_ANALYSIS_DIR,
    NON_CASH_COST_ANALYSIS_FILE
)
from extract import create_spark_session, load_raw_data
from transform import (
    clean_orders,
    clean_customers,
    clean_products,
    clean_regions,
    build_enriched_orders,
    build_fact_orders,
    build_dim_customers,
    build_dim_products,
    build_dim_regions,
    build_dim_date
)
from quality_checks import generate_data_quality_reports
from analysis import (
    build_analysis_base_table,
    analyze_sales_by_product,
    analyze_sales_by_category,
    analyze_sales_by_region,
    analyze_monthly_sales,
    analyze_yearly_sales,
    analyze_discount_level,
    analyze_top_customers,
    analyze_payment_method,
    analyze_order_status,
    analyze_sales_by_repurchase_level,
    analyze_sales_by_product_type,
    analyze_non_cash_cost,
    analyze_top_customers_dynamic
)
from load import save_multiple_dataframes
from column_maps import COMMON_COLUMN_NAME_MAP

def check_raw_files_exist():
    raw_files = [
        ORDERS_FILE, 
        CUSTOMERS_FILE, 
        PRODUCTS_FILE, 
        REGIONS_FILE
    ]

    missing_files = []

    for file in raw_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print("以下原始数据文件不存在：")
        for file_path in missing_files:
            print(f"- {file_path}")

        raise FileNotFoundError(
            "原始数据文件缺失。"
        )
    
def print_step(title):
    """
    打印流程阶段标题。
    """

    print()
    print("=" * 100)
    print(title)
    print("=" * 100)

def main():
    spark = None

    clean_orders_df = None
    clean_customers_df = None
    clean_products_df = None
    clean_regions_df = None
    enriched_orders_df = None
    fact_orders_df = None
    dim_customers_df = None
    dim_products_df = None
    dim_regions_df = None
    dim_date_df = None
    analysis_df = None

    try:
        print_step("1. 检查原始数据文件")
        check_raw_files_exist()
        print("所有原始数据文件均存在，继续执行数据管道。")

        print_step("2. 创建SparkSession")
        spark = create_spark_session("电子商务数据管道处理总流程")
        print("SparkSession创建成功。")

        print_step("3. 加载原始数据")
        orders_df, customers_df, products_df, regions_df = load_raw_data(spark)

        print("原始orders表结构：")
        orders_df.printSchema()
        print("原始customers表结构：")
        customers_df.printSchema()
        print("原始products表结构：")
        products_df.printSchema()
        print("原始regions表结构：")
        regions_df.printSchema()

        print_step("4. 数据清洗")
        clean_orders_df = clean_orders(orders_df).cache()
        clean_customers_df = clean_customers(customers_df).cache()
        clean_products_df = clean_products(products_df).cache()
        clean_regions_df = clean_regions(regions_df).cache()

        raw_orders_count = orders_df.count()
        raw_customers_count = customers_df.count()
        raw_products_count = products_df.count()
        raw_regions_count = regions_df.count()

        clean_orders_count = clean_orders_df.count()
        clean_customers_count = clean_customers_df.count()
        clean_products_count = clean_products_df.count()
        clean_regions_count = clean_regions_df.count()

        print("【原始数据数量】")
        print(f"原始订单数量： {raw_orders_count}")
        print(f"原始客户数量： {raw_customers_count}")
        print(f"原始商品数量： {raw_products_count}")
        print(f"原始地区数量： {raw_regions_count}")

        print("=" * 100)

        print("【清洗后数据数量】")
        print(f"清洗后订单数量： {clean_orders_count}")
        print(f"清洗后客户数量： {clean_customers_count}")
        print(f"清洗后商品数量： {clean_products_count}")
        print(f"清洗后地区数量： {clean_regions_count}")

        print("=" * 100)

        print("【清洗过滤数量】")
        print(f"订单清洗过滤数量： {raw_orders_count - clean_orders_count}")
        print(f"客户清洗过滤数量： {raw_customers_count - clean_customers_count}")
        print(f"商品清洗过滤数量： {raw_products_count - clean_products_count}")
        print(f"地区清洗过滤数量： {raw_regions_count - clean_regions_count}")

        print_step("5. 构建订单宽表")
        enriched_orders_df = build_enriched_orders(
            clean_orders_df=clean_orders_df,
            clean_customers_df=clean_customers_df,
            clean_products_df=clean_products_df,
            clean_regions_df=clean_regions_df
        ).cache()

        enriched_orders_count = enriched_orders_df.count()

        print("订单宽表结构：")
        enriched_orders_df.printSchema()
        print(f"订单宽表记录数量： {enriched_orders_count}")

        print_step("6. 构建数据仓库事实表与维度表")
        fact_orders_df = build_fact_orders(enriched_orders_df).cache()
        dim_customers_df = build_dim_customers(clean_customers_df).cache()
        dim_products_df = build_dim_products(clean_products_df).cache()
        dim_regions_df = build_dim_regions(clean_regions_df).cache()
        dim_date_df = build_dim_date(enriched_orders_df).cache()

        fact_orders_count = fact_orders_df.count()
        dim_customers_count = dim_customers_df.count()
        dim_products_count = dim_products_df.count()
        dim_regions_count = dim_regions_df.count()
        dim_date_count = dim_date_df.count()

        print("事实表fact_orders结构：")
        fact_orders_df.printSchema()
        print(f"事实表fact_orders记录数量： {fact_orders_count}")

        print("维度表dim_customers结构：")
        dim_customers_df.printSchema()
        print(f"维度表dim_customers记录数量： {dim_customers_count}")

        print("维度表dim_products结构：")
        dim_products_df.printSchema()
        print(f"维度表dim_products记录数量： {dim_products_count}")

        print("维度表dim_regions结构：")
        dim_regions_df.printSchema()
        print(f"维度表dim_regions记录数量： {dim_regions_count}")

        print("维度表dim_date结构：")
        dim_date_df.printSchema()
        print(f"维度表dim_date记录数量： {dim_date_count}")

        print_step("7. 保存清洗后数据、订单宽表和数据仓库表")
        save_multiple_dataframes([
            {
                "name": "clean_orders",
                "df": clean_orders_df,
                "output_dir": CLEAN_ORDERS_DIR,
                "output_file": CLEAN_ORDERS_FILE
            },
            {
                "name": "enriched_orders",
                "df": enriched_orders_df,
                "output_dir": ENRICHED_ORDERS_DIR,
                "output_file": ENRICHED_ORDERS_FILE
            },
            {
                "name": "fact_orders",
                "df": fact_orders_df,
                "output_dir": FACT_ORDERS_DIR,
                "output_file": FACT_ORDERS_FILE
            },
            {
                "name": "dim_customers",
                "df": dim_customers_df,
                "output_dir": DIM_CUSTOMERS_DIR,
                "output_file": DIM_CUSTOMERS_FILE
            },
            {
                "name": "dim_products",
                "df": dim_products_df,
                "output_dir": DIM_PRODUCTS_DIR,
                "output_file": DIM_PRODUCTS_FILE
            },
            {
                "name": "dim_regions",
                "df": dim_regions_df,
                "output_dir": DIM_REGIONS_DIR,
                "output_file": DIM_REGIONS_FILE
            },
            {
                "name": "dim_date",
                "df": dim_date_df,
                "output_dir": DIM_DATE_DIR,
                "output_file": DIM_DATE_FILE
            }
        ])

        print_step("8. 生成数据质量报告")
        quality_report_result = generate_data_quality_reports(
            raw_orders_df=orders_df,
            raw_customers_df=customers_df,
            raw_products_df=products_df,
            raw_regions_df=regions_df,
            clean_orders_df=clean_orders_df,
            clean_customers_df=clean_customers_df,
            clean_products_df=clean_products_df,
            clean_regions_df=clean_regions_df,
            enriched_orders_df=enriched_orders_df,
            fact_orders_df=fact_orders_df,
            dim_customers_df=dim_customers_df,
            dim_products_df=dim_products_df,
            dim_regions_df=dim_regions_df,
            dim_date_df=dim_date_df
        )

        print("数据质量报告生成结果：")
        print(f"风险摘要： {quality_report_result['summary_report']}")
        print(f"完整报告： {quality_report_result['full_report']}")
        print(f"风险明细： {quality_report_result['issues_file']}")
        print(f"风险项数量： {quality_report_result['risk_count']}")

        print_step("9. 构建销售分析基础表")
        analysis_df = build_analysis_base_table(
            fact_orders_df=fact_orders_df,
            dim_customers_df=dim_customers_df,
            dim_products_df=dim_products_df,
            dim_regions_df=dim_regions_df,
            dim_date_df=dim_date_df
        ).cache()

        print("销售分析基础表结构：")
        analysis_df.printSchema()
        print(f"销售分析基础表记录数量： {analysis_df.count()}")

        print_step("10. 执行销售分析")
        sales_by_product_df = analyze_sales_by_product(analysis_df)
        sales_by_category_df = analyze_sales_by_category(analysis_df)
        sales_by_region_df = analyze_sales_by_region(analysis_df)
        monthly_sales_df = analyze_monthly_sales(analysis_df)
        yearly_sales_df = analyze_yearly_sales(analysis_df)
        discount_level_analysis_df = analyze_discount_level(analysis_df)
        top_customers_df = analyze_top_customers(analysis_df, top_n=100)
        payment_method_analysis_df = analyze_payment_method(analysis_df)
        order_status_analysis_df = analyze_order_status(analysis_df)
        sales_by_repurchase_level_df = analyze_sales_by_repurchase_level(analysis_df)
        sales_by_product_type_df = analyze_sales_by_product_type(analysis_df)
        non_cash_cost_analysis_df = analyze_non_cash_cost(analysis_df)

        dynamic_top_customers_df = analyze_top_customers_dynamic(
            analysis_df=analysis_df,
            city="上海市", 
            top_n=100,
            sort_by="total_sales_amount"
        )

        print("销售分析任务执行完成。")

        print_step("11. 保存销售分析结果")
        save_multiple_dataframes([
            {
                "name": "sales_by_product",
                "df": sales_by_product_df,
                "output_dir": SALES_BY_PRODUCT_DIR,
                "output_file": SALES_BY_PRODUCT_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "sales_by_category",
                "df": sales_by_category_df,
                "output_dir": SALES_BY_CATEGORY_DIR,
                "output_file": SALES_BY_CATEGORY_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "sales_by_region",
                "df": sales_by_region_df,
                "output_dir": SALES_BY_REGION_DIR,
                "output_file": SALES_BY_REGION_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "monthly_sales",
                "df": monthly_sales_df,
                "output_dir": MONTHLY_SALES_DIR,
                "output_file": MONTHLY_SALES_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "yearly_sales",
                "df": yearly_sales_df,
                "output_dir": YEARLY_SALES_DIR,
                "output_file": YEARLY_SALES_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "discount_level_analysis",
                "df": discount_level_analysis_df,
                "output_dir": DISCOUNT_LEVEL_ANALYSIS_DIR,
                "output_file": DISCOUNT_LEVEL_ANALYSIS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "top_customers",
                "df": top_customers_df,
                "output_dir": TOP_CUSTOMERS_DIR,
                "output_file": TOP_CUSTOMERS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "payment_method_analysis",
                "df": payment_method_analysis_df,
                "output_dir": PAYMENT_METHOD_ANALYSIS_DIR,
                "output_file": PAYMENT_METHOD_ANALYSIS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "order_status_analysis",
                "df": order_status_analysis_df,
                "output_dir": ORDER_STATUS_ANALYSIS_DIR,
                "output_file": ORDER_STATUS_ANALYSIS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "sales_by_repurchase_level",
                "df": sales_by_repurchase_level_df,
                "output_dir": SALES_BY_REPURCHASE_LEVEL_DIR,
                "output_file": SALES_BY_REPURCHASE_LEVEL_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "sales_by_product_type",
                "df": sales_by_product_type_df,
                "output_dir": SALES_BY_PRODUCT_TYPE_DIR,
                "output_file": SALES_BY_PRODUCT_TYPE_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "non_cash_cost_analysis",
                "df": non_cash_cost_analysis_df,
                "output_dir": NON_CASH_COST_ANALYSIS_DIR,
                "output_file": NON_CASH_COST_ANALYSIS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            },
            {
                "name": "dynamic_top_customers",
                "df": dynamic_top_customers_df,
                "output_dir": DYNAMIC_TOP_CUSTOMERS_DIR,
                "output_file": DYNAMIC_TOP_CUSTOMERS_FILE,
                "column_name_map": COMMON_COLUMN_NAME_MAP
            }
        ])

        print_step("数据管道执行完成")
        print("所有数据处理、质量检查、仓库建模和销售分析结果已生成。")
        print("主要输出文件：")
        print(f"清洗订单：{CLEAN_ORDERS_FILE}")
        print(f"订单宽表：{ENRICHED_ORDERS_FILE}")
        print(f"事实表：{FACT_ORDERS_FILE}")
        print(f"商品分析：{SALES_BY_PRODUCT_FILE}")
        print(f"质量风险摘要：{quality_report_result['summary_report']}")

    except Exception as error:
        print_step("数据管道执行失败")
        print(f"错误信息： {str(error)}")
        raise

    finally:
        for cached_df in [
            analysis_df,
            fact_orders_df,
            dim_customers_df,
            dim_products_df,
            dim_regions_df,
            dim_date_df,
            enriched_orders_df,
            clean_orders_df,
            clean_customers_df,
            clean_products_df,
            clean_regions_df
        ]:
            if cached_df is not None:
                cached_df.unpersist()

        if spark is not None:
            spark.stop()
            print("SparkSession已停止。")

if __name__ == "__main__":
    main()



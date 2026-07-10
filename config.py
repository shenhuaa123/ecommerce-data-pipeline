import os

# 定义项目目录结构和文件路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
WAREHOUSE_DIR = os.path.join(DATA_DIR, "warehouse")
OUTPUT_DATA_DIR = os.path.join(DATA_DIR, "output")

# 原始数据文件路径
ORDERS_FILE = os.path.join(RAW_DATA_DIR, "orders.csv")
CUSTOMERS_FILE = os.path.join(RAW_DATA_DIR, "customers.csv")
PRODUCTS_FILE = os.path.join(RAW_DATA_DIR, "products.csv")
REGIONS_FILE = os.path.join(RAW_DATA_DIR, "regions.csv")

# 处理后数据文件路径
CLEAN_ORDERS_DIR = os.path.join(PROCESSED_DATA_DIR, "clean_orders")
CLEAN_ORDERS_FILE = os.path.join(PROCESSED_DATA_DIR, "clean_orders.csv")

ENRICHED_ORDERS_DIR = os.path.join(PROCESSED_DATA_DIR, "enriched_orders")
ENRICHED_ORDERS_FILE = os.path.join(PROCESSED_DATA_DIR, "enriched_orders.csv")

# 仓库表文件路径
FACT_ORDERS_DIR = os.path.join(WAREHOUSE_DIR, "fact_orders")
FACT_ORDERS_FILE = os.path.join(WAREHOUSE_DIR, "fact_orders.csv")

DIM_CUSTOMERS_DIR = os.path.join(WAREHOUSE_DIR, "dim_customers")
DIM_CUSTOMERS_FILE = os.path.join(WAREHOUSE_DIR, "dim_customers.csv")

DIM_PRODUCTS_DIR = os.path.join(WAREHOUSE_DIR, "dim_products")
DIM_PRODUCTS_FILE = os.path.join(WAREHOUSE_DIR, "dim_products.csv")

DIM_REGIONS_DIR = os.path.join(WAREHOUSE_DIR, "dim_regions")
DIM_REGIONS_FILE = os.path.join(WAREHOUSE_DIR, "dim_regions.csv")

DIM_DATE_DIR = os.path.join(WAREHOUSE_DIR, "dim_date")
DIM_DATE_FILE = os.path.join(WAREHOUSE_DIR, "dim_date.csv")

# 分析结果文件路径
SALES_BY_PRODUCT_DIR = os.path.join(OUTPUT_DATA_DIR, "sales_by_product")
SALES_BY_PRODUCT_FILE = os.path.join(OUTPUT_DATA_DIR, "sales_by_product.csv")

SALES_BY_CATEGORY_DIR = os.path.join(OUTPUT_DATA_DIR, "sales_by_category")
SALES_BY_CATEGORY_FILE = os.path.join(OUTPUT_DATA_DIR, "sales_by_category.csv")

SALES_BY_REGION_DIR = os.path.join(OUTPUT_DATA_DIR, "sales_by_region")
SALES_BY_REGION_FILE = os.path.join(OUTPUT_DATA_DIR, "sales_by_region.csv")

MONTHLY_SALES_DIR = os.path.join(OUTPUT_DATA_DIR, "monthly_sales")
MONTHLY_SALES_FILE = os.path.join(OUTPUT_DATA_DIR, "monthly_sales.csv")

YEARLY_SALES_DIR = os.path.join(OUTPUT_DATA_DIR, "yearly_sales")
YEARLY_SALES_FILE = os.path.join(OUTPUT_DATA_DIR, "yearly_sales.csv")

DISCOUNT_LEVEL_ANALYSIS_DIR = os.path.join(OUTPUT_DATA_DIR, "discount_level_analysis")
DISCOUNT_LEVEL_ANALYSIS_FILE = os.path.join(OUTPUT_DATA_DIR, "discount_level_analysis.csv")

TOP_CUSTOMERS_DIR = os.path.join(OUTPUT_DATA_DIR, "top_customers")
TOP_CUSTOMERS_FILE = os.path.join(OUTPUT_DATA_DIR, "top_customers.csv")

PAYMENT_METHOD_ANALYSIS_DIR = os.path.join(OUTPUT_DATA_DIR, "payment_method_analysis")
PAYMENT_METHOD_ANALYSIS_FILE = os.path.join(OUTPUT_DATA_DIR, "payment_method_analysis.csv")

ORDER_STATUS_ANALYSIS_DIR = os.path.join(OUTPUT_DATA_DIR, "order_status_analysis")
ORDER_STATUS_ANALYSIS_FILE = os.path.join(OUTPUT_DATA_DIR, "order_status_analysis.csv")

SALES_BY_REPURCHASE_LEVEL_DIR = os.path.join(OUTPUT_DATA_DIR, "sales_by_repurchase_level")
SALES_BY_REPURCHASE_LEVEL_FILE = os.path.join(OUTPUT_DATA_DIR, "sales_by_repurchase_level.csv")

DYNAMIC_TOP_CUSTOMERS_DIR = os.path.join(OUTPUT_DATA_DIR, "dynamic_top_customers")
DYNAMIC_TOP_CUSTOMERS_FILE = os.path.join(OUTPUT_DATA_DIR, "dynamic_top_customers.csv")

SALES_BY_PRODUCT_TYPE_DIR = os.path.join(OUTPUT_DATA_DIR, "sales_by_product_type")
SALES_BY_PRODUCT_TYPE_FILE = os.path.join(OUTPUT_DATA_DIR, "sales_by_product_type.csv")

NON_CASH_COST_ANALYSIS_DIR = os.path.join(OUTPUT_DATA_DIR, "non_cash_cost_analysis")
NON_CASH_COST_ANALYSIS_FILE = os.path.join(OUTPUT_DATA_DIR, "non_cash_cost_analysis.csv")

# 质量报告输出路径
QUALITY_SUMMARY_REPORT_FILE = os.path.join(OUTPUT_DATA_DIR, "data_quality_summary.txt")
QUALITY_FULL_REPORT_FILE = os.path.join(OUTPUT_DATA_DIR, "data_quality_full_report.txt")
QUALITY_ISSUES_FILE = os.path.join(OUTPUT_DATA_DIR, "data_quality_issues.csv")

##### 数据规模参数 #####
TOTAL_CUSTOMERS = 30000
TOTAL_ORDERS = 300000

VALID_PAYMENT_METHODS = ["信用卡", "借记卡", "支付宝", "微信支付", "现金"]
VALID_ORDER_STATUSES = ["已完成", "已取消", "已退款"]

MIN_DISCOUNT = 0
MAX_DISCOUNT = 1

# 每个地区的最少客户数量，确保每个地区都有足够的数据进行分析
MIN_CUSTOMERS_PER_REGION = 120

# 重点地区的客户数量下限，确保这些地区有足够的数据进行深入分析
KEY_REGION_CUSTOMER_MINIMUMS = {
    1: 1600,   # 深圳市
    2: 1500,   # 广州市
    3: 2200,   # 上海市
    4: 2000,   # 北京市
    5: 1400,   # 杭州市
    6: 1300,   # 成都市
    7: 1300,   # 南京市
    11: 1500,  # 重庆市
    21: 1200,  # 悉尼
    22: 1200,  # 墨尔本
}

# 硬性数据范围
AGE_HARD_MIN = 0
AGE_HARD_MAX = 150

# 合理数据范围
NORMAL_AGE_MIN = 18
NORMAL_AGE_MAX = 80


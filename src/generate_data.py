import csv
import os
import random
import sys
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(BASE_DIR)
from config import (
    RAW_DATA_DIR, 
    ORDERS_FILE, 
    CUSTOMERS_FILE, 
    PRODUCTS_FILE, 
    REGIONS_FILE,
    TOTAL_CUSTOMERS,
    TOTAL_ORDERS,
    VALID_PAYMENT_METHODS,
    VALID_ORDER_STATUSES,
    MIN_CUSTOMERS_PER_REGION,
    KEY_REGION_CUSTOMER_MINIMUMS,
    AGE_HARD_MIN,
    AGE_HARD_MAX
)

REGIONS = [
        [1, "中国", "广东省", "深圳市"],
        [2, "中国", "广东省", "广州市"],
        [3, "中国", "上海市", "上海市"],
        [4, "中国", "北京市", "北京市"],
        [5, "中国", "浙江省", "杭州市"],
        [6, "中国", "四川省", "成都市"],
        [7, "中国", "江苏省", "南京市"],
        [8, "中国", "湖北省", "武汉市"],
        [9, "中国", "陕西省", "西安市"],
        [10, "中国", "福建省", "厦门市"],
        [11, "中国", "重庆市", "重庆市"],
        [12, "中国", "天津市", "天津市"],
        [13, "中国", "山东省", "青岛市"],
        [14, "中国", "河南省", "郑州市"],
        [15, "中国", "湖南省", "长沙市"],
        [16, "中国", "辽宁省", "大连市"],
        [17, "中国", "云南省", "昆明市"],
        [18, "中国", "安徽省", "合肥市"],
        [19, "中国", "江西省", "南昌市"],
        [20, "中国", "广西壮族自治区", "南宁市"],
        [21, "澳大利亚", "新南威尔士州", "悉尼"],
        [22, "澳大利亚", "维多利亚州", "墨尔本"],
        [23, "澳大利亚", "昆士兰州", "布里斯班"],
        [24, "澳大利亚", "西澳大利亚州", "珀斯"],
        [25, "澳大利亚", "南澳大利亚州", "阿德莱德"],
        [26, "澳大利亚", "首都领地", "堪培拉"],
        [27, "澳大利亚", "塔斯马尼亚州", "霍巴特"],
        [28, "澳大利亚", "北领地", "达尔文"],
        [29, "新西兰", "奥克兰大区", "奥克兰"],
        [30, "新西兰", "惠灵顿大区", "惠灵顿"],
    ]

PRODUCTS = [
    [101, "无线鼠标", "电子产品", "罗技", 50.00, 99.00, "低", "普通商品"],
    [102, "机械键盘", "电子产品", "京东京造", 198.00, 399.00, "低", "普通商品"],
    [103, "蓝牙音箱", "电子产品", "漫步者", 124.00, 249.00, "低", "普通商品"],
    [104, "移动硬盘", "电子产品", "西部数据", 318.00, 599.00, "低", "普通商品"],
    [105, "显示器", "电子产品", "戴尔", 770.00, 1599.00, "低", "普通商品"],
    [106, "无线耳机", "电子产品", "索尼", 350.00, 699.00, "低", "普通商品"],
    [107, "路由器", "电子产品", "华为", 132.00, 249.00, "低", "普通商品"],
    [108, "打印机", "电子产品", "惠普", 548.00, 1099.00, "低", "普通商品"],
    [109, "摄像头", "电子产品", "小米", 86.00, 159.00, "低", "普通商品"],
    [110, "平板电脑", "电子产品", "联想", 1320.00, 2699.00, "低", "普通商品"],

    [111, "数据线", "数码配件", "安克", 11.80, 15.90, "高", "普通商品"],
    [112, "笔记本支架", "数码配件", "绿联", 27.50, 39.90, "中", "普通商品"],
    [113, "手机壳", "数码配件", "倍思", 7.20, 9.90, "高", "普通商品"],
    [114, "充电宝", "数码配件", "小米", 63.00, 99.00, "中", "普通商品"],
    [115, "无线充电器", "数码配件", "华为", 45.00, 69.00, "中", "普通商品"],
    [116, "键盘膜", "数码配件", "倍思", 4.40, 5.90, "高", "普通商品"],
    [117, "电脑清洁套装", "数码配件", "绿联", 8.80, 12.90, "高", "普通商品"],
    [118, "扩展坞", "数码配件", "贝尔金", 104.00, 199.00, "低", "普通商品"],
    [119, "耳机收纳盒", "数码配件", "品胜", 6.60, 8.90, "高", "普通商品"],
    [120, "钢化膜", "数码配件", "闪魔", 4.30, 5.90, "高", "普通商品"],

    [121, "办公椅", "家具", "宜家", 248.00, 529.00, "低", "普通商品"],
    [122, "台灯", "家具", "飞利浦", 42.00, 69.00, "中", "普通商品"],
    [123, "书桌", "家具", "全友", 405.00, 899.00, "低", "普通商品"],
    [124, "护眼灯", "家具", "孩视宝", 96.00, 199.00, "低", "普通商品"],
    [125, "床头柜", "家具", "林氏木业", 148.00, 329.00, "低", "普通商品"],
    [126, "衣帽架", "家具", "原始原素", 56.00, 99.00, "低", "普通商品"],
    [127, "电脑桌", "家具", "乐歌", 268.00, 599.00, "低", "普通商品"],
    [128, "置物架", "家具", "溢彩年华", 36.00, 59.00, "中", "普通商品"],
    [129, "折叠椅", "家具", "宜家", 36.00, 59.00, "低", "普通商品"],
    [130, "书架", "家具", "全友", 210.00, 459.00, "低", "普通商品"],

    [131, "保温杯", "生活用品", "膳魔师", 45.00, 79.00, "中", "普通商品"],
    [132, "双肩包", "生活用品", "李宁", 88.00, 169.00, "低", "普通商品"],
    [133, "收纳箱", "生活用品", "禧天龙", 21.00, 29.90, "中", "普通商品"],
    [134, "马克杯", "生活用品", "乐扣乐扣", 11.20, 15.90, "中", "普通商品"],
    [135, "雨伞", "生活用品", "天堂伞", 25.00, 35.90, "中", "普通商品"],
    [136, "拖鞋", "生活用品", "回力", 18.50, 25.90, "中", "普通商品"],
    [137, "毛巾", "生活用品", "洁丽雅", 9.20, 12.90, "高", "普通商品"],
    [138, "垃圾桶", "生活用品", "茶花", 18.20, 25.90, "中", "普通商品"],
    [139, "衣架", "生活用品", "茶花", 7.00, 9.90, "高", "普通商品"],
    [140, "靠枕", "生活用品", "南极人", 26.00, 39.90, "中", "普通商品"],

    [141, "笔记本", "文具", "晨光", 5.00, 6.90, "高", "普通商品"],
    [142, "中性笔", "文具", "得力", 1.55, 2.00, "高", "普通商品"],
    [143, "文件夹", "文具", "齐心", 3.35, 4.90, "高", "普通商品"],
    [144, "订书机", "文具", "得力", 10.80, 16.90, "中", "普通商品"],
    [145, "便签纸", "文具", "晨光", 2.05, 2.90, "高", "普通商品"],
    [146, "修正带", "文具", "晨光", 3.45, 4.90, "高", "普通商品"],
    [147, "荧光笔", "文具", "斑马", 4.10, 5.90, "高", "普通商品"],
    [148, "铅笔盒", "文具", "得力", 8.10, 12.90, "中", "普通商品"],
    [149, "计算器", "文具", "卡西欧", 35.00, 59.00, "低", "普通商品"],
    [150, "白板笔", "文具", "齐心", 3.50, 4.90, "高", "普通商品"],

    [151, "碎纸机", "办公用品", "得力", 150.00, 299.00, "低", "普通商品"],
    [152, "办公剪刀", "办公用品", "齐心", 5.80, 7.90, "中", "普通商品"],
    [153, "打印纸", "办公用品", "晨鸣", 24.00, 29.90, "高", "普通商品"],
    [154, "档案盒", "办公用品", "得力", 6.40, 8.90, "高", "普通商品"],
    [155, "白板", "办公用品", "得力", 52.00, 89.00, "低", "普通商品"],
    [156, "标签纸", "办公用品", "兄弟", 12.80, 19.90, "高", "普通商品"],
    [157, "印泥", "办公用品", "得力", 5.20, 6.90, "高", "普通商品"],
    [158, "回形针", "办公用品", "齐心", 2.10, 2.90, "高", "普通商品"],
    [159, "文件袋", "办公用品", "晨光", 1.45, 2.00, "高", "普通商品"],
    [160, "鼠标垫", "办公用品", "雷蛇", 24.50, 39.90, "中", "普通商品"],

    [161, "瑜伽垫", "运动用品", "Keep", 50.00, 89.00, "低", "普通商品"],
    [162, "跳绳", "运动用品", "李宁", 18.50, 25.90, "中", "普通商品"],
    [163, "运动水壶", "运动用品", "特百惠", 31.00, 45.00, "中", "普通商品"],
    [164, "哑铃", "运动用品", "Keep", 62.00, 119.00, "低", "普通商品"],
    [165, "篮球", "运动用品", "斯伯丁", 108.00, 229.00, "低", "普通商品"],
    [166, "羽毛球拍", "运动用品", "尤尼克斯", 168.00, 359.00, "低", "普通商品"],
    [167, "跑步腰包", "运动用品", "迪卡侬", 30.00, 45.00, "中", "普通商品"],
    [168, "护腕", "运动用品", "李宁", 10.50, 14.90, "高", "普通商品"],
    [169, "运动毛巾", "运动用品", "Keep", 14.00, 19.90, "高", "普通商品"],
    [170, "筋膜球", "运动用品", "Keep", 15.80, 22.90, "中", "普通商品"],

    [171, "洗衣液", "日化用品", "蓝月亮", 26.00, 35.90, "高", "普通商品"],
    [172, "抽纸", "日化用品", "维达", 7.50, 9.90, "高", "普通商品"],
    [173, "牙刷", "日化用品", "高露洁", 5.90, 7.90, "高", "普通商品"],
    [174, "牙膏", "日化用品", "云南白药", 13.80, 19.90, "高", "普通商品"],
    [175, "洗发水", "日化用品", "海飞丝", 32.00, 45.00, "高", "普通商品"],
    [176, "沐浴露", "日化用品", "多芬", 26.00, 35.90, "高", "普通商品"],
    [177, "洗手液", "日化用品", "滴露", 13.50, 19.90, "高", "普通商品"],
    [178, "洁厕剂", "日化用品", "威猛先生", 10.50, 14.90, "高", "普通商品"],
    [179, "湿巾", "日化用品", "心相印", 6.70, 8.90, "高", "普通商品"],
    [180, "洗洁精", "日化用品", "立白", 8.90, 11.90, "高", "普通商品"],

    [181, "矿泉水", "食品饮料", "农夫山泉", 1.65, 2.00, "高", "普通商品"],
    [182, "速溶咖啡", "食品饮料", "雀巢", 26.00, 35.90, "高", "普通商品"],
    [183, "牛奶", "食品饮料", "蒙牛", 12.20, 14.90, "高", "普通商品"],
    [184, "饼干", "食品饮料", "奥利奥", 6.70, 8.90, "高", "普通商品"],
    [185, "方便面", "食品饮料", "康师傅", 3.65, 4.50, "高", "普通商品"],
    [186, "薯片", "食品饮料", "乐事", 6.60, 8.90, "高", "普通商品"],
    [187, "茶叶", "食品饮料", "八马", 58.00, 99.00, "中", "普通商品"],
    [188, "坚果礼盒", "食品饮料", "三只松鼠", 62.00, 99.00, "中", "普通商品"],
    [189, "火腿肠", "食品饮料", "双汇", 3.95, 4.90, "高", "普通商品"],
    [190, "酸奶", "食品饮料", "伊利", 9.70, 11.90, "高", "普通商品"],

    [191, "T恤", "服饰鞋包", "优衣库", 45.00, 99.00, "中", "普通商品"],
    [192, "牛仔裤", "服饰鞋包", "森马", 115.00, 249.00, "低", "普通商品"],
    [193, "运动鞋", "服饰鞋包", "安踏", 188.00, 399.00, "低", "普通商品"],
    [194, "棒球帽", "服饰鞋包", "李宁", 36.00, 79.00, "中", "普通商品"],
    [195, "袜子", "服饰鞋包", "南极人", 7.40, 9.90, "高", "普通商品"],
    [196, "皮带", "服饰鞋包", "七匹狼", 62.00, 129.00, "低", "普通商品"],
    [197, "斜挎包", "服饰鞋包", "小米", 48.00, 99.00, "中", "普通商品"],
    [198, "围巾", "服饰鞋包", "恒源祥", 44.00, 89.00, "中", "普通商品"],
    [199, "衬衫", "服饰鞋包", "海澜之家", 92.00, 199.00, "中", "普通商品"],
    [200, "运动裤", "服饰鞋包", "安踏", 96.00, 199.00, "中", "普通商品"],

    [201, "电水壶", "家用电器", "美的", 58.00, 119.00, "低", "普通商品"],
    [202, "电饭煲", "家用电器", "苏泊尔", 178.00, 399.00, "低", "普通商品"],
    [203, "空气炸锅", "家用电器", "九阳", 222.00, 499.00, "低", "普通商品"],
    [204, "吹风机", "家用电器", "飞科", 57.00, 119.00, "低", "普通商品"],
    [205, "电风扇", "家用电器", "格力", 112.00, 249.00, "低", "普通商品"],
    [206, "加湿器", "家用电器", "小熊", 72.00, 139.00, "低", "普通商品"],
    [207, "吸尘器", "家用电器", "美的", 310.00, 649.00, "低", "普通商品"],
    [208, "电磁炉", "家用电器", "美的", 158.00, 329.00, "低", "普通商品"],
    [209, "剃须刀", "家用电器", "飞利浦", 118.00, 249.00, "中", "普通商品"],
    [210, "挂烫机", "家用电器", "松下", 205.00, 429.00, "低", "普通商品"],

    [211, "婴儿湿巾", "母婴用品", "好奇", 14.80, 19.90, "高", "普通商品"],
    [212, "奶瓶", "母婴用品", "贝亲", 41.00, 69.00, "中", "普通商品"],
    [213, "儿童水杯", "母婴用品", "膳魔师", 49.00, 79.00, "中", "普通商品"],
    [214, "儿童牙刷", "母婴用品", "高露洁", 6.90, 9.90, "高", "普通商品"],
    [215, "儿童餐具", "母婴用品", "babycare", 36.00, 59.00, "中", "普通商品"],
    [216, "婴儿洗衣液", "母婴用品", "蓝月亮", 27.00, 39.00, "高", "普通商品"],
    [217, "儿童书包", "母婴用品", "迪士尼", 82.00, 179.00, "低", "普通商品"],
    [218, "儿童雨衣", "母婴用品", "迪士尼", 32.00, 49.90, "中", "普通商品"],
    [219, "儿童积木", "母婴用品", "乐高", 172.00, 359.00, "低", "普通商品"],
    [220, "儿童台灯", "母婴用品", "孩视宝", 105.00, 219.00, "低", "普通商品"],

    [221, "赠品贴纸", "生活用品", "平台自营", 0.50, 0.00, "低", "赠品"],
    [222, "赠品钥匙扣", "生活用品", "平台自营", 1.20, 0.00, "低", "赠品"],
    [223, "赠品帆布袋", "生活用品", "平台自营", 4.50, 0.00, "低", "赠品"],
    [224, "赠品书签", "文具", "晨光", 0.30, 0.00, "中", "赠品"],
    [225, "赠品小样湿巾", "日化用品", "心相印", 0.80, 0.00, "高", "赠品"],
    [226, "赠品数据线收纳扣", "数码配件", "绿联", 1.00, 0.00, "中", "赠品"],
    [227, "赠品运动束口袋", "运动用品", "李宁", 3.50, 0.00, "低", "赠品"],
    [228, "赠品儿童贴纸", "母婴用品", "迪士尼", 0.60, 0.00, "中", "赠品"],
    [229, "赠品咖啡试喝包", "食品饮料", "雀巢", 1.50, 0.00, "高", "赠品"],
    [230, "赠品清洁布", "数码配件", "倍思", 0.80, 0.00, "高", "赠品"],

    [231, "试用装洗手液", "日化用品", "滴露", 1.50, 0.00, "高", "试用品"],
    [232, "试用装洗发水", "日化用品", "海飞丝", 2.00, 0.00, "高", "试用品"],
    [233, "试用装沐浴露", "日化用品", "多芬", 2.00, 0.00, "高", "试用品"],
    [234, "试用装牙膏", "日化用品", "云南白药", 1.20, 0.00, "高", "试用品"],
    [235, "试吃装饼干", "食品饮料", "奥利奥", 1.00, 0.00, "高", "试用品"],
    [236, "试喝装咖啡", "食品饮料", "雀巢", 1.20, 0.00, "高", "试用品"],
    [237, "试用装婴儿湿巾", "母婴用品", "好奇", 1.00, 0.00, "高", "试用品"],
    [238, "试用装洗衣液", "日化用品", "蓝月亮", 1.80, 0.00, "高", "试用品"],
    [239, "试用装护手霜", "日化用品", "隆力奇", 1.50, 0.00, "中", "试用品"],
    [240, "试用装纸巾", "日化用品", "维达", 0.80, 0.00, "高", "试用品"],

    [241, "积分兑换笔记本", "文具", "晨光", 5.00, 0.00, "高", "积分兑换"],
    [242, "积分兑换中性笔", "文具", "得力", 1.55, 0.00, "高", "积分兑换"],
    [243, "积分兑换马克杯", "生活用品", "乐扣乐扣", 11.20, 0.00, "中", "积分兑换"],
    [244, "积分兑换毛巾", "生活用品", "洁丽雅", 9.20, 0.00, "高", "积分兑换"],
    [245, "积分兑换雨伞", "生活用品", "天堂伞", 25.00, 0.00, "中", "积分兑换"],
    [246, "积分兑换帆布袋", "生活用品", "平台自营", 4.50, 0.00, "中", "积分兑换"],
    [247, "积分兑换数据线", "数码配件", "安克", 11.80, 0.00, "高", "积分兑换"],
    [248, "积分兑换保温杯", "生活用品", "膳魔师", 45.00, 0.00, "中", "积分兑换"],
    [249, "积分兑换抽纸", "日化用品", "维达", 7.50, 0.00, "高", "积分兑换"],
    [250, "积分兑换洗手液", "日化用品", "滴露", 13.50, 0.00, "高", "积分兑换"],
]

CATEGORY_WEIGHT_MAP = {
        "食品饮料": 3.5,
        "日化用品": 3.2,
        "文具": 2.8,
        "办公用品": 2.5,
        "数码配件": 2.4,
        "生活用品": 2.2,
        "母婴用品": 2.0,
        "服饰鞋包": 1.8,
        "运动用品": 1.6,
        "家用电器": 1.1,
        "家具": 0.9,
        "电子产品": 0.8,
    }

REPURCHASE_WEIGHT_MAP = {
        "高": 2.0,
        "中": 1.2,
        "低": 0.7
    }

PRODUCT_TYPE_WEIGHT_MAP = {
        "普通商品": 1.0,
        "赠品": 0.1,
        "试用品": 0.20,
        "积分兑换": 0.15
    }

CATEGORY_QUANTITY_PROFILE  = {
        "食品饮料": {
            "高": ([1, 2, 3, 4, 5, 6, 8, 10, 12],
                  [6, 10, 14, 16, 16, 14, 10, 8, 6]),
            "中": ([1, 2, 3, 4, 5, 6],
                  [15, 22, 24, 18, 13, 8]),
            "低": ([1, 2, 3],
                  [60, 30, 10])
        },

        "日化用品": {
            "高": ([1, 2, 3, 4, 5, 6],
                  [10, 18, 22, 20, 18, 12]),
            "中": ([1, 2, 3, 4],
                  [30, 35, 25, 10]),
            "低": ([1, 2],
                  [85, 15])
        },

        "文具": {
            "高": ([1, 2, 3, 5, 10, 20],
                  [8, 12, 15, 20, 25, 20]),
            "中": ([1, 2, 3, 5, 10],
                  [20, 25, 25, 20, 10]),
            "低": ([1, 2, 3],
                  [70, 20, 10])
        },

        "办公用品": {
            "高": ([1, 2, 3, 5, 10],
                  [15, 20, 25, 25, 15]),
            "中": ([1, 2, 3, 5],
                  [35, 30, 25, 10]),
            "低": ([1, 2],
                  [85, 15])
        },

        "数码配件": {
            "高": ([1, 2, 3],
                  [55, 35, 10]),
            "中": ([1, 2],
                  [75, 25]),
            "低": ([1, 2],
                  [90, 10])
        },

        "生活用品": {
            "高": ([1, 2, 3, 4],
                  [25, 35, 25, 15]),
            "中": ([1, 2, 3],
                  [45, 40, 15]),
            "低": ([1, 2],
                  [85, 15])
        },

        "服饰鞋包": {
            "高": ([1, 2, 3, 5],
                  [20, 30, 30, 20]),
            "中": ([1, 2, 3],
                  [55, 35, 10]),
            "低": ([1, 2],
                  [88, 12])
        },

        "运动用品": {
            "高": ([1, 2, 3],
                  [65, 25, 10]),
            "中": ([1, 2],
                  [85, 15]),
            "低": ([1, 2],
                  [92, 8])
        },

        "母婴用品": {
            "高": ([1, 2, 3, 4],
                  [25, 35, 25, 15]),
            "中": ([1, 2, 3],
                  [55, 35, 10]),
            "低": ([1, 2],
                  [88, 12])
        },

        "电子产品": {
            "高": ([1, 2],
                  [82, 18]),
            "中": ([1, 2],
                  [90, 10]),
            "低": ([1, 2],
                  [96, 4])
        },

        "家具": {
            "高": ([1, 2],
                  [85, 15]),
            "中": ([1, 2],
                  [92, 8]),
            "低": ([1, 2],
                  [97, 3])
        },

        "家用电器": {
            "高": ([1, 2],
                  [85, 15]),
            "中": ([1, 2],
                  [93, 7]),
            "低": ([1, 2],
                  [97, 3])
        }
    }

DISCOUNT_PROFILE = {
    "高": {
        "低价": (
            [0, 0.02, 0.03, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20],
            [35, 8, 10, 16, 12, 10, 5, 3, 1]
        ),
        "中价": (
            [0, 0.02, 0.03, 0.05, 0.08, 0.10, 0.12],
            [45, 8, 10, 16, 10, 8, 3]
        ),
        "高价": (
            [0, 0.02, 0.03, 0.05, 0.08],
            [65, 8, 10, 12, 5]
        )
    },

    "中": {
        "低价": (
            [0, 0.02, 0.03, 0.05, 0.08, 0.10, 0.12],
            [50, 8, 10, 15, 10, 5, 2]
        ),
        "中价": (
            [0, 0.02, 0.03, 0.05, 0.08, 0.10],
            [60, 8, 10, 14, 6, 2]
        ),
        "高价": (
            [0, 0.02, 0.03, 0.05],
            [75, 8, 10, 7]
        )
    },

    "低": {
        "低价": (
            [0, 0.02, 0.03, 0.05, 0.08],
            [65, 8, 10, 12, 5]
        ),
        "中价": (
            [0, 0.02, 0.03, 0.05],
            [78, 7, 8, 7]
        ),
        "高价": (
            [0, 0.02, 0.03],
            [88, 7, 5]
        )
    }
}

PAYMENT_METHOD_WEIGHT_MAP = {
    "支付宝": 0.35,
    "微信支付": 0.40,
    "借记卡": 0.12,
    "信用卡": 0.10,
    "现金": 0.03
}

CATEGORY_DISCOUNT_BOOST_MAP = {
        "食品饮料": 1.25,
        "日化用品": 1.20,
        "文具": 1.10,
        "办公用品": 1.08,
        "母婴用品": 1.08,
        "生活用品": 1.05,
        "数码配件": 1.00,
        "服饰鞋包": 0.95,
        "运动用品": 0.90,
        "家用电器": 0.85,
        "家具": 0.80,
        "电子产品": 0.75,
    }

FAMILY_NAMES = [
        "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈",
        "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许",
        "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏",

        "陶", "姜", "戚", "谢", "邹", "喻", "柏", "水", "窦", "章",
        "云", "苏", "潘", "葛", "奚", "范", "彭", "郎", "鲁", "韦",
        "昌", "马", "苗", "凤", "花", "方", "俞", "任", "袁", "柳",

        "鲍", "史", "唐", "费", "廉", "岑", "薛", "雷", "贺", "倪",
        "汤", "滕", "殷", "罗", "毕", "郝", "邬", "安", "常", "乐",
        "于", "时", "傅", "皮", "卞", "齐", "康", "伍", "余", "元",

        "顾", "孟", "平", "黄", "和", "穆", "萧", "尹", "姚", "邵",
        "湛", "汪", "祁", "毛", "禹", "狄", "米", "贝", "明", "臧",
        "计", "伏", "成", "戴", "谈", "宋", "庞", "熊", "纪", "舒",
        "屈", "项", "祝", "董", "梁", "杜", "阮", "蓝", "闵", "席"
    ]

MALE_GIVEN_NAMES = [
    "伟", "强", "磊", "军", "勇", "杰", "涛", "明", "超", "博",
    "彬", "阳", "锋", "刚", "鹏",

    "建国", "国庆", "志强", "志远", "宏伟", "海峰", "立新", "振华", "德明", "家辉",
    "文杰", "俊杰", "明辉", "浩然", "泽宇", "天宇", "宇航", "子昂", "嘉豪", "睿哲",
    "思远", "启明", "承泽", "景行", "彦博", "亦辰", "一鸣", "柏林", "书恒", "皓轩",
    "浩宇", "明轩", "俊熙", "奕辰", "沐阳", "嘉俊", "子健", "宇轩", "辰逸", "远航"
]

FEMALE_GIVEN_NAMES = [
    "芳", "娜", "敏", "静", "艳", "娟", "丽", "慧", "雪", "佳",
    "宁", "琳", "倩", "婷", "萍",

    "晓彤", "晓琳", "雅婷", "雅雯", "静怡", "诗涵", "雨欣", "雨桐", "梦瑶", "欣怡",
    "佳琪", "佳宁", "婉婷", "若曦", "紫涵", "语嫣", "思琪", "依娜", "可欣", "雪莹",
    "嘉怡", "嘉宁", "安琪", "梓萱", "若琳", "诗雨", "语桐", "芷若", "雨菲", "雨萱",
    "梦洁", "语晴", "欣然", "婉清", "若雪", "诗雅", "心怡", "静雯", "晓雅", "雨婷"
]

NEUTRAL_GIVEN_NAMES = [
    "晨", "鑫", "宇", "然", "宁", "楠", "洋", "华", "佳", "安",

    "晨曦", "晨宇", "安然", "子涵", "子墨", "梓涵", "思源", "嘉言", "亦然", "若辰",
    "景然", "书言", "一诺", "星宇", "子宁", "明安", "云舒", "清和", "知远", "乐然"
]

MALE_NAME_CHARS = [
    "泽", "宇", "浩", "轩", "辰", "睿", "博", "远", "航", "铭",
    "杰", "峰", "凯", "霖", "昊", "然", "诚", "毅", "彦", "承",
    "景", "柏", "林", "恒", "熙", "阳", "新", "辉", "文", "俊"
]

FEMALE_NAME_CHARS = [
    "雅", "婷", "雯", "欣", "怡", "涵", "琪", "琳", "瑶", "萱",
    "晴", "雪", "菲", "洁", "婉", "清", "若", "诗", "梦", "彤",
    "妍", "悦", "宁", "佳", "雨", "桐", "静", "柔", "璇", "茜"
]

NEUTRAL_NAME_CHARS = [
    "安", "然", "晨", "宁", "宇", "星", "云", "清", "和", "乐",
    "嘉", "言", "知", "远", "子", "书", "一", "明", "思", "源"
]

def ensure_raw_data_dir():
    """确保原始数据目录存在,如果不存在则创建"""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

def random_date(start_date, end_date):
    """生成指定范围内的随机日期"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    days_between = (end - start).days
    random_days = random.randint(0, days_between)

    result_date = start + timedelta(days=random_days)

    return result_date.strftime("%Y-%m-%d")

def random_date_after_register(register_date, end_date="2026-05-19"):
    """
    根据客户注册日期生成订单日期。

    规则：
    1. 订单日期不能早于客户注册日期。
    2. 订单日期不能晚于 end_date。
    3. 如果注册日期异常晚于 end_date，则直接使用 end_date。
    """
    if isinstance(register_date, str):
        start = datetime.strptime(register_date, "%Y-%m-%d")
    else:
        start = datetime.strptime(str(register_date), "%Y-%m-%d")

    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        start = end

    days_between = (end - start).days
    random_days = random.randint(0, days_between)
    result_date = start + timedelta(days=random_days)

    return result_date.strftime("%Y-%m-%d")

def generate_regions():
    """生成地区数据，确保每个地区至少有一定数量的客户"""
    with open(REGIONS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["region_id", "country", "province", "city"])
        writer.writerows(REGIONS)

    return REGIONS

def generate_products():
    """生成商品数据"""
    with open(PRODUCTS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "product_id", 
            "product_name", 
            "category", 
            "brand", 
            "cost_price", 
            "sale_price",
            "repurchase_level",
            "product_type"
        ])
        writer.writerows(PRODUCTS)

    return PRODUCTS

def get_category_weight(category):
    """
    根据商品类别设置基础购买权重。

    权重越高，代表该类别越容易被购买。
    """
    return CATEGORY_WEIGHT_MAP.get(category, 1.0)

def get_price_weight(sale_price):
    """
    根据销售价格设置价格权重。

    便宜商品购买频率更高；
    高价商品购买频率更低。
    """
    if sale_price <= 20:
        return 3.0
    elif sale_price <= 50:
        return 2.5
    elif sale_price <= 100:
        return 2.0
    elif sale_price <= 300:
        return 1.5
    elif sale_price <= 800:
        return 1.0
    elif sale_price <= 1500:
        return 0.7
    else:
        return 0.45

def get_repurchase_weight(repurchase_level):
    """
    根据商品复购等级设置购买频率权重。
    """
    return REPURCHASE_WEIGHT_MAP.get(repurchase_level, 1.0)

def get_product_type_weight(product_type):
    """
    根据商品类型设置购买权重。
    """
    return PRODUCT_TYPE_WEIGHT_MAP.get(product_type, 0.1)

def calculate_product_weight(product):
    """
    综合商品类别、销售价格和复购等级，计算商品购买权重。
    """
    category = product[2]
    sale_price = product[5]
    repurchase_level = product[6]
    product_type = product[7]

    category_weight = get_category_weight(category)
    price_weight = get_price_weight(sale_price)
    repurchase_weight = get_repurchase_weight(repurchase_level)
    product_type_weight = get_product_type_weight(product_type)

    product_weight = category_weight * price_weight * repurchase_weight * product_type_weight

    return product_weight

def build_product_sampling_data(products):
    """
    构建商品随机选择所需的数据。

    返回：
    1. product_ids：商品ID列表
    2. product_weights：每个商品对应的购买权重
    3. product_price_dict：商品ID到销售价格的映射
    4. product_category_dict：商品ID到商品类别的映射
    5. product_repurchase_level_dict：商品ID到复购等级的映射
    6. product_type_dict: 商品ID到商品类型的映射
    """
    product_ids = []
    product_weights = []
    product_price_dict = {}
    product_category_dict = {}
    product_repurchase_level_dict = {}
    product_type_dict = {}

    for product in products:
        product_id = product[0]
        category = product[2]
        sale_price = product[5]
        repurchase_level = product[6]
        product_type = product[7]

        product_ids.append(product_id)
        product_weights.append(calculate_product_weight(product))

        product_price_dict[product_id] = sale_price
        product_category_dict[product_id] = category
        product_repurchase_level_dict[product_id] = repurchase_level
        product_type_dict[product_id] = product_type

    return (
        product_ids,
        product_weights,
        product_price_dict,
        product_category_dict,
        product_repurchase_level_dict,
        product_type_dict
    )

def choose_product_id(product_ids, product_weights):
    """
    根据权重随机选择一个商品 ID。
    """
    return random.choices(
        product_ids,
        weights=product_weights,
        k=1
    )[0]

def generate_quantity(sale_price, category, repurchase_level):
    """
    根据复购等级、商品类别和销售价格生成购买数量。

    设计原则：
    1. sale_price 用于限制高价商品的购买数量。
    2. repurchase_level 决定商品是否容易多件购买。
    3. category 作为微调器，体现不同商品类型的购买习惯差异。
    """

    # 高价商品优先限制购买数量
    if sale_price >= 1500:
        return random.choices(
            [1, 2],
            weights=[96, 4],
            k=1
        )[0]

    if sale_price >= 800:
        return random.choices(
            [1, 2],
            weights=[92, 8],
            k=1
        )[0]

    if sale_price >= 300:
        return random.choices(
            [1, 2, 3],
            weights=[78, 18, 4],
            k=1
        )[0]


    level_profile = CATEGORY_QUANTITY_PROFILE.get(category)

    # 根据category和repurchase_level选择数量分布
    if level_profile is not None:
        quantity_profile = level_profile.get(repurchase_level)

        if quantity_profile is not None:
            quantity_values, quantity_weights = quantity_profile

            return random.choices(
                quantity_values,
                weights=quantity_weights,
                k=1
            )[0]

    if repurchase_level == "高":
        return random.choices(
            [1, 2, 3, 4, 5],
            weights=[20, 30, 25, 15, 10],
            k=1
        )[0]

    if repurchase_level == "中":
        return random.choices(
            [1, 2, 3],
            weights=[55, 35, 10],
            k=1
        )[0]

    return random.choices(
        [1, 2],
        weights=[90, 10],
        k=1
    )[0]

def get_price_segment(sale_price):
    """
    根据商品售价划分价格段。

    低价：适合小额促销，折扣档位可以稍多。
    中价：折扣适中。
    高价：大额折扣少见。
    """
    if sale_price <= 100:
        return "低价"
    elif sale_price <= 800:
        return "中价"
    else:
        return "高价"
    
def get_category_discount_boost(category):
    """
    根据商品类别设置折扣倾向微调系数。

    返回值越大，越容易从有折扣档位中抽中折扣。
    """
    return CATEGORY_DISCOUNT_BOOST_MAP.get(category, 1.0)

def generate_discount(sale_price, category, repurchase_level):
    """
    根据复购等级、商品类别和价格生成折扣。

    规则：
    1. 大多数订单不打折。
    2. 高复购商品更容易有小额促销。
    3. 低复购高价商品大额折扣概率极低。
    4. 类别用于微调促销倾向。
    """
    price_segment = get_price_segment(sale_price)

    level_profile = DISCOUNT_PROFILE.get(repurchase_level)

    if level_profile is None:
        return 0

    discount_profile = level_profile.get(price_segment)

    if discount_profile is None:
        return 0

    discount_values, discount_weights = discount_profile

    category_boost = get_category_discount_boost(category)

    adjusted_weights = []

    for discount_value, weight in zip(discount_values, discount_weights):
        if discount_value == 0:
            adjusted_weights.append(weight)
        else:
            adjusted_weights.append(weight * category_boost)

    return random.choices(
        discount_values,
        weights=adjusted_weights,
        k=1
    )[0]

def calculate_age_weight(age):
    """
    根据年龄计算生成权重。

    设计目标：
    1. 0-9岁极少，但不是完全不存在。
    2. 10-17岁较少，作为未成年风险数据保留。
    3. 18-45岁为主要消费人群，权重最高。
    4. 46-65岁仍然有较高消费能力，但开始缓慢下降。
    5. 66-80岁明显下降。
    6. 81-100岁极低概率。
    7. 101-150岁极罕见，但保留极端风险数据。
    """

    if age < AGE_HARD_MIN or age > AGE_HARD_MAX:
        return 0

    # 0-3岁：极端风险数据，极罕见
    if age <= 3:
        return 0.002 + age * 0.001

    # 4-9岁：仍然极低，但略高于0-3岁
    if age <= 9:
        return 0.008 + (age - 4) * 0.002

    # 10-17岁：未成年客户，低概率，逐渐上升
    if age <= 17:
        return 0.08 + (age - 10) * 0.025

    # 18-30岁：快速进入主力消费年龄段
    if age <= 30:
        return 1.4 + (age - 18) * 0.08

    # 31-45岁：核心消费年龄段，保持最高权重
    if age <= 45:
        return 2.4

    # 46-60岁：开始缓慢下降
    if age <= 60:
        return 2.4 - (age - 46) * 0.07

    # 61-70岁：继续下降
    if age <= 70:
        return 1.35 - (age - 61) * 0.07

    # 71-80岁：低概率
    if age <= 80:
        return 0.65 - (age - 71) * 0.045

    # 81-90岁：很低概率
    if age <= 90:
        return 0.18 - (age - 81) * 0.012

    # 91-100岁：极低概率
    if age <= 100:
        return 0.045 - (age - 91) * 0.0035

    # 101-120岁：极罕见
    if age <= 120:
        return max(0.001, 0.008 - (age - 101) * 0.0003)

    # 121-150岁：极端风险数据，几乎不出现，但不彻底归零
    return 0.0005


AGE_VALUES = list(range(AGE_HARD_MIN, AGE_HARD_MAX + 1))
AGE_WEIGHTS = [calculate_age_weight(age) for age in AGE_VALUES]


def generate_age():
    """
    使用连续权重随机生成客户年龄。

    年龄范围来自 config.py：
    AGE_HARD_MIN 到 AGE_HARD_MAX。

    权重规则来自 calculate_age_weight(age)。
    """

    return random.choices(
        AGE_VALUES,
        weights=AGE_WEIGHTS,
        k=1
    )[0]

def allocate_customers_to_regions(
    regions,
    total_customers,
    min_customers_per_region,
    key_region_minimums
):
    """
    为每个地区分配客户数量。

    规则：
    1. 每个地区先分配一个基础下限。
    2. 重点地区使用更高下限。
    3. 剩余客户随机分配到各地区。
    """
    region_ids = [region[0] for region in regions]

    allocation = {}

    for region_id in region_ids:
        allocation[region_id] = min_customers_per_region
    
    for region_id, minimum_count in key_region_minimums.items():
        if region_id in allocation:
            allocation[region_id] = max(
                allocation[region_id],
                minimum_count
            )

    allocated_count = sum(allocation.values())

    if allocated_count > total_customers:
        raise ValueError("地区客户下限总和超过总客户数量，请调整配置")

    remaining_count = total_customers - allocated_count

    for _ in range(remaining_count):
        selected_region_id = random.choice(region_ids)
        allocation[selected_region_id] += 1

    return allocation

def generate_customer_name(gender, used_names):
    """
    随机生成顾客姓名
    
    参数说明:
    - gender: 字符串，传入 "男" 或 "女"
    - used_names: 集合(set)，用于全局姓名碰撞检测，控制重名率
    
    业务逻辑:
    1. 姓氏与名字池：随机组合姓氏库和名字库，名字库按性别+中性字拆分。
    2. 生成策略：70% 概率直接在预设名池中抽取，30%概率自由组合双字名。
    3. 碰撞处理：去重200次尝试均失败后，允许释放自然重名。
    
    返回:
    - full_name: 生成的姓名字符串
    """
    max_retry = 200

    if gender == "男":
        fixed_given_name_pool = MALE_GIVEN_NAMES + NEUTRAL_GIVEN_NAMES
        char_pool = MALE_NAME_CHARS + NEUTRAL_NAME_CHARS
    else:
        fixed_given_name_pool = FEMALE_GIVEN_NAMES + NEUTRAL_GIVEN_NAMES
        char_pool = FEMALE_NAME_CHARS + NEUTRAL_NAME_CHARS

    for _ in range(max_retry):
        family_name = random.choice(FAMILY_NAMES)

        # 70%使用预设名字池，30%自动组合双字名
        if random.random() < 0.7:
            given_name = random.choice(fixed_given_name_pool)
        else:
            first_char = random.choice(char_pool)
            second_char = random.choice(char_pool)

            while second_char == first_char:
                second_char = random.choice(char_pool)

            given_name = first_char + second_char

        full_name = family_name + given_name

        if full_name not in used_names:
            used_names.add(full_name)
            return full_name

    # 200次都撞名后，允许自然重名
    return full_name

def generate_customers(
    regions,
    total_customers,
    min_customers_per_region,
    key_region_minimums
):
    """
    生成客户数据。
    """

    genders = ["男", "女"]

    # 计算每个地区生成客户数量
    region_customer_allocation = allocate_customers_to_regions(
        regions=regions,
        total_customers=total_customers,
        min_customers_per_region=min_customers_per_region,
        key_region_minimums=key_region_minimums
    )

    customers = []
    used_names = set()
    customer_id = 1001

    # 按地区生成客户
    for region in regions:
        region_id = region[0]
        customer_count = region_customer_allocation[region_id]

        for _ in range(customer_count):
            gender = random.choice(genders)

            customer_name = generate_customer_name(
                gender=gender,
                used_names=used_names
            )

            age = generate_age()

            register_date = random_date(
                start_date="2016-01-01",
                end_date="2026-05-19"
            )

            customers.append([
                customer_id,
                customer_name,
                gender,
                age,
                region_id,
                register_date
            ])

            customer_id += 1

    # 写入csv
    with open(CUSTOMERS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id",
            "customer_name",
            "gender",
            "age",
            "region_id",
            "register_date"
        ])
        writer.writerows(customers)

    # 打印客户地区分布
    print("客户地区分布：")

    for region in regions:
        region_id = region[0]
        city = region[3]
        customer_count = region_customer_allocation[region_id]

        print(f"{city}：{customer_count} 人")

    return customers

def generate_payment_method():
    """
    按加权概率生成支付方式。

    设计目标：
    1. 支付宝、微信支付占比较高。
    2. 借记卡、信用卡占中等比例。
    3. 现金支付占比较低。
    """
    return random.choices(
        population=VALID_PAYMENT_METHODS,
        weights=[
            PAYMENT_METHOD_WEIGHT_MAP.get(payment_method, 0.01)
            for payment_method in VALID_PAYMENT_METHODS
        ],
        k=1
    )[0]

def generate_order_status():
    """
    按现实业务概率生成订单状态。

    说明：
    已完成订单应占大多数；
    已取消订单占少数；
    已退款订单应更少。
    """

    status_weight_map = {
        "已完成": 0.82,
        "已取消": 0.12,
        "已退款": 0.06
    }

    return random.choices(
        population=VALID_ORDER_STATUSES,
        weights=[
            status_weight_map.get(status, 0.01)
            for status in VALID_ORDER_STATUSES
        ],
        k=1
    )[0]

def generate_orders(customers, products, total_orders):
    """
    生成订单数据。

    规则：
    1. customer_id从已生成的customers列表中随机选择。
    2. product_id根据商品权重加权随机选择。
    3. unit_price从商品表中的sale_price自动获取。
    4. quantity根据商品复购等级、类别和价格生成。
    5. 保留少量脏数据，用于后续数据清洗和质量检查。
    """
    customer_ids = [customer[0] for customer in customers]

    customer_register_date_dict = {
        customer[0]: customer[5]
        for customer in customers
    }

    (
        product_ids,
        product_weights,
        product_price_dict,
        product_category_dict,
        product_repurchase_level_dict,
        product_type_dict
    ) = build_product_sampling_data(products)

    with open(ORDERS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        writer.writerow([
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

        for i in range(1, total_orders + 1):
            order_id = i

            # 从客户列表的id中随机选择
            customer_id = random.choice(customer_ids)

            # 根据权重随机选择商品
            product_id = choose_product_id(
                product_ids=product_ids, 
                product_weights=product_weights
            )

            # 根据商品ID得到商品属性
            category = product_category_dict[product_id]
            unit_price = product_price_dict[product_id]
            repurchase_level = product_repurchase_level_dict[product_id]
            product_type = product_type_dict[product_id]

            register_date = customer_register_date_dict[customer_id]

            order_date = random_date_after_register(
                register_date=register_date,
                end_date="2026-05-19"
            )

            if product_type in ["赠品", "试用品", "积分兑换"]:
                # 赠品、试用品、积分兑换的数量仅限1
                quantity = 1
                # 赠品、试用品、积分兑换无折扣
                discount = 0
            else:
                # 根据复购等级、类别、价格生成购买数量
                quantity = generate_quantity(
                    repurchase_level=repurchase_level,
                    category=category,
                    sale_price=unit_price
                )

                # 根据复购等级、类别、价格生成折扣力度
                discount = generate_discount(
                    repurchase_level=repurchase_level,
                    category=category,
                    sale_price=unit_price
                )
            
            payment_method = generate_payment_method()
            order_status = generate_order_status()

            if i % 157 == 0:
                quantity = 0

            if i % 233 == 0:
                unit_price = -unit_price

            if i % 311 == 0:
                order_status = ""

            writer.writerow([order_id, 
                             customer_id, 
                             product_id, 
                             order_date, 
                             quantity, 
                             unit_price, 
                             discount, 
                             payment_method, 
                             order_status
            ])

def main():
    """
        生成模拟电商业务数据。
        订单表依赖客户表和商品表,所以必须先生成customers和products，再生成orders。
    """
    ensure_raw_data_dir()

    regions = generate_regions()
    products = generate_products()

    customers = generate_customers(
        regions=regions,
        total_customers=TOTAL_CUSTOMERS,
        min_customers_per_region=MIN_CUSTOMERS_PER_REGION,
        key_region_minimums=KEY_REGION_CUSTOMER_MINIMUMS
    )

    generate_orders(
        customers=customers,
        products=products,
        total_orders=TOTAL_ORDERS
    )

    print("模拟数据生成完成！")
    print(f"地区数量：{len(regions)}")
    print(f"商品数量：{len(products)}")
    print(f"客户数量：{len(customers)}")
    print(f"订单数量：{TOTAL_ORDERS}")
    print(f"地区数据：{REGIONS_FILE}")
    print(f"商品数据：{PRODUCTS_FILE}")
    print(f"客户数据：{CUSTOMERS_FILE}")
    print(f"订单数据：{ORDERS_FILE}")

if __name__ == "__main__":
    main()
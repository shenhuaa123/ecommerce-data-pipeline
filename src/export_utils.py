import glob
import os
import shutil


def export_spark_csv_for_excel(spark_output_dir, output_file):
    """
    将Spark输出目录中的part-*.csv文件导出为Excel兼容的单个CSV文件。
    """

    csv_files = sorted(glob.glob(os.path.join(spark_output_dir, "part-*.csv")))

    if not csv_files:
        raise FileNotFoundError(f"未找到Spark输出目录中的CSV文件：{spark_output_dir}")

    csv_file = csv_files[0]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(csv_file, "r", encoding="utf-8", newline="") as source_file, \
            open(output_file, "w", encoding="utf-8-sig", newline="") as target_file:
        shutil.copyfileobj(source_file, target_file, length=1024 * 1024)

    print(f"已成功将Spark输出的CSV文件导出为Excel兼容的CSV文件：{output_file}")
from export_utils import export_spark_csv_for_excel

def rename_columns_for_export(df, column_name_map=None):
    """
    按字段映射关系重命名 DataFrame 字段。

    说明：
    1. 如果 column_name_map 为 None，则不重命名。
    2. 如果映射中的字段不存在于 df 中，则自动跳过，避免报错。
    """

    if column_name_map is None:
        return df

    renamed_df = df

    for old_name, new_name in column_name_map.items():
        if old_name in renamed_df.columns:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)

    return renamed_df

def save_dataframe_to_csv(df, output_dir, output_file, column_name_map=None):
    """
    保存 Spark DataFrame，并导出为Excel兼容的单个CSV文件。

    如果传入 column_name_map，则在导出前把字段名转换为中文。
    """
    export_df = rename_columns_for_export(
        df=df, 
        column_name_map=column_name_map
    )

    export_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_dir)

    export_spark_csv_for_excel(
        spark_output_dir=output_dir,
        output_file=output_file
    )


def save_multiple_dataframes(save_tasks):
    """
    批量保存多个 Spark DataFrame。

    每个 task 可以选择性传入 column_name_map。
    """

    for task in save_tasks:
        name = task["name"]
        df = task["df"]
        output_dir = task["output_dir"]
        output_file = task["output_file"]
        column_name_map = task.get("column_name_map")

        print(f"正在保存：{name}")

        save_dataframe_to_csv(
            df=df,
            output_dir=output_dir,
            output_file=output_file,
            column_name_map=column_name_map
        )

        print(f"保存完成：{name} -> {output_file}")
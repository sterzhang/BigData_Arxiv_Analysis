from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date
import matplotlib.pyplot as plt

def generate_plot(file_suffix):
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("PaperAnalysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # 构造文件路径
    file_path = f"cs_{file_suffix}.csv"

    # 读取 CSV 文件为 Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # 提取提交日期中的年份
    df = df.withColumn("提交日期", to_date(df["提交日期"], "dd MMMM, yyyy"))
    df = df.withColumn("年份", year(df["提交日期"]))

    # 按年份统计论文数量
    papers_by_year = df.groupBy("年份").count().orderBy("年份")

    # 显示统计结果
    papers_by_year.show()

    # 将统计结果转换为 Pandas DataFrame 以便绘图
    papers_by_year_pandas = papers_by_year.toPandas()

    # 绘制论文数量的统计图
    plt.figure(figsize=(10, 6))
    bars = plt.bar(papers_by_year_pandas["年份"], papers_by_year_pandas["count"], color="skyblue")
    
    # 在柱状图顶部添加数字
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, yval, ha='center', va='bottom')

    plt.xlabel("Year")
    plt.ylabel("Number of Papers")
    plt.title(f"Annual Count of Papers in {file_suffix.upper()}")
    plt.xticks(papers_by_year_pandas["年份"], rotation=45, ha="right")
    plt.tight_layout()  # 调整布局以避免保存时切割

    # 保存图形到文件
    output_file_path = f'./visualization/cs_{file_suffix}_yearly.png'
    plt.savefig(output_file_path, format='PNG', bbox_inches='tight')
    plt.close()  # 关闭图形，以便下一次绘图

    # 停止 Spark 会话
    spark.stop()

# 文件后缀列表
file_suffixes = ["CL", "CV", "DB", "DC", "NI", "SE"]

# 为每个文件后缀生成图片
for suffix in file_suffixes:
    generate_plot(suffix)

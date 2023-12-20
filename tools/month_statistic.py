from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, to_date
import matplotlib.pyplot as plt

def generate_monthly_plot(file_suffix):
    # 创建 Spark 会话，并设置 timeParserPolicy
    spark = SparkSession.builder.appName("PaperAnalysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # 构造文件路径
    file_path = f"cs_{file_suffix}.csv"
    
    # 读取 CSV 文件为 Spark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # 将字符串日期转换为日期格式
    df = df.withColumn("提交日期", to_date(df["提交日期"], "dd MMMM, yyyy"))

    # 提取年份和月份
    df = df.withColumn("年份", year(df["提交日期"]))
    df = df.withColumn("月份", month(df["提交日期"]))

    # 按年份和月份统计论文数量
    papers_by_month = df.groupBy("年份", "月份").count().orderBy("年份", "月份")

    # 将统计结果转换为 Pandas DataFrame 以便绘图
    papers_by_month_pandas = papers_by_month.toPandas()

    # 设置图表大小和子图布局
    fig, ax = plt.subplots(figsize=(15, 10))

    # 遍历每一年的数据
    for year_ in papers_by_month_pandas['年份'].unique():
        # 选择当前年份的数据
        yearly_data = papers_by_month_pandas[papers_by_month_pandas['年份'] == year_]
        # 绘制每年的数据，添加标签以便创建图例
        ax.plot(yearly_data['月份'], yearly_data['count'], label=str(year_))

    # 添加图例。考虑放置在图表外部以节省空间。
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    # 添加网格线以提高可读性
    ax.grid(True)

    # 添加标题和轴标签
    ax.set_title(f'Monthly Count of Papers in {file_suffix.upper()}', fontsize=18)
    ax.set_xlabel('Month', fontsize=14)
    ax.set_ylabel('Number of Papers', fontsize=14)

    # 设置x轴的刻度，使其更容易阅读
    ax.set_xticks(range(1, 13))  # 1到12月
    ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])

    # 保存图形到文件
    output_file_path = f'./visualization/cs_{file_suffix}_monthly.png'
    plt.savefig(output_file_path, format='PNG', bbox_inches='tight')

    # 显示图表
    plt.show()
    plt.close()  # 关闭绘图对象，防止重叠

    # 停止 Spark 会话
    spark.stop()

# 文件后缀列表
file_suffixes = ["CL", "CV", "DB", "DC", "NI", "SE"]

# 为每个文件后缀生成图片
for suffix in file_suffixes:
    generate_monthly_plot(suffix)

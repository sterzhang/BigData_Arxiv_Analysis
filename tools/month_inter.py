from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from datetime import datetime
import matplotlib.pyplot as plt

# 创建 Spark 会话
spark = SparkSession.builder.appName("arxiv_analysis").getOrCreate()
# 读取 CSV 文件为 DataFrame
df = spark.read.option("header", "true").csv("arxiv_cs_22_11_30_to_23_12_1year.csv")

# 定义一个UDF来计算月份差
@udf(IntegerType())
def calculate_month_diff(submission_date, initial_date):
    # 将日期字符串转换为日期对象
    submission_date = datetime.strptime(submission_date, "%d %B, %Y")
    initial_date = datetime.strptime(initial_date, "%B %Y")
    
    # 计算月份差
    month_diff = (submission_date.year - initial_date.year) * 12 + (submission_date.month - initial_date.month)
    
    return month_diff

# 使用UDF计算月份差并创建一个新列
df = df.withColumn("month_diff", calculate_month_diff(df["提交日期"], df["最初公布日期"]))
# 统计月份差的个数
month_diff_counts = df.groupBy("month_diff").count().orderBy("month_diff")

# 显示结果
# month_diff_counts.show()

# 转换为 Pandas DataFrame，只保留月份差在0到12之间的记录
filtered_df = df.filter((df["month_diff"] > 0) & (df["month_diff"] <= 12)).toPandas()

# 统计月份差的个数
month_diff_counts = filtered_df["month_diff"].value_counts().sort_index()


def add_labels(ax, rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f"{height}",
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha="center", va="bottom")

plt.figure(figsize=(12, 6))
bars = plt.bar(month_diff_counts.index, month_diff_counts.values)
plt.xlabel("Time Interval (month)")
plt.ylabel("Count Number")
plt.xticks(range(1, 13))  # 从1到12的整数刻度
plt.title("Time Interval Between Initial Submission and Final Submission \n from 11/30/2022 to 12/01/2023")
plt.grid(True)

# 调用函数添加文本标签
add_labels(plt.gca(), bars)

plt.show()
# 保存
output_visualization_path = './visualization/MonthInterval.png'
plt.savefig(output_visualization_path, bbox_inches='tight')


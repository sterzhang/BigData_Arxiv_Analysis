from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np


# 创建 Spark 会话
spark = SparkSession.builder.appName("cata_num_rank").getOrCreate()

# 读取 CSV 文件为 DataFrame
data = spark.read.option("header", "true").csv("arxiv_cs_22_11_30_to_23_12_1year.csv")

# 展开标签列，将其拆分为多个行
data = data.withColumn("tags", split(col("标签"), ", "))
data = data.withColumn("tag", explode(col("tags")))

# 计算每个标签下的论文数量
tag_counts = data.groupBy("tag").count()
tag_counts = tag_counts.orderBy("count", ascending=False)

# 使用 Pandas 的字符串方法进行cs类别的筛选
filtered_tags = tag_counts.filter(tag_counts["tag"].startswith("cs."))

# 显示筛选后的结果并保存
filtered_tags.show(100)
filtered_tags_pandas = filtered_tags.toPandas()
filtered_tags_pandas.to_csv('./results/CataNumRank_LastYear.csv', index=False)


# 添加全名
full_names = {
    'cs.LG': 'Machine Learning',
    'cs.CV': 'Computer Vision',
    'cs.AI': 'Artificial Intelligence',
    'cs.CL': 'Computational Linguistics',
    'cs.RO': 'Robotics',
    'cs.CR': 'Cryptography and Security',
    'cs.IT': 'Information Theory',
    'cs.HC': 'Human-Computer Interaction',
    'cs.CY': 'Cybersecurity',
    'cs.SD': 'Software Design',
    'cs.DC': 'Distributed Computing',
    'cs.DS': 'Data Structures',
    'cs.SE': 'Software Engineering',
    'cs.IR': 'Information Retrieval',
    'cs.SI': 'Social Informatics',
    'cs.NI': 'Networking and Internet Architecture',
    'cs.LO': 'Logic in Computer Science',
    'cs.NE': 'Neural and Evolutionary Computing',
    'cs.GT': 'Game Theory',
    'cs.MA': 'Multiagent Systems',
    'cs.DM': 'Discrete Mathematics',
    'cs.GR': 'Graphics',
    'cs.MM': 'Multimedia',
    'cs.CC': 'Computational Complexity',
    'cs.CE': 'Computational Engineering',
    'cs.DB': 'Databases',
    'cs.AR': 'Computer Architecture',
    'cs.PL': 'Programming Languages',
    'cs.ET': 'Emerging Technologies',
    'cs.CG': 'Computational Geometry',
    'cs.FL': 'Formal Languages',
    'cs.PF': 'Performance Analysis',
    'cs.DL': 'Digital Libraries',
    'cs.SC': 'Symbolic Computation',
    'cs.MS': 'Mathematical Software',
    'cs.OS': 'Operating Systems',
    'cs.OH': 'Other Hardware',
    'cs.GL': 'General Literature'
}
df = pd.read_csv('./results/CataNumRank_LastYear.csv')  # 确保文件名和路径正确
df['full_name'] = df['tag'].map(full_names)
df.to_csv('./results/CataNumRank_LastYear.csv', index=False)

# 现在开始绘制饼图
# 设定一个更精细的颜色方案
colors = sns.color_palette('pastel')[0:len(filtered_tags_pandas)]

# 计算总数以便计算百分比
total_count = filtered_tags_pandas['count'].sum()

# 获取前10名的计数值
top_10_counts = filtered_tags_pandas['count'].head(10)

# 设定显示饼图的阈值
threshold = 3000  # 只有数量超过这个阈值的标签才显示百分比

# 选出前10名
explode_values = [0.1 if count in top_10_counts.values else 0 for count in filtered_tags_pandas['count']]

# 绘制饼图
plt.figure(figsize=(10, 10))
wedges, texts = plt.pie(filtered_tags_pandas['count'],
                        startangle=140,
                        colors=colors,
                        explode=explode_values,
                        textprops={'fontsize': 14})

# 在饼图上添加注解
for i, (wedge, count) in enumerate(zip(wedges, filtered_tags_pandas['count'])):
    if count in top_10_counts.values:
        angle = (wedge.theta2 - wedge.theta1) / 2. + wedge.theta1
        x = wedge.r * 0.65 * np.cos(np.deg2rad(angle))
        y = wedge.r * 0.65 * np.sin(np.deg2rad(angle))
        plt.annotate(f"{count:,}", (x, y), ha='center', va='center', fontsize=12, color='black')

# 饼图样式美化
plt.setp(wedges, width=0.3, edgecolor='white')

# 添加图例
plt.legend(wedges, filtered_tags_pandas['tag'],
           title="Tags",
           loc="center left",
           bbox_to_anchor=(1, 0, 0.5, 1))

# 设置标题
plt.title('Top CS Categories by Number of Papers\n from 11/30/2022 - 12/01/2023', pad=20, fontsize=20)

# 保存饼图
output_visualization_path = './visualization/CataNumRank_LastYear_PieChart.png'
plt.savefig(output_visualization_path, bbox_inches='tight')

# 显示图形
plt.show()

print(f"Visualization saved to {output_visualization_path}")



# #可视化结果
# total_count = filtered_tags_pandas['count'].sum()
# threshold = 3000
# def my_autopct(pct):
#     return f'{pct:.1f}%' if pct >= threshold / total_count * 100 else ''

# labels = [f'{tag}' if count >= threshold else '' for tag, count in zip(filtered_tags_pandas['tag'], filtered_tags_pandas['count'])]
# plt.figure(figsize=(16, 16))
# plt.pie(filtered_tags_pandas['count'], labels=labels, autopct=my_autopct, startangle=140, textprops={'fontsize':14})
# plt.rcParams['font.size'] = 20
# plt.title('Distribution of Tags')

# plt.savefig('./visualization/cata_num.png')
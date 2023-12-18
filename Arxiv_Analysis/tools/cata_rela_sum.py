from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split, col
from pyspark.sql.types import ArrayType, StringType
import re
from itertools import combinations
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

# 创建 Spark 会话
spark = SparkSession.builder.appName("diff_class_rela").getOrCreate()

# 读取data
data = spark.read.option("header", "true").csv("arxiv_cs_22_11_30_to_23_12_1year.csv")

# 过滤
def filter_tags(tags):
    pattern = re.compile(r'^\d+\.\d+(/\w+)*$')
    return [tag for tag in tags if not pattern.match(tag)]

filter_tags_udf = udf(filter_tags, ArrayType(StringType()))

# 生成所有可能的标签对的 UDF
def generate_pairs(tags):
    if tags is not None:
        return list(combinations(sorted(set(tags)), 2))
    return []

generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

# 应用 UDFs处理data
data = data.withColumn("tags", filter_tags_udf(split(col("标签"), ", ")))
data = data.withColumn("tag_pairs", explode(generate_pairs_udf("tags")))

# 统计每个标签对的出现次数，并打印出前20行
tag_pair_counts = data.groupBy("tag_pairs").count()
tag_pair_counts.show()

# 转换为 Pandas DataFrame，并保存至csv文件中
tag_pair_counts_df = tag_pair_counts.toPandas()

tag_pair_counts_df.to_csv('./results/ClassRelation.csv', index=False)


# 创建图形实例
G = nx.Graph()

# 添加边和节点到图中
for index, row in tag_pair_counts_df.iterrows():
    tag_pair = row['tag_pairs']
    weight = row['count']
    G.add_edge(tag_pair[0], tag_pair[1], weight=weight)

# 计算所有边的权重
weights = nx.get_edge_attributes(G, 'weight').values()

# 设置边权重和节点大小的阈值
weight_threshold = sorted(weights, reverse=True)[int(len(weights) * 0.2)]  # 例如，保留权重最大的20%的边
size_threshold = sorted([G.degree(node) for node in G], reverse=True)[int(len(G) * 0.2)]  # 保留度最大的20%的节点

# 过滤掉权重低的边和度低的节点
G_filtered = nx.Graph()
for u, v, weight in G.edges(data='weight'):
    if weight > weight_threshold and G.degree(u) > size_threshold and G.degree(v) > size_threshold:
        G_filtered.add_edge(u, v, weight=weight)

# 调整节点大小
node_sizes = [G_filtered.degree(node) * 100 for node in G_filtered]

# 设置节点大小的阈值，只为大于此阈值的节点添加标签
node_size_threshold = np.percentile(node_sizes, 85)  # 可以调整百分位数来选择更大的节点

# 绘制图形
plt.figure(figsize=(15, 15))
pos = nx.spring_layout(G_filtered, k=0.15, iterations=50)

# 绘制节点
nx.draw_networkx_nodes(G_filtered, pos, node_size=node_sizes, node_color='skyblue', alpha=0.9)

# 绘制边
nx.draw_networkx_edges(G_filtered, pos, width=1, alpha=0.5, edge_color='gray')

# 添加节点标签
nx.draw_networkx_labels(G_filtered, pos, font_size=8, font_color='black')

# 设置标题和关闭坐标轴
plt.title('Catagory Relationship Network\nfrom 11/30/2022 - 12/01/2023', fontsize=25)
plt.axis('off')

# 保存图形到文件
output_file_path = './visualization/CatagoryRelationship_LastYear.png'
plt.savefig(output_file_path, format='PNG', bbox_inches='tight')

# 显示图形
plt.show()

print(f"Graph saved to {output_file_path}")

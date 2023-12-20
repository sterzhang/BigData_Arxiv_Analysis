from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, VectorSizeHint
from pyspark.ml.linalg import Vectors, VectorUDT

import matplotlib.pyplot as plt
import pandas as pd
import os

def visualize_clusters(result_df, output_path):
    # 将 Spark DataFrame 转换为 Pandas DataFrame
    result_pd = result_df.select("title", "features", "prediction").toPandas()

    # 将 DenseVector 转换为列表
    result_pd['features'] = result_pd['features'].apply(lambda x: list(x))

    # 绘制散点图
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'yellow']
    for cluster in range(6):
        cluster_data = result_pd[result_pd['prediction'] == cluster]
        plt.scatter(cluster_data['features'].apply(lambda x: x[0]),
                    cluster_data['features'].apply(lambda x: x[1]),
                    label=cluster_titles[cluster], color=colors[cluster])

    plt.title('DataBase subcategories')
    plt.xlabel('Feature 1')
    plt.ylabel('Feature 2') # 将图例框置于右侧
    plt.legend(loc='upper right', fontsize='x-small')

    # 保存图像到文件
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

# 定义聚类标签
cluster_titles = {
    0: 'Database Applications',
    1: 'Computing Systems',
    2: 'Database Performance Evaluation',
    3: 'Database Performance & Metrics',
    4: 'Analysis of Database Queries and Structures',
    5: 'LOB Storage, Clustering, and Performance'
}

if __name__ == "__main__":
    # 创建 Spark 会话
    spark = SparkSession.builder.appName("DB_cata_kmeans").getOrCreate()

    # ----读取并处理数据----
    print("------------------读取数据-----------------")

    # 读取数据
    data = spark.read.option("header", "true").csv("./dataset/bert_CV.csv")

    # 定义一个UDF将字符串表示的列表转换为密集向量
    def parse_feature_string(feature_str):
        # 去除首尾的方括号，并使用逗号分隔字符串
        feature_values = feature_str.strip("[]").split(", ")

        # 将字符串转换为浮点数，但在转换之前检查是否接近零
        def safe_float(value):
            try:
                float_value = float(value)
                # 如果浮点数非常接近零，将其设置为零
                return 0.0 if abs(float_value) < 1e-8 else float_value
            except ValueError:
                return None

        # 使用safe_float进行转换
        feature_list = [safe_float(value) for value in feature_values]

        if len(feature_list) != 2:
            return None

        # 过滤掉None值
        feature_list = [value for value in feature_list if value is not None]

        # 将浮点数列表转换为密集向量
        feature_vector = Vectors.dense(feature_list)
        return feature_vector

    # 将UDF注册为Spark SQL函数
    parse_feature_udf = udf(parse_feature_string, VectorUDT())

    # 使用UDF转换"feature"列
    data = data.withColumn("feature", parse_feature_udf(data["feature"]))

    # 打印转换后的数据
    data.show(truncate=False)
    
    # 创建特征向量
    assembler = VectorAssembler(inputCols=["feature"], outputCol="features")
    data = assembler.transform(data)

    print("Number of partitions in RDD:", data.rdd.getNumPartitions())

    # 创建KMeans模型
    kmeans = KMeans().setK(6).setSeed(1)  # 设置簇的数量和随机种子
    model = kmeans.fit(data)

    # 进行预测
    predictions = model.transform(data)

    # 定义保存路径
    result_output_path = "./visualization/kmeans_DB.png"

    # 可视化聚类结果
    visualize_clusters(predictions, result_output_path)

    print(f"KMeans聚类结果已保存到: {result_output_path}")
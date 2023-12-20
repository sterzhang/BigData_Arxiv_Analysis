import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_radar_chart(category, data, title, output_path):
    # 数量的标签和值
    labels = data['neighbour']
    stats = data['count']

    # 设置雷达图的角度
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()

    # 使雷达图封闭
    stats = np.concatenate((stats, [stats[0]]))
    angles += angles[:1]

    # 绘制雷达图
    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
    ax.fill(angles, stats, color='darkred', alpha=0.25)
    ax.plot(angles, stats, color='red', linewidth=2)
    ax.set_yticklabels([])
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=12, color='darkblue')

    # 设置标题
    plt.title(title, color='black', y=1.1, fontsize=15)
    # 保存图形
    plt.savefig(output_path, format='PNG', bbox_inches='tight')


# 读取CSV文件
df = pd.read_csv('./results/CS_relationship.csv')

# 获取所有独特的category
categories = df['category'].unique()

# 遍历每个category，绘制雷达图
for category in categories:
    category_data = df[df['category'] == category]
    # 按neighbour分组并计算总数
    category_data = category_data.groupby('neighbour').sum().reset_index()  
    output_path = f'./visualization/radar_{category}.png'
    plot_radar_chart(category, category_data, f"Radar Chart for {category}", output_path)


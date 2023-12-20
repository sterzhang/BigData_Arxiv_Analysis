import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt

df = pd.read_csv("./results/CataNumRank_LastYear.csv") 

# 词云所需的数据格式{full_name: count}
data = dict(zip(df['full_name'], df['count']))

# 创建一个词云对象
wordcloud = WordCloud(width=800, height=400, background_color='white')

wordcloud.generate_from_frequencies(data)

# 绘制词云图
plt.figure(figsize=(15, 8), facecolor=None)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.tight_layout(pad=0)

# 保存饼图到文件
output_visualization_path = './visualization/WordCloud.png'
plt.savefig(output_visualization_path, format='PNG', bbox_inches='tight')

# 显示图形
plt.show()

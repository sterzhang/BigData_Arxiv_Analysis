import pandas as pd

# 读取 CSV 文件
df = pd.read_csv("./results/ClassRelation.csv")

# 转换 tag_pairs 列为列表
df['tag_pairs'] = df['tag_pairs'].apply(eval)

# 感兴趣的类别列表
categories = ['cs.LG', 'cs.CV', 'cs.AI', 'cs.CL', 'cs.RO' ,'cs.CR']

# 用于存储所有结果的DataFrame列表
top_pairs_list = []

# 遍历每个类别，并将数量前 8 的标签对保存
for category in categories:
    # 筛选包含当前类别的行
    filtered_pairs = df[df['tag_pairs'].apply(lambda x: category in x)]

    # 按数量排序并选择前 8 行
    top_pairs = filtered_pairs.sort_values(by='count', ascending=False).head(8)

    # 添加一个列来指示当前类别
    top_pairs['category'] = category

    # 将结果添加到列表中
    top_pairs_list.append(top_pairs)

# 合并所有DataFrame
top_pairs_all_categories = pd.concat(top_pairs_list, ignore_index=True)

# 保存汇总结果到CSV文件
top_pairs_all_categories.to_csv("./results/CS_relationship.csv", index=False)

print("Results saved to ./results/CS_relationship.csv")

def adjust_tags(row):
    # 从tag_pairs中移除category标签，并将另一个标签赋值给tag1
    tags = eval(row['tag_pairs'])
    tags.remove(row['category'])
    return tags[0] if tags else None

# 读取CSV文件
df = pd.read_csv('./results/CS_relationship.csv')  # 替换为您的CSV文件的路径

df['neighbour'] = df.apply(adjust_tags, axis=1)

# 保存更新后的DataFrame为CSV文件
df.to_csv('./results/CS_relationship.csv', index=False)

print("Updated DataFrame saved to ./results/CS_relationship.csv")


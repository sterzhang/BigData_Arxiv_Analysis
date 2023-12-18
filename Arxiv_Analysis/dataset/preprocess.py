"""
该文件在本地执行，执行完后将处理好的数据put到hdfs中
hdfs dfs -put /home/hadoop/bigdata/Arxiv_Analysis/dataset/arxiv_cs_22_11_30_to_23_12_1year.csv /user/hadoop/
"""
import pandas as pd

# hadoop中的文件路径
# csv_file_path = "./dataset/raw_arxiv_cs_22_11_30_to_23_12_1year.csv"
csv_file_path = "./dataset/raw_arxiv_cs_07_to_23.csv"
df = pd.read_csv(csv_file_path)

def remove_symbols(s):
    return ''.join(e for e in s if e not in ('\'', '\"', '[', ']', ';'))

df=df.applymap(remove_symbols)
df['最初公布日期'] = df['最初公布日期'].str.replace('.', '')

# 用于删除 "doi" 及其后面的部分
def remove_doi(tag_string):
    if 'doi' in tag_string:
        # 分割字符串并保留 "doi" 之前的部分
        return tag_string.split(', doi')[0]
    return tag_string

# 应用函数到 '标签' 列
df['标签'] = df['标签'].apply(remove_doi)

# 保存预处理后的数据到本地同目录下的arxiv_cs_1year.csv
# new_csv_file_path = './dataset/arxiv_cs_22_11_30_to_23_12_1year.csv'  
new_csv_file_path = './dataset/arxiv_cs_07_to_23.csv'  
df.to_csv(new_csv_file_path, index=False)
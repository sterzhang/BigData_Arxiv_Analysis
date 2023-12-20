from transformers import BertModel, BertConfig, BertTokenizer
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
import torch
from tqdm import tqdm
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained("bert-base-uncased")

csv_file_path = "cs_CV.csv"
df = pd.read_csv(csv_file_path)

output_array = []

for text in tqdm(df["标题"]):
    encoded_input = tokenizer(text, return_tensors='pt')
    output = model(**encoded_input)

    output_array.append(output.pooler_output.detach().numpy()[0])

n_components = 2  # 您可以根据需要选择合适的维度
pca = PCA(n_components=n_components)

pca_result = pca.fit_transform(output_array)
# 打开文件并写入列表
# np.savetxt('pca_result.txt', pca_result)

# pca_result = np.loadtxt('pca_result.txt')

print(pca_result.shape)

# 创建一个DataFrame
nxt_df = pd.DataFrame({
    'title': df["标题"],  # 请用实际的标题替换
    'feature': pca_result.tolist()  # 转换为列表，每个元素是一个包含两个特征的列表
})

nxt_df.to_csv('bert_CV.csv', index=False)


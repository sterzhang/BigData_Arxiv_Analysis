# 📰 BigData Project : Arxiv_Analysis (Computer Science)

Get to know some interesting things concerning the academic frontier in CS by analysing numerous papers in ArXiv.


Group Leader: Jianshu Zhang

Group Member: Yanfu Kai; Ziheng Peng 

Adviser : Prof. Run Wang

[Report](https://github.com/sterzhang/BigData_Arxiv_Analysis/blob/main/Report.pdf)
[Pre](https://github.com/sterzhang/BigData_Arxiv_Analysis/blob/main/presentation.pptx)

# Our work
![image](https://github.com/sterzhang/BigData_Arxiv_Analysis/assets/119802220/e06a5622-6e25-4077-b0e1-52b4ffc17af5)


# Arxiv_Analysis Project Structure

This project is structured as follows:

- `./crawler_utils`: Contains utilities for crawling data from [Arxiv](https://arxiv.org/).

- `./dataset`:  To replicate the whole project, you need to download [bert-base-uncased](https://huggingface.co/bert-base-uncased). And all the csv file can be reproduced by running _./crawler_utils/crawl.py_,  _./dataset/prepocess.py_, _./dataset/trans_to_bert.py_. 

- `./results`: The result of data analysis.

- `./tools`: Includes every tools used for analysis and the outputs will be saved in _./visualization_.
Below is a list of the scripts along with a brief description of their purpose:

   _`cata_kmeans.py`_: Performs K-Means clustering on the dataset to identify distinct groups based on characteristics.

   _`cata_num_rank.py`_: Rank the number of different catagories from 11/30/2022 - 12/01/2023 .

   _`cata_rela_cs.py`_: Analyzes the relationship between different categories .

   _`cata_rela_sum.py`_: Summarizes the relationships between categories by using a network.

   _`cata_wordcloud.py`_: Generates a word cloud from categorical data to visualize the frequency or importance of categories.

   _`month_inter.py`_: Try to find the statistic regularity of the interval of the initial and the last submission.

   _`month_statistic.py`_: Interprets monthly data, possibly to identify trends or patterns over time.

  _ `rela_cs_radar.py`_: Creates a radar chart to show the relationship of cs with other catagories.

   _`year_statistic.py`_: Calculates yearly statistics to provide insights into long-term trends.

- `./visualization`: Visualization of data analysis results, containing various and appropriate figures. 

- `./test_if_spark_can_work.py`: Test the Spark environment setup.

# Tasks
![image](https://github.com/sterzhang/BigData_Arxiv_Analysis/assets/119802220/497e4747-6a68-4a17-8c5b-2fa3a4dc6603)

# Algorithm Design
![image](https://github.com/sterzhang/BigData_Arxiv_Analysis/assets/119802220/8a782d1d-dd86-4397-89f0-a161a172e7a0)


# Few examples of our visualization
![image](https://github.com/sterzhang/BigData_Arxiv_Analysis/assets/119802220/6d70133b-e491-4c6e-99a1-9c467d67fe3e)


import requests
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
import os
import random

import smtplib
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


def get_one_page(url):
    response = requests.get(url)
    print(response.status_code) 
    while response.status_code == 403:
        time.sleep(500 + random.uniform(0, 500))
        response = requests.get(url)
        print(response.status_code)
    print(response.status_code)
    if response.status_code == 200:
        return response.text

    return None


def main():
    page_sum = 7237

    for page_num in range(0, page_sum, 200):
        url = 'https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=&terms-0-field=title&classification-computer_science=y&classification-physics_archives=all&classification-include_cross_list=include&date-year=&date-filter_by=date_range&date-from_date=2022-11-01&date-to_date=2022-12-01&date-date_type=submitted_date&abstracts=hide&size=200&order=submitted_date&start='
        url = url + str(page_num)
        html = get_one_page(url)
        soup = BeautifulSoup(html, 'html.parser')

        arxiv_results = soup.find_all('li', class_='arxiv-result')

        columns = ['arXiv ID', '标题', '标签', '作者', '提交日期', '最初公布日期'] 
        paper = pd.DataFrame(columns=columns)

        # 遍历每个元素
        for result in arxiv_results:
            # 提取arXiv ID
            arxiv_id = result.find('p', class_='list-title').a.text

            # 提取标题
            title = result.find('p', class_='title').text.strip()

            # 提取标签
            tags = [tag.text for tag in result.find_all('span', class_='tag')]

            # 提取作者
            authors = [author.text.strip() for author in result.find('p', class_='authors').find_all('a')]

            # 提取提交日期和最初公布日期
            submitted_span = result.find('span', string='Submitted')
            submitted_date = submitted_span.find_next_sibling(string=True).strip() if submitted_span else None

            announced_span = result.find('span', string='originally announced')
            announced_date = announced_span.find_next_sibling(string=True).strip() if announced_span else None

            print("\n")
            paper = paper._append({
                'arXiv ID': arxiv_id,
                '标题': title,
                '标签': tags,
                '作者': authors,
                '提交日期': submitted_date,
                '最初公布日期': announced_date
            }, ignore_index=True)

        paper.to_csv('test.csv', index=False, mode='a', header=not os.path.exists('test.csv'))


if __name__ == '__main__':
    main()
    time.sleep(1)





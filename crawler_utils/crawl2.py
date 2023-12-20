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
    if response.status_code == 400:
        return 400
    return None


def main():
    page_sum = 6000
    years = ['2022','2023']
    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    days = ['01','15']
    for year in range(2):
        for month in range(12):
            for day in range(2):
                for page_num in range(0, page_sum, 200):
                    day2 = (day + 1) % 2
                    month2 = month
                    year2 = year
                    if day == 1:
                        month2 = (month + 1) % 12
                        if month2 == 0:
                            year2 = year + 1
                            if year2 == 2:
                                return
                    url = 'https://arxiv.org/search/advanced?advanced=&terms-0-operator=AND&terms-0-term=&terms-0-field=title&classification-computer_science=y&classification-physics_archives=all&classification-include_cross_list=include&date-year=&date-filter_by=date_range&date-from_date='+years[year]+'-'+months[month]+'-'+days[day]+'&date-to_date='+years[year2]+'-'+months[month2]+'-'+days[day2]+'&date-date_type=submitted_date&abstracts=hide&size=200&order=submitted_date&start='
                    url = url + str(page_num)
                    print(url)
                    html = get_one_page(url)
                    if html == 400:
                        continue
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





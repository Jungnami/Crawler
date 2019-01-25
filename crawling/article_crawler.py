# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pymysql
import datetime


req = requests.get('https://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&sectionId=100&date=20190118')
html = req.text
soup = BeautifulSoup(html, 'html.parser')
crawlingData = []

for article in soup.findAll("div", {"class": "ranking_thumb"}):
    crawlingData.append((article.a.get('href'), article.a.get('title'), article.img.get('src'), datetime.datetime.now(), datetime.datetime.now()))


# MySQL Connection 연결
conn = pymysql.connect(host='', user='', password='', db='', charset='')

# Connection 으로부터 Cursor 생성
curs = conn.cursor()

# SQL문 실행
sql = "SELECT * FROM ARTICLE"
curs.execute(sql)
fetchedData = curs.fetchall()


# fetch 를 통해 가져온것과 크롤링 해온 것 중복 검사 -> fetchedData vs crawlingData의 link 이용
# updateDate where 절로 비교
# 겹치면 업데이트 아니면 인서트


print fetchedData

# Connection 닫기
conn.close()


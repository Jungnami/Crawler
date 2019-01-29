# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import datetime
from secretModule import dbconfig
from collections import namedtuple

# 현재 사이트에 있는 최신 순위 기사들 crawling
todayDate = datetime.datetime.today().strftime('%Y%m%d')
req = requests.get('https://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&sectionId=100&date='+todayDate)
html = req.text
soup = BeautifulSoup(html, 'html.parser')
crawlingData = []

for i in range(1, 31):
    fields = 'title link img_src office registe_date update_date ranking'
    crawlingTuple = namedtuple('crawlingTuple', fields)
    article = soup.find("li", {"class": "ranking_item is_num"+str(i)})
    img_src = ''
    if not (article.img is None):
        img_src = article.img.get('src')
    crawlingData.append(crawlingTuple(article.a.get('title'), article.a.get('href'), img_src, article.find('div', {"class": "ranking_office"}).text, datetime.datetime.now(), datetime.datetime.now(), i))

# DB에 있는 최신 기사 fetch
conn = dbconfig.dbConnect()
curs = conn.cursor()
# TODO ranking 이 최신인거 30개 가져와야함. ranking 1-30까지 최근에 업데이트 된것들 골라야함
# sql = "SELECT id, link FROM article WHERE update_date=(SELECT MAX(update_date) FROM article) ORDER BY ranking;"
# curs.execute(sql)

sql = "select * from article where ranking = '1' and update_date = max(update_date);"
curs.execute(sql)
result = curs.fetchall()
#
# fetchedTuple = namedtuple('fetchedTuple', 'id link')
# fetchedData = []
# for result in curs.fetchall():
#     result = fetchedTuple(result[0], result[1])
#     fetchedData.append(result)
#
# # fetch 를 통해 가져온것과 크롤링 해온 것 중복 검사 -> fetchedData vs crawlingData의 link 이용
# # crawlingData 는 title, link, img_src, registe_date, update_date, ranking
# # fetchedDate 는 id, link
#
# intersection = set(list(map(lambda x: x.link, crawlingData))) & set(list(map(lambda x: x.link, fetchedData)))
#
# #TODO id 로 where 절 걸 방법 찾기
# for item in crawlingData:
#     if item.link in intersection:
#         print "update"
#         sql = "UPDATE article SET title = %s, link = %s, img_src = %s, office = %s, ranking = %s, update_date = %s WHERE link = %s"
#         val = (item.title, item.link, item.img_src, item.office, item.ranking, datetime.datetime.now(), item.link)
#         curs.execute(sql, val)
#         conn.commit()
#     else:
#         print "insert"
#         sql = "INSERT INTO article (title, link, img_src, office, registe_date, update_date, ranking) VALUES (%s, %s, %s, %s, %s, %s, %s)"
#         curs.execute(sql, item)
#         conn.commit()
conn.close()

# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import datetime
from secretModule import dbconfig
from collections import namedtuple


# soup 만들어내는 함수
def get_soup(url):
    return BeautifulSoup(requests.get(url).text, 'html.parser')


# 크롤링 함수
def get_crawling_data():
    todayDate = datetime.datetime.today().strftime('%Y%m%d')
    currentArticleUrl = 'https://news.naver.com/main/ranking/popularDay.nhn?rankingType=popular_day&sectionId=100&date='+todayDate
    articleListSoup = get_soup(currentArticleUrl)
    crawling_data = []
    articleList = articleListSoup.find("ol", {"class": "ranking_list"})
    for i in range(1, 31):
        article = articleList.find("li", {"class": "ranking_item is_num" + str(i)})
        title = article.a.get('title')
        detailUrl = str("https://news.naver.com" + article.a.get('href'))
        office = article.find('div', {"class": "ranking_office"}).text
        ranking = i
        img_src = article.img.get('src') if not (article.img is None) else ''

        # 기사 상세보기 들어가서 찾는 정보
        articleDetailSoup = get_soup(detailUrl)
        realArticleUrl = articleDetailSoup.find("div", {"class": "sponsor"}).a.get('href')

        articleDetailBody = articleDetailSoup.find('div', {"id": "articleBodyContents"})
        # TODO 디비에 넣기
        print articleDetailBody.text

        fields = 'title link img_src office registe_date update_date ranking'
        crawlingTuple = namedtuple('crawlingTuple', fields)
        crawling_data.append(
            crawlingTuple(title, realArticleUrl, img_src, office, datetime.datetime.now(), datetime.datetime.now(),
                          ranking))
    return crawling_data


# 디비 페치 함수
def get_fetched_data(cursor):
    sql = "select t.id, t.link from crawling.article t " \
          "inner join (" \
          "select ranking, max(update_date) as MaxDate, max(id) as MaxId from crawling.article group by ranking" \
          ") tm on t.ranking = tm.ranking and t.update_date = tm.MaxDate and  t.id = tm.MaxId order by t.ranking;"

    cursor.execute(sql)

    fetchedTuple = namedtuple('fetchedTuple', 'id link')
    fetched_data = []
    for result in cursor.fetchall():
        result = fetchedTuple(result[0], result[1])
        fetched_data.append(result)
    return fetched_data


def get_intersection_id(x):
    if x.link in list(map(lambda y: y.link, crawlingData)):
        return {x.link: x.id}


conn = dbconfig.dbConnect()
curs = conn.cursor()

crawlingData = get_crawling_data()
fetchedData = get_fetched_data(curs)

# fetch 를 통해 가져온것과 크롤링 해온 것 중복 검사 -> fetchedData vs crawlingData의 link 이용.
# link, id keyValue 쌍 이용
dictList = list(filter(lambda x: x is not None, list(map(get_intersection_id, fetchedData))))
idLinkDic = {key: value for d in dictList for key, value in d.items()}

for item in crawlingData:
    if item.link in idLinkDic.keys():
        articleId = idLinkDic[item.link]
        print "update"
        sql = "UPDATE article SET title = %s, link = %s, img_src = %s, office = %s, ranking = %s, update_date = %s WHERE id = %s"
        val = (item.title, item.link, item.img_src, item.office, item.ranking, datetime.datetime.now(), articleId)
        curs.execute(sql, val)
        conn.commit()
    else:
        print "insert"
        sql = "INSERT INTO article (title, link, img_src, office, registe_date, update_date, ranking) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        curs.execute(sql, item)
        conn.commit()
conn.close()

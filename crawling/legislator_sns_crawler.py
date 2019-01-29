# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from secretModule import dbconfig

allLegislator = []
finalData = []

for i in range(1, 11):
    req = requests.get(
        'http://watch.peoplepower21.org/?mid=AssemblyMembers&mode=search&party=&region=&sangim=&gender=&elect_num'
        '=&page=' + str(i))
    html = req.text
    soup = BeautifulSoup(html, 'html.parser')
    for item in soup.findAll("div", {"class": "col-xs-6 col-sm-3 col-md-2"}):
        allLegislator.append((item.h4.text.split()[1], item.a.get('href')))

for legislator in allLegislator:
    req = requests.get('http://watch.peoplepower21.org' + legislator[1])
    html = req.text
    soup = BeautifulSoup(html, 'html.parser')
    snsList = []
    print(legislator[0])
    for item in soup.findAll("a", {"class": "btn-kso btn-primary"}):
        snsList.append(item.get('href'))
    finalData.append((legislator[0], snsList))

val = []
for item in finalData:
    fb = ""
    twitter = ""
    blog = ""
    for sns in item[1]:
        if "facebook" in sns:
            fb = sns
        elif "twitter" in sns:
            twitter = sns
        else:
            blog = sns
    val.append((item[0], blog, twitter, fb))


# MySQL Connection 연결
conn = dbconfig.dbConnect()

# Connection 으로부터 Cursor 생성
curs = conn.cursor()

# SQL문 실행
sql = "INSERT INTO legislator (l_name, blog, twitter, fb) VALUES (%s, %s, %s, %s)"

curs.executemany(sql, val)

# connection is not autocommit by default. So you must commit to save your changes.
conn.commit()

# Connection 닫기
conn.close()

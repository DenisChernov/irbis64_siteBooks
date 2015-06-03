#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'demiin'
import socket
import datetime
import re
import psycopg2
import requests
import time

CURDATE = "201503"

regex_autor    = re.compile("\|(.*)&&&")
regex_autor_f    = re.compile("#(.*)\|")
regex_bookname = re.compile("&&&(.*)###")
regex_isbn     = re.compile("###(.*)~~~")
regex_filials  = re.compile("~~~(.*)$")
regex_fullCover = re.compile("eMicroGallery_fullImage\" src=\"\\/\\/([a-z0-9\\.\\/_]*)")
regex_annotation = re.compile("<!-- Data\\[ANNOTATION\\] -->(.*)<\\/td>")
regex_manyResults = re.compile("\\A  <div itemprop=\"itemListElement\".*data-omniture-suffix=\"pic\" href=\"\\/(context\\/detail\\/id\\/[0-9\\/\\._]*)\".*")
regex_manyanswers = re.compile("(tilesTotalCount)")
regex_firstInMany = re.compile("data-omniture-suffix=\"pic\" href=\"([0-9a-zA-Z\/]*)\">")

counter = 1;
bases = ['OLDEK', 'RETRO', 'CKC', 'RDR']
SERVER = "192.168.9.20"
log = open("/tmp/site_newbooks", "w")
log.write(str(datetime.datetime.now()) + "\n")
### First step - connect   ####################
msg_connect = "\nA\nC\nA\n31771\n" + str(counter) + "\n" + "9f9@7Nuq\nirbisoft\n\n\n\n" + "irbisoft\n9f9@7Nuq"
msg_connect = str(len(msg_connect) - 1) + msg_connect
sock = socket.socket()
sock.connect((SERVER, 6666))
sock.send(msg_connect)
data = sock.recv(64000)
sock.close()
print("connected")
log.write("connected to: " + SERVER + "\n")


counter += 1
#msg_list = "\nK\nC\nK\n31771\n" + str(counter) + "\n" + "9f9@7Nuq\nirbisoft\n\n\n\nOLDEK\n\n10000\n1\nmpl,if p(v700) then v700^A' ' v700^g else if p(v961) then v961^a' 'v961^b else '-|-' fi fi'&&&'if p(v461) then v461^c'('v461^e') 'v200^T': 'v200^a else if p(v200) then v200^a fi fi'###'&uf('Av10^a#1')'~~~'(if p(v910^d) then v910^D';' fi)\n0\n0\n!if v910^c.6='201502' then '1' else '0' fi"
msg_list = "\nK\nC\nK\n31771\n" + str(counter) + "\n" + "9f9@7Nuq\nirbisoft\n\n\n\nOLDEK\n\n10000\n1\nmpl,if p(v700) then v700^a'|'v700^A' ' v700^g else if p(v961) then v961^a'|'v961^a' 'v961^b else '-' fi fi'&&&'if p(v461) then v461^c else if p(v200) then v200^a fi fi'###'&uf('Av10^a#1')'~~~'(if p(v910^d) then v910^D';' fi)\n0\n0\n!if v910^c.6='" + CURDATE + "' then '1' else '0' fi"
msg_list = str(len(msg_list) - 1) + msg_list
sock = socket.socket()
sock.connect((SERVER, 6666))
sock.send(msg_list)

data = ""
buf = "1"
while buf != "":
    buf = sock.recv(1024)
    if buf != "":
        data += buf

sock.close()
log.write("list books created\n")
print("Список книг сформирован")
data = data.split("\n")

books = []
for book in data:
    if "#" in book:
        books.append(book)

def getBookinfo(book):
    print("Получаю данные о " + book)
    url = "http://www.ozon.ru/?context=search&text=" + book
    print (url)
    result = []
    try:
        req = requests.request('GET', url, timeout=20)
        print("Ответ: " + str(req.status_code) + '\n')
        result_manyanswers = re.findall(regex_manyanswers, req.text)
        if result_manyanswers != []:
            print ("Много вариантов")
            result_firstInMany = re.findall(regex_firstInMany, req.text)
            if result_firstInMany != []:
                url = "http://ozon.ru" + result_firstInMany[1]
                print("Новый адрес перехода: ")
                print(url)
                req = requests.request('GET', url, timeout=20)
        result_cover = re.findall(regex_fullCover, req.text)
        if result_cover != []:
            result.append(result_cover[0])
            print ("http://ozon.ru/" + result_cover[0])
        else:
            result.append(" ")
        result_desc = re.findall(regex_annotation, req.text)
        if result_desc != []:
            result.append(result_desc[0])
        else:
            result.append(" ")
    except:
        print("timeout")
        result.append("")
        result.append("")
    time.sleep(5)

    return result

# Добавляем книги в БД
try:
    conn = psycopg2.connect("dbname='site_books' user='oa' host='192.168.9.250' password='oa'")
#    conn = psycopg2.connect("dbname='site_books' user='oa' host='192.168.6.12' password='oa'")
except:
    print ("Ошибка подключения к серверу БД")
cur = conn.cursor()
cur.execute("TRUNCATE TABLE books")
cur.execute("TRUNCATE TABLE url_books")

for book in books:
	print (book)
	result_bookname = re.findall(regex_bookname, book)
	result_autor    = re.findall(regex_autor, book)
	result_autor_f  = re.findall(regex_autor_f, book)
	result_filials  = re.findall(regex_filials, book)
	result_isbn     = re.findall(regex_isbn, book)
#    print (result_autor[0] + ": " + result_bookname[0] + " --> " + result_filials[0])
	if result_autor != []:
		if result_autor[0] != "-":
			result = getBookinfo(result_autor[0] + " " + result_bookname[0])
		else:
			result = getBookinfo(result_isbn[0] + " " + result_bookname[0])
	else:
		result = getBookinfo(result_isbn[0] + " " + result_bookname[0])
	if result[0] == "":
		print("Уменьшаем количество параметров поиска")
		if result_autor_f[0] == "":
			result_autor_f[0] = " "
		result = getBookinfo(result_autor_f[0] + " " + result_bookname[0])

#	print(result_autor[0])
#	print(result_bookname[0])
#	p
	try:
		cur.execute("INSERT INTO books VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (result_autor[0], result_bookname[0], result_filials[0][0:len(result_filials[0])-2], 0, result_isbn[0], result[0], result[1], CURDATE))
		conn.commit()
	except:
		print("Ошибка добавления в базу. Возможно не все распарсилось")
counter += 1
### Last step - disconnect   ################
msg_disconnect = "\nB\nA\nB\n31771\n" + str(counter) + "\n" + "9f9@7Nuq\nirbisoft\n\n\n\n" + "irbisoft\n9f9@7Nuq"
msg_disconnect = str(len(msg_disconnect) - 1) + msg_disconnect
sock = socket.socket()
sock.connect((SERVER, 6666))
sock.send(msg_disconnect)
data = sock.recv(64000)
sock.close()
print ("disconnect")
#print("Всего добавлено: ")
#print(totalBooksInserted)
#print("\n")
#print("Всего пропущено: ")
#print(totalBooksSkipped)

log.write("disconnect" + "\n")
log.close()

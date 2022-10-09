#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

import sys
import getopt
import configparser
import pymysql
import traceback
import requests
import random
import time
import datetime
import os
import hashlib
from urllib.request import urlparse, urlunparse, urljoin
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

CONFIG_FILE = 'init.cnf'

# добавление url в таблицу `log` базы данных
def db_log_insert(db, url, status, commit=True):
    db_cursor = db.cursor()
    sql = "insert into `log` (url, status) values (%s, %s);"
    try:
        db_cursor.execute(sql, (url, status))
        if commit:
            db.commit()
    except Exception as e:
        db.close()
        error("Mysql query failed!\n %s\n%s" % (e,sql))

# обновление статуса url в таблице `log` базы данных
def db_log_update(db, url, status, commit=True):
    db_cursor = db.cursor()
    sql = "update `log` set status = %s where url = %s;" 
    try:
        db_cursor.execute(sql, (status, url))
        if commit:
            db.commit()
    except Exception as e:
        db.close()
        error("Mysql query failed!\n %s\n%s" % (e,sql))

# прощание с пользователем и завершение работы
def bye():
    print("Bye.")
    exit(0)

# вывод сообщения об ошибке и завершение работы
def error(msg):
    func_name = traceback.extract_stack(None, 2)[0][2]
    print()
    print('Function: ' + func_name)
    print('ERROR: ' + msg)
    bye()

# вывод подсказки по аргументам командной строки и завершение работы
def help(default_options):
    print('Usage: ')
    print('%s <url> <database> [ options ]\n' % sys.argv[0])
    print('possible option:         | default value (auto / %s):' % CONFIG_FILE)
    print('  -n <domen>             | auto from url')
    print('  -d <delay, ms>         | %d,' % default_options['delay'])
    print('  -r <random_delay, ms>  | %d,' % default_options['random'])
    print('  -i <ignore_file>       | %s,' % default_options['ignorefile'])
    exit(0)

# перенаправление вывода потоков в файл
def redirect_stream_to_file(stream, filename):
    f = open(filename, 'a')
    OUT = stream.fileno()
    new_fd = os.dup(OUT)
    os.fdopen(new_fd, 'w')
    os.dup2(f.fileno(), OUT)
    f.close()
    
# загрузка игнорирумых url-ов сайта (файлы, документы, корзина, информация и т.п.)
def load_ignore_urls_config(ignore_file):
    ignore = {'files': [], 'keywords': []}
    regim = None
    try:
        with open(ignore_file, 'r') as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                elif line == '[files]':
                    regim = 'files'
                elif line == '[keywords]':
                    regim = 'keywords'
                else:
                    if regim == 'files':
                        ignore['files'].extend(line.split())
                    elif regim == 'keywords':
                        ignore['keywords'].extend(line.split())
    except IOError:
        error("can't read ignore file '%s'!" % ignore_file)
    return ignore 

# проверяет необходимость игнорирования данного url 
def ignorable(url, ignore_config):
    if url is None:
        return True 
    if url.find('.')!=-1:
        ext = url.split('.')[-1]
        if ext.lower() in ignore_config['files']:
            return True 
    for ignore_word in ignore_config['keywords']:
        if url.find(ignore_word)!=-1:
            return True 
    return False
    
# печать опций с которыми запущена программа
def show_config(config):
    print('PID: \t\t%s' % os.getpid())
    options = config['options']
    print('Url:\t\t%s' % options['url'])
    print('Domen:\t\t%s' % options['domen'])
    print('Delay:\t\t%d ms' % options['delay'])
    print('Random:\t\t%s ms' % options['random'])
    print('Database:\t%s' % options['database'])
    print('Ignore urls:\t%s' % options['ignorefile'])

    # печать игнорируемых url-ов
    if options['ignorefile']:
        ignore_urls = load_ignore_urls_config(options['ignorefile'])
        print('[')
        print('    Files:')
        print('    ' + ', '.join([e for e in ignore_urls['files']]))
        print()
        print('    Keywords:')
        for k in ignore_urls['keywords']:
            print('    ' + k)
        print(']')
    print()

# загрузка конфигурационного файла
def load_config():
    config = {
        'mysql': {
            'host'     : None,
            'username' : None,
            'password' : None,
            'charset'  : None
        },
        'options': {
            'delay'      : None,
            'random'     : None,
            'ignorefile' : None 
        }
    }

    config_parser = configparser.ConfigParser()
    try:
        config_parser.read(CONFIG_FILE)
    except:
        error("can't read config file %s!", CONFIG_FILE)

    for section in config:
        for option in config[section]:
            try:
                config[section][option] = config_parser.get(section, option) 
            except:
                error("can't read section '[%s]' -> option '%s' from config file '%s'!" % (section, option, CONFIG_FILE))
    return config 

# проверка существования базы
def check_db_exists(db_cursor, db_name):
    sql = "show databases;"
    try:
        db_cursor.execute(sql)
    except Exception as e:
        error("Mysql query failed!\n %s" % e)

    exist_databases = set([i for r in db_cursor for i in r])
    if db_name in exist_databases:
        return True
    return False

# создание новой базы данных и таблицы pages 
def create_database(config):
    mysql = config['mysql']
    try:    
        db = pymysql.connect(host=mysql['host'],
                             user=mysql['username'],
                             passwd=mysql['password'],
                             charset=mysql['charset'])
    except pymysql.err.OperationalError as e:
        error("Mysql connection failed!\n %s" % e)
        
    db_name = config['options']['database']

    db_cursor = db.cursor()
    if check_db_exists(db_cursor, db_name):
        db.close()
        error("Database '%s' is already exists!" % db_name)

    sql1 = "create database %s" % db_name 
    sql2 = "use %s" % db_name 
    sql3 = "create table `pages` (      \
        `id` int(11) not null,          \
        `url` text not null,            \
        `level` int(11) not null,       \
        `parent_id` int(11) not null,   \
        `code` longtext not null,       \
        primary key(`id`)               \
    ) character set utf8 collate utf8_unicode_ci;"
    sql4 = "create table `log` (               \
        `id` int(11) not null auto_increment,  \
        `url` text not null,                   \
        `status` text,                         \
        primary key(`id`)                      \
    ) character set utf8 collate utf8_unicode_ci;"
    try:
        db_cursor.execute(sql1)
        db_cursor.execute(sql2)
        db_cursor.execute(sql3)
        db_cursor.execute(sql4)
        db.commit()
    except Exception as e:
        db.close()
        error("Mysql query failed!\n %s" % e)
    
    return db

# Глобальные переменные (c ними работают функции, описанные ниже)
config = dict()
page_id_counter = int()

pages_to_parse = list() # список словарей c ключами id, url, level, parent_id

taken_urls = set()
skipped_urls = set()

# Добавление кода страницы в таблицу pages
def save_page(page, code, database):    
    print('\t\tsave_page_code')
    page_code_hash = hashlib.sha256(str.encode(code)).hexdigest() 
    try:
        db_cursor = database.cursor()
        db_cursor.execute("insert into `pages` (id, url, level, parent_id, code) values (%s, %s, %s, %s, %s);", 
                             (page['id'], page['url'], page['level'], page['parent_id'], page_code_hash))
        database.commit()
    except Exception as e:
        print('Error!')
        print(e)

# Получение абсолютного адреса ссылки по относительному адресу ссылки
# и адресу страницы, где ссылка найдена. 
# Также следит, чтобы домен имел префикс 'www',
# при необходимости добавляет его в название домена!
def abs_ref(href, current_url=None):
    if current_url:
        href = urljoin(href, current_url)
#   if not "www." in href:
#       u = urlparse(href)
#       href = urlunparse((u.scheme, "www." + u.netloc, u.path, u.params, u.query, u.fragment))
    return href

# Удаляем часть ссылки на фрагмент страницы,
# например page.html#footer -> page.html
def remove_fragments_refs(href):
   index = href.find('#')
   return href[:index]

# Сбор ссылок на другие веб-страницы
def collect_hrefs(page, code, db):
    print("    collect hrefs: ", end='')
    ignore_file = config['options']['ignorefile']
    ignore_conf = load_ignore_urls_config(ignore_file) 
    
    soup = BeautifulSoup(code, 'lxml')
    if not soup:
        soup = BeautifulSoup(code, 'html5lib')
    if not soup:
        print("PARSE ERROR: Can't parse page (no soup)!")
        return

    collect = soup.find('html') 
    a_tags_on_page = soup.findAll('a')

    links_on_page = []
    for link in a_tags_on_page:
        rel = link.get('rel')
        if not rel or rel!="nofollow":
            href = link.get('href')
            if href:
                if not ignorable(href, ignore_conf):
                    try:
                        href = abs_ref(page['url'], href)      # получаем абсолютный url ссылки 
                        domen = config['options']['domen']
                        href_domen = urlparse(href).netloc
                        if href_domen == domen:                # внутренняя ссылка сайта
                            links_on_page.append(href)
                        else:
                            skipped_urls.add(href)             # пропускаем внешнюю ссылку
                            db_log_insert(db, href, 'extern')
                    except Exception as e:
                        print(e)
                else:
                    skipped_urls.add(href)
                    db_log_insert(db, href, 'ignored')

    taken_urls.add(page['url'])

    new_urls_count = 0
    for u in links_on_page:
        if u not in taken_urls:
            p = {'id'        : None,
                 'url'       : u, 
                 'level'     : page['level'] + 1,
                 'parent_id' : page['id']}
            pages_to_parse.append(p)
            new_urls_count += 1
            taken_urls.add(u)
            db_log_insert(db, u, 'process', commit=False)

    db.commit()

    print('found %d urls on page, %d new local urls' % (len(links_on_page), new_urls_count))

# обработка сайта в соответствии с настройками пользователя 
def work():
    db = create_database(config)

    global page_id_counter
    page_id_counter = 0 

    global pages_to_parse
    pages_to_parse = [{'id'        : None,
                       'url'       : config['options']['url'],
                       'level'     : 0,
                       'parent_id' : -1}]

    ua = UserAgent()    
    header = {'User-Agent': str(ua.firefox)}


    # обход сайта
    while pages_to_parse:
        page = pages_to_parse.pop(0) # для того, чтобы обход был по возрастанию уровней выбираем первый, а не последний элемент списка
        url = page['url']
        level = page['level']
        print(datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), end='\t')
        print('%d %s ' % (level, url))
        try:
            # пытаемся определить тип контента url, если возможно
            request = requests.get(url, allow_redirects=True, headers=header) 
            content_type = request.headers.get('content-type', None)
            if content_type and 'text/html' in content_type:
                request = requests.get(url, headers=header) 
                if request.ok:
                    request.encoding = "utf-8"
                    html_doc = request.text
                    try:
                        page['id'] = page_id_counter
                        collect_hrefs(page, html_doc, db) # db needs only for write db_log
                        save_page(page, html_doc, db)
                        db_log_update(db, url, 'ok')
                        page_id_counter += 1
                    except Exception as e:
                        print('Error\n', e)
                else:
                    print('\tError: status %s' % (request.status_code))
                    db_log_update(db, url, request.status_code)
            else:
                skipped_urls.add(url)
                print("\tSkip '%s' - not html content" % url);
                db_log_update(db, url, 'not_html')

            # задержка между запросами к сайту
            delay = config['options']['delay'] + random.random() * config['options']['random'] # мс
            time.sleep(delay/1000)

        except requests.exceptions.RequestException as e:
            print(e)
    
    db.close()

# подготовка к работе: чтение конфига, аргументов командной строки, запуск 
def main(argv):
    global config
    config = load_config()

    options = config['options']
    options['delay'] = int(options['delay'])
    options['random'] = int(options['random'])

    if len(argv)<3:
        help(options)

    url = argv[1]
    url = abs_ref(url)
    options['url'] = url 
    
    options['database'] = argv[2]

    domen = urlparse(url).netloc
    options['domen'] = domen 

    if len(argv)>3:
        try:
            opts, args = getopt.getopt(argv[3:], 'n:d:r:i:')
        except getopt.GetoptError:
            help(options)
        
        if not opts:
            help(options)
        
        for opt, arg in opts:
            if opt == '-n':
                options['domen'] = arg
            elif opt == '-d':
                options['delay'] = int(arg)
            elif opt == '-r':
                options['random'] = int(arg)
            elif opt == '-i':
                options['ignore'] = arg

    config['options'] = options
    show_config(config) 
    try:
        work() # основная работа программы
    except Exception as e:
        print(e)

    print("All done.")

# вход в главную функцию программу
if __name__ == '__main__':
    main(sys.argv)

# -*- coding: utf-8 -*-
"find all tweets mentioning 'brexit' in the last day and store them in DB"
import sys
import datetime
import time
import json
import requests
import MySQLdb
import urllib3
import urllib3.contrib.pyopenssl
from config import Config
from unidecode import unidecode

class Tweet(object):
    'a tweet'
    def __init__(self, t_author, t_created, t_created_l, t_id_link, t_link, t_message, t_type,
                 t_isrt, t_isrep, t_rts, t_favs, t_isqrt):
        self.author = t_author
        self.created = t_created
        self.created_local = t_created_l
        self.id_link = t_id_link
        self.link = t_link
        self.message = t_message
        self.type = t_type
        self.isrt = t_isrt
        self.isrep = t_isrep
        self.rts = t_rts
        self.favs = t_favs
        self.isqrt = t_isqrt

def sql_insert(command):
    'Tries to execute sql insert command'
    try:
        CURSOR.execute(command)
        DB.commit()
    except Exception as EXC:
        DB.rollback()

def sql_log(lrecord):
    'Inserts a record into the log'
    lcommand = "insert into post_log (record) values ('%s')" % (lrecord)
    print lrecord
    sql_insert(lcommand)

def runtime(exctype, value, trb):
    'reroutes output to log'
    sql_log(DB.escape_string(repr(exctype)))
    sql_log('Value:'+value.message)
    sql_log('Traceback line:'+str(trb.tb_lineno))

def media_type(tweet, t_link):
    '''determines tweet's type'''
    if "media" in tweet['entities']:
        t_type = "photo"
    elif t_link == "":
        t_type = "text"
    elif t_link != "":
        if tweet['entities']['urls'][0]['display_url'].startswith("amp.twimg.com/v"):
            t_type = "video"
        else:
            t_type = "text"
    else:
        t_type = "text"
    return t_type

def rt_link_type(tweet):
    '''determine if tweet is a retweet and what it's link is'''
    if "quoted_status" in tweet:    #if it's a retweet w/quote
        isrt_ = 1
        isqrt_ = 1
        rts_ = tweet['retweet_count']
        if tweet['quoted_status']['entities']['urls'] == []:
            link_ = ""
        else:
            link_ = tweet['quoted_status']['entities']['urls'][0]['expanded_url']
        t_type_ = media_type(tweet, link_)
    else:
        isqrt_ = 0
        if "retweeted_status" in tweet:    #if it's a reg retweet
            isrt_ = 1
            rts_ = 0
            if "quoted_status" in tweet['retweeted_status']:
            #if it's a RT of quoted RT
                if tweet['retweeted_status']['quoted_status'] \
                        ['entities']['urls'] == []:
                    link_ = ""
                else:
                    link_ = tweet['retweeted_status']['quoted_status'] \
                            ['entities']['urls'][0]['expanded_url']
                t_type_ = media_type(tweet, link_)
            else:   #if it's a RT of a non-quoted RT
                if tweet['entities']['urls'] == []:
                    link_ = ""
                else:
                    link_ = tweet['entities']['urls'][0]['expanded_url']
                t_type_ = media_type(tweet, link_)
        else:   #if it's not a retweet
            isrt_ = 0
            rts_ = tweet['retweet_count']
            if tweet['entities']['urls'] == []:
                link_ = ""
            else:
                link_ = tweet['entities']['urls'][0]['expanded_url']
            t_type_ = media_type(tweet, link_)
    return [isrt_, rts_, link_, t_type_, isqrt_]

def get_tweets(max_id_):
    '''fetch tweets mentioning brexit'''
    twapi = "https://api.twitter.com/1.1/search/tweets.json"
    while True:
        list_of_tweets = []
        if max_id_ == 0:
            myparams = {'q' : 'brexit', 'count' : 100, 'result_type' : 'recent', 'until' :TODAY}
        else:
            myparams = {'q' : 'brexit', 'count' : 100, 'result_type' : 'recent', 'until' :TODAY, \
                        'max_id' : max_id_}
        myheaders = {'Authorization' : "Bearer "+TW_ACCESS_TOKEN}
        try:
            timeline = requests.get(twapi, params=myparams, headers=myheaders)
        except Exception as Exc:
            sql_log(Exc)
            sql_log("about to hit TWITTER rate limit")
            time.sleep(60)
        try:
            timelined = json.loads(timeline.text)
        except ValueError:
            sql_log("JSON Couldn't be decoded, trying request again")
            continue
        try:
            rate_limit = int(timeline.headers['x-rate-limit-remaining'])
        except Exception as Exc:
            sql_log(Exc)
            sql_log("Rate limit problem 1")
        else:
            print rate_limit
            if (rate_limit) == 0:
                time.sleep(15*60)
                continue
            elif 0 < (rate_limit) <= 3:
                sql_log("Waiting for rate limit")
                time.sleep(15*60)
        if 'statuses' not in timelined:
            sql_log("Failed to get statuses, waiting 1 min and sending request again")
            time.sleep(60)
            continue
        for tweet in range(len(timelined['statuses'])):
            tweet_time = datetime.datetime.strptime(timelined['statuses'][tweet]['created_at'] \
                                                   , "%a %b %d %H:%M:%S +0000 %Y")
            if tweet_time > FROM_ and tweet_time < UNTIL:
                author = timelined['statuses'][tweet]['user']['screen_name']
                created = datetime.datetime.strftime(tweet_time, "%Y-%m-%d %H:%M:%S")
                created_local = tweet_time + datetime.timedelta(hours=1)
                t_id = timelined['statuses'][tweet]['id_str']
                t_id_link_c = "http://www.twitter.com/"+author+"/status/"+t_id
                t_id_link = DB.escape_string(t_id_link_c)
                favs = timelined['statuses'][tweet]['favorite_count']
                t_attr_list = rt_link_type(timelined['statuses'][tweet])
                isrt = t_attr_list[0]
                rts = t_attr_list[1]
                link = t_attr_list[2]
                t_type = t_attr_list[3]
                isqrt = t_attr_list[4]
                if timelined['statuses'][tweet]['text'] == []:   #is tweet text empty?
                    message = ""
                else:
                    message = unidecode(timelined['statuses'][tweet]['text']).replace('\\', '')
                    if message.startswith('@'):
                        isrep = 1
                    else:
                        isrep = 0
                list_of_tweets.append(Tweet(author, created, created_local, t_id_link, link, message, t_type,
                                            isrt, isrep, rts, favs, isqrt))
        n_returned_tw = len(timelined['statuses'])
        if n_returned_tw > 0:
            last_returned_tw = timelined['statuses'][n_returned_tw - 1]
            max_id_tw = last_returned_tw['id'] - 1
        else:
            continue
        print datetime.datetime.strptime(last_returned_tw['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        if datetime.datetime.strptime(last_returned_tw['created_at'] \
                                          , "%a %b %d %H:%M:%S +0000 %Y") \
                                          < FROM_:
            insert_tweets(list_of_tweets)
            del list_of_tweets[:]
            timelined.clear()
            break
        else:
            insert_tweets(list_of_tweets)
            timelined.clear()
            del list_of_tweets[:]
            max_id_ = max_id_tw

def insert_tweets(list_of_tweets_):
    for item in list_of_tweets_:
        insert_tweet = "insert ignore into alltweets (author, created, created_local, id_link, \
                        link, message, type, isrt, isrep, rts, favs, isqrt) values \
                        ('%s', '%s', '%s', '%s', '%s', \
                        '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                        % (item.author, item.created, item.created_local, item.id_link, item.link,
                           DB.escape_string(item.message),
                           item.type, item.isrt, item.isrep, item.rts, item.favs,
                           item.isqrt)
        sql_insert(insert_tweet)
    sql_log("Inserted tweets")

def clear_nonbrexit():
    '''delete tweets that don't mention brexit in the message
    (this can happen because of how twitter does searches)'''
    clearsql = r"delete from brexit.alltweets where message not like '%brexit%' "
    sql_insert(clearsql)
    sql_log("Non brexit tweets cleared")

def main():
    #insert script started into db
    sql_log("Script has started")

    #get tweets from twitter
    get_tweets(0)

    sql_log("Finished tweet retreival from Twitter")

    clear_nonbrexit()

    sql_log("Script has succeeded")



#sys.excepthook = runtime
sys.setrecursionlimit(10000)

#for ssl verif
requests.packages.urllib3.disable_warnings()
urllib3.contrib.pyopenssl.inject_into_urllib3()

#database connection
MYSQL_FILE = file('mysqlbrexit2.cfg')
MYSQL_CONF = Config(MYSQL_FILE)

DB = MySQLdb.connect(MYSQL_CONF.db_address,
                     MYSQL_CONF.db_user,
                     MYSQL_CONF.db_password,
                     MYSQL_CONF.db_name,
                     charset='utf8',
                     init_command='SET NAMES UTF8',
                     use_unicode=True)

#prepare CURSOR
CURSOR = DB.cursor()

#time variables
NOW = datetime.datetime.utcnow()
CURRENT_DATETIME = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
ONE_DAY = datetime.timedelta(days=1)
YESTERDAY = (NOW - ONE_DAY)
YESTERDAY_STR_FORMAT = (datetime.datetime.utcnow()-ONE_DAY).strftime('%Y-%m-%d')
DAY_BEFORE_YEST = YESTERDAY - ONE_DAY
TODAY = (datetime.datetime.utcnow()).strftime('%Y-%m-%d')
FROM_ = DAY_BEFORE_YEST.replace(hour=23, minute=0, second=0, microsecond=0)
UNTIL = YESTERDAY.replace(hour=23, minute=0, second=0, microsecond=0)
WEEKAGO = (NOW - datetime.timedelta(days=8)).strftime('%Y-%m-%d')

#api variables and connection
TW_ACCESS_TOKEN = "***" #should be encrypted

main()



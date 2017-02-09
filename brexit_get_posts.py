# -*- coding: utf-8 -*-
"""Get today's facebook posts and tweets from a specified list of users
and put resulting posts and tweets in db. Also update posts made in the last
week with new engagement numbers (likes, retweets, etc)"""
import re
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
    '''a tweet'''
    def __init__(self, t_iid, t_author, t_created, t_id_link, t_link, t_message,
                 t_type, t_isrt, t_isrep, t_rts, t_favs, t_brexit, t_isqrt):
        self.id = t_iid
        self.author = t_author
        self.created = t_created
        self.id_link = t_id_link
        self.link = t_link
        self.message = t_message
        self.type = t_type
        self.isrt = t_isrt
        self.isrep = t_isrep
        self.rts = t_rts
        self.favs = t_favs
        self.brexit = t_brexit
        self.isqrt = t_isqrt

def chunks(l, n):
    '''split a list into multiple chunks with n items'''
    n = max(1, n)
    return [l[j:j + n] for j in range(0, len(l), n)]

def sql_insert(command):
    '''Tries to execute sql insert command'''
    try:
        CURSOR.execute(command)
        DB.commit()
    except Exception as EXC:
        DB.rollback()

def sql_log(lrecord):
    '''Inserts a record into the log'''
    lcommand = "insert into post_log (record) values ('%s')" % (lrecord)
    print lrecord
    sql_insert(lcommand)

def runtime(exctype, value, trb):
    '''reroutes output to log'''
    sql_log(DB.escape_string(repr(exctype)))
    sql_log('Value:'+value.message)
    sql_log('Traceback line:'+str(trb.tb_lineno))

def get_fb_posts(fusername):
    '''retreives a user's recent posts'''
    try:
        fbpostr = requests.get(GRAPH+
                               fusername[0]+
                               FB_POST_HTTP+
                               "&until="+TODAY+
                               "&since="+YESTERDAY_STR_FORMAT+
                               FB_ACCESS_TOKEN,
                               verify=False)
    except Exception as e:
        sql_log(e)
        sql_log("FB post request error for ==> "+fusername[0])
        FB_MISSED_LIST.append(fusername[0])
        return []
    else:
        return process_fb_posts(fusername, fbpostr)

def process_fb_posts(fusername, fbpostr):
    '''creates dictionary for each post, adds to user's list of posts, then adds to DB'''
    users_posts_ = []
    sql_log("FB post counting ==> "+fusername[0])
    fbpostrd = json.loads(fbpostr.text)
    if "error" in fbpostrd:
        sql_log("FB post error for ==> "+fusername[0])
        FB_MISSED_LIST.append(fusername[0])
        return []
    if fbpostrd['data'] == []:
        return []
    else:
        for post in range(len(fbpostrd['data'])):
            post_dict = {'id' : fusername[1], 'author' : fusername[0]}
            created_time_o = datetime.datetime.strptime(fbpostrd['data'][post]['created_time'],
                                                        "%Y-%m-%dT%H:%M:%S+0000")
            post_dict['created_time'] = datetime.datetime.strftime(created_time_o,\
                                                                  "%Y-%m-%d %H:%M:%S")
            post_id = fbpostrd['data'][post]['id']
            post_id_list = post_id.split('_')
            post_id_link_c = "http://www.facebook.com/"+post_id_list[0]+"/posts/"\
                +post_id_list[1]
            post_dict['id_link'] = DB.escape_string(post_id_link_c)
            post_dict['status_type'] = fbpostrd['data'][post]['status_type']
            post_dict['likes'] = fbpostrd['data'][post]['likes']['summary']['total_count']
            post_dict['comments'] = \
                fbpostrd['data'][post]['comments']['summary']['total_count']
            if "message" in fbpostrd['data'][post]:
                post_dict['message'] = unidecode(fbpostrd['data'][post]['message'])
            else:
                post_dict['message'] = ""
            if "shares" in fbpostrd['data'][post]:
                post_dict['shares'] = fbpostrd['data'][post]['shares']['count']
            else:
                post_dict['shares'] = 0
            if "link" in fbpostrd['data'][post]:
                post_dict['link'] = fbpostrd['data'][post]['link']
            else:
                post_dict['link'] = ""
            if "name" in fbpostrd['data'][post]:
                post_dict['link_name'] = unidecode(fbpostrd['data'][post]['name'])
            else:
                post_dict['link_name'] = ""
            if "description" in fbpostrd['data'][post]:
                post_dict['description'] = unidecode(fbpostrd['data'][post]['description'])
            else:
                post_dict['description'] = ""
            post_dict['brexit'] = is_brexit(post_dict['message'], post_dict['link_name'], \
                post_dict['description'])
            users_posts_.append(post_dict)
    return users_posts_


def is_brexit(message, link_name, description):
    '''determine if particular post is related to the brexit'''
    if any(r.search(message) for r in INCLUDED) or any(r.search(link_name) for r in INCLUDED) or\
        any(r.search(description) for r in INCLUDED):
        return 1
    else:
        return 0

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
    '''determine if tweet is a retweet and what its link is'''
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

def get_user_tweets(username, max_id_):
    '''fetch a user's tweets'''
    sql_log("Tweet data ==> %s" % (username[0]))
    twapi = "https://api.twitter.com/1.1/statuses/user_timeline.json"
    if max_id_ == 0:    #when max id is 0, this means it's the first API call for that user
        myparams = {'screen_name' : username[0], 'trim_user' : 1, 'count' : 100, 'include_rts' : 1}
    else:
        myparams = {'screen_name' : username[0], 'trim_user' : 1, 'count' : 100, 'include_rts' : 1,\
                    'max_id' : max_id_}
    myheaders = {'Authorization' : "Bearer "+TW_ACCESS_TOKEN}
    try:
        timeline = requests.get(twapi, params=myparams, headers=myheaders)
    except Exception as Exc:
        sql_log(Exc)
        sql_log("error in making call to TW API, will try waiting and moving to next user")
        TW_MISSED_LIST.append(username[0])
        time.sleep(15*60)
        return
    timelined = json.loads(timeline.text)
    try:
        rate_limit = int(timeline.headers['x-rate-limit-remaining'])
    except Exception as Exc:
        sql_log(Exc)
        sql_log("Problem in retreiving the rate limit")
    else:
        if (rate_limit) == 0:   #if rate limit is 0, \ then our call did not go through
                                # so we have to wait and then redo the call
            time.sleep(15*60)
            return get_user_tweets(username, max_id_)
        elif 0 < (rate_limit) <= 3:
            time.sleep(15*60)
    if "errors" in timelined:
        sql_log(timelined['errors'][0])
        TW_MISSED_LIST.append(username[0])
        return
    for tweet in range(len(timelined)):
        tweet_time = datetime.datetime.strptime(timelined[tweet]['created_at'] \
                                               , "%a %b %d %H:%M:%S +0000 %Y")
        #datetime.datetime.strptime('2016-01-01T12:00:00+0000',"%Y-%m-%dT%H:%M:%S+0000")
        if tweet_time > FROM_ and tweet_time < UNTIL:
            author = username[0]
            created = datetime.datetime.strftime(tweet_time, "%Y-%m-%d %H:%M:%S")
            t_id = timelined[tweet]['id_str']
            t_id_link_c = "http://www.twitter.com/"+username[0]+"/status/"+t_id
            t_id_link = DB.escape_string(t_id_link_c)
            favs = timelined[tweet]['favorite_count']
            t_attr_list = rt_link_type(timelined[tweet])
            isrt = t_attr_list[0]
            rts = t_attr_list[1]
            link = t_attr_list[2]
            t_type = t_attr_list[3]
            isqrt = t_attr_list[4]
            if timelined[tweet]['text'] == []:   #is tweet text empty?
                message = ""
            else:
                message = unidecode(timelined[tweet]['text']).replace('\\', '')
                if message.startswith('@'):
                    isrep = 1
                else:
                    isrep = 0
            if isqrt == 1:
                quoted_status = timelined[tweet]['quoted_status']['text']
                t_brexit = is_brexit(message, quoted_status, "")
            else:
                t_brexit = is_brexit(message, "", "")
            LIST_OF_TWEETS.append(Tweet(username[1], author, created, t_id_link, 
                                        link, message, t_type,
                                        isrt, isrep, rts, favs, t_brexit, isqrt))
    n_returned_tw = len(timelined)
    if n_returned_tw > 0:
        last_returned_tw = timelined[n_returned_tw - 1]
        max_id_tw = last_returned_tw['id'] - 1
    else:
        return
    if datetime.datetime.strptime(last_returned_tw['created_at'] \
                                      , "%a %b %d %H:%M:%S +0000 %Y") \
                                      < FROM_:
        return
    else:
        return get_user_tweets(username, max_id_tw)

def update_fb():
    '''update the engagement numbers on facebook posts'''
    sql_log("starting facebook post update process")
    posts_to_update = []
    readin_posts = "select post_id_link from fb_posts where created_time between '%s' \
                    and '%s'" %(WEEKAGO, YESTERDAY_STR_FORMAT)
    try:
        CURSOR.execute(readin_posts)
        post_results = CURSOR.fetchall()
        for post in post_results:
            posts_to_update.append([post[0]])
    except:
        sql_log("Failed to load posts to update")
        return
    for j, post in enumerate(posts_to_update):
        strindex1 = post[0].rfind(r'.com/')+5
        strindex2 = post[0].find(r'/posts/')
        part1 = post[0][strindex1:strindex2]
        part2 = post[0][strindex2+7:]
        posts_to_update[j].append(part1+"_"+part2)
    update_fb_posts(posts_to_update)
    updatefbindb()

def update_fb_posts(posts_to_update_):
    '''go through list of posts that need updating and get new numbers from FB API'''
    for post in posts_to_update_:
        request = GRAPH+post[1]+FB_POST_FIELDS+FB_ACCESS_TOKEN
        fbpostr = requests.get(request, verify=False)
        fbpostrd = json.loads(fbpostr.text)
        if "error" not in fbpostrd:
            nlikes = fbpostrd['likes']['summary']['total_count']
            ncomments = fbpostrd['comments']['summary']['total_count']
            if "shares" in fbpostrd:
                nshares = fbpostrd['shares']['count']
            else:
                nshares = 0
            UPDATED_POSTS_LIST.append([post[0], nlikes, ncomments, nshares])

def updatefbindb():
    '''updates FB posts in the database based on UPDATED_POSTS_LIST'''
    for post in UPDATED_POSTS_LIST:
        update_sql = "update fb_posts set likes='%s', comments='%s', shares='%s', asof='%s' \
                      where post_id_link='%s'" % (post[1], post[2], post[3], CURRENT_DATETIME, \
                        post[0])
        sql_insert(update_sql)
    sql_log("updated FB posts in DB")

def update_tweets():
    '''update engagement numbers on tweets'''
    sql_log("starting tweet update process")
    tweets_to_update = []
    readin_tweets = "select id_link from tweets where created between '%s' and '%s'" % \
                    (WEEKAGO, YESTERDAY_STR_FORMAT)
    try:
        CURSOR.execute(readin_tweets)
        tweet_results = CURSOR.fetchall()
        for tweet in tweet_results:
            tweets_to_update.append([tweet[0]])
    except:
        sql_log("Failed to load tweets to update")
        return
    for j, tweet in enumerate(tweets_to_update):
        strindex = tweet[0].rfind(r'/status')
        tidnum = tweet[0][strindex+8:]
        tweets_to_update[j] = tidnum
    update_tweets_api(tweets_to_update)

def update_tweets_api(tweets_to_update_):
    '''send tweets needing updating to twitter api to get new numbers'''
    twapi = "https://api.twitter.com/1.1/statuses/lookup.json"
    myheaders = {'Authorization' : "Bearer "+TW_ACCESS_TOKEN}
    for chunk in chunks(tweets_to_update_, 100):
        currentids = ','.join(chunk)
        try:
            myparams = {'id' : currentids}
            statuses = requests.get(twapi, params=myparams, headers=myheaders)
        except Exception as Exc:
            sql_log(Exc)
            sql_log("failed to get updated tweets")
        statusesd = json.loads(statuses.text)
        try:
            rate_limit = int(statuses.headers['x-rate-limit-remaining'])
        except Exception as Exc:
            sql_log(Exc)
            sql_log("Rate limit problem")
        else:
            if (rate_limit) == 0:
                time.sleep(15*60)
                return update_tweets_api(tweets_to_update_)
            elif 0 < (rate_limit) <= 3:
                time.sleep(15*60)
        for tweet in statusesd:
            tid = str(tweet['id'])
            favs = tweet['favorite_count']
            t_attr_list = rt_link_type(tweet)
            rts = t_attr_list[1]
            UPDATED_TWEETS_LIST.append([tid, rts, favs])
    updatetwindb()

def updatetwindb():
    '''updates tweets in the database based on UPDATED_TWEETS_LIST'''
    for tweet in UPDATED_TWEETS_LIST:
        update_sql = "update tweets set rts='%s', favs='%s', asof='%s' \
                      where id_link like '%s'" % (tweet[1], tweet[2], \
                        CURRENT_DATETIME, '%'+tweet[0])
        sql_insert(update_sql)
    sql_log("updated tweets in DB")

def insert_fb_post(fbpd):
    '''inserts a facebook post into the database'''
    insert_post = "insert ignore into fb_posts (id, author, created_time, post_id_link, likes, \
                    comments, shares, message, link, link_name, description, status_type, brexit) \
                    values ('%s', '%s', '%s', '%s', '%s', \
                    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                    % (fbpd['id'], DB.escape_string(fbpd['author']), fbpd['created_time'], \
                       DB.escape_string(fbpd['id_link']), \
                       fbpd['likes'], fbpd['comments'], \
                       fbpd['shares'], DB.escape_string(fbpd['message']), fbpd['link'], \
                       DB.escape_string(fbpd['link_name']), DB.escape_string(fbpd['description']), \
                       fbpd['status_type'], fbpd['brexit'])
    sql_insert(insert_post)

#sys.excepthook = runtime

#for ssl verif
requests.packages.urllib3.disable_warnings()
urllib3.contrib.pyopenssl.inject_into_urllib3()

#database connection
MYSQL_FILE = file('mysqlbrexit.cfg')
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
TODAY = (datetime.datetime.utcnow()).strftime('%Y-%m-%d')
FROM_ = YESTERDAY.replace(hour=0, minute=0, second=0, microsecond=0)
UNTIL = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
WEEKAGO = (NOW - datetime.timedelta(days=8)).strftime('%Y-%m-%d')

#api variables and connection
FB_ACCESS_TOKEN = "***" #should be encrypted
GRAPH = "https://graph.facebook.com/v2.5/"
FB_POST_HTTP = "/posts?fields="\
                            "likes.limit(0).summary(true)"\
                            ",comments.limit(0).summary(true)"\
                            ",shares"\
                            ",created_time"\
                            ",id"\
                            ",link"\
                            ",name"\
                            ",description"\
                            ",status_type"\
                            ",message" \
                            "&limit=100"
FB_POST_FIELDS = "?fields=likes.limit(0).summary(true),comments.limit(0).summary(true),shares"
TW_ACCESS_TOKEN = "***" #should be encrypted

#lists of data
FB_USERNAMES_LIST = []
TW_USERNAMES_LIST = []
LIST_OF_POSTS = []
IDS2 = []
LIST_OF_TWEETS = []
UPDATED_POSTS_LIST = []
UPDATED_TWEETS_LIST = []
FB_MISSED_LIST = []
TW_MISSED_LIST = []

#insert script started into db
sql_log("Script has started")


#read in ids from DB
READ_IN = "select name, twitter_id, facebook_id, id from influencers"
try:
    CURSOR.execute(READ_IN)
    READ_IN_RESULTS = CURSOR.fetchall()
    for row in READ_IN_RESULTS:
        name = row[0]
        twitter_id = row[1]
        facebook_id = row[2]
        iid = row[3]
        IDS2.append([name, twitter_id, facebook_id, iid])
except Exception as e:
    sql_log(e)
    sql_log("Couldn't read in candidates")

#read in keywords from DB
KEYWORDS = []
KEYWORDS_SQL = "select * from keywords"
try:
    CURSOR.execute(KEYWORDS_SQL)
    KWRESULTS = CURSOR.fetchall()
    for keyword in KWRESULTS:
        KEYWORDS.append(keyword[0])
except:
    sql_log("couldn't read in keywords")
else:
    sql_log("Successfully read in from DB")

#SET UP REGEX
INCLUDED = []
for i, term in enumerate(KEYWORDS):
    KEYWORDS[i] += (r'\b')
    INCLUDED.append(re.compile(KEYWORDS[i], re.IGNORECASE))
INCLUDED.append(re.compile(r'brexit', re.IGNORECASE))

#make user id lists

for x, item in enumerate(IDS2):
    if item[2] != "-":
        FB_USERNAMES_LIST.append([item[2], int(item[3])])
sql_log("FB account list created")

for x, item in enumerate(IDS2):
    if item[1] != "-":
        TW_USERNAMES_LIST.append([item[1], int(item[3])])
sql_log("TW account list created")


#get post data from facebook
for item in FB_USERNAMES_LIST:
    users_posts = get_fb_posts(item)
    for userpost in users_posts:
        insert_fb_post(userpost)

sql_log("Finished post data retreival from Facebook and post insertion")

#get tweets from twitter
for i, item in enumerate(TW_USERNAMES_LIST):
    get_user_tweets(item, 0)

sql_log("Finished tweet retreival from Twitter")

#insert posts into database

for item in LIST_OF_TWEETS:
    insert_tweet = "insert ignore into tweets (id, author, created, id_link, link, message, type, \
                    isrt, isrep, rts, favs, brexit, isqrt) values ('%s', '%s', '%s', '%s', '%s', \
                    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                    % (item.id, item.author, item.created, item.id_link, item.link,
                       DB.escape_string(item.message),
                       item.type, item.isrt, item.isrep, item.rts, item.favs, item.brexit,
                       item.isqrt)
    sql_insert(insert_tweet)

sql_log("Inserted tweets")

update_fb()

update_tweets()

sql_log("Script has succeeded")

#outputs users for whom there were errors in data collection
if FB_MISSED_LIST:
    sql_log("The following Facebook users failed:")
    sql_log(','.join(FB_MISSED_LIST))
if TW_MISSED_LIST:
    sql_log("The following Twitter users failed:")
    sql_log(','.join(TW_MISSED_LIST))

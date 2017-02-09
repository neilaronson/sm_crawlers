# -*- coding: utf-8 -*-
"""This crawler gathers daily social media metrics on a list of personalities stored in a SQL
database and then puts that data into various tables in the database

Metrics gathered:
Facebook:
-Page likes
-Daily post likes
-Daily post comments
-Daily post shares

Twitter
-Twitter followers
-Daily number of tweets
-Daily mentions (of username and full name)

Instagram
-Followers
-Number of posts
-Daily comments on posts
-Daily likes on posts
"""

from __future__ import division
import datetime
import json
import time
import sys
import requests
import MySQLdb
import urllib3
import urllib3.contrib.pyopenssl
from twython import Twython
from config import Config

#helper functions

def runtime(exctype, value, trb):
    sql_log(DB.escape_string(repr(exctype)))
    sql_log('Value:'+value.message)
    sql_log('Traceback line:'+str(trb.tb_lineno))

def sql_insert(command):
    'Tries to execute sql insert command'
    try:
        CURSOR.execute(command)
        DB.commit()
    except:
        DB.rollback()

def sql_log(lrecord):
    'Inserts a record into the log'
    print lrecord
    lcommand = "insert into log (record) values ('%s')" % (lrecord)
    sql_insert(lcommand)

def totimestamp(dt, epoch=datetime.datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return int(round(td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6)

def count_ig_engagements_in_batch(ig_results):
    """ count_ig_engagements_in_batch """
    ig_likes_in_batch = 0
    ig_comments_in_batch = 0
    batch_results = []
    n_returned_ig_posts = len(ig_results['data'])
    for post in range(n_returned_ig_posts):
        created_time = datetime.datetime.fromtimestamp\
                        (int(ig_results['data'][post]['created_time']))
        if created_time >= FROM_ and created_time < UNTIL:
            ig_likes_in_batch += ig_results['data'][post]['likes']['count']
            ig_comments_in_batch += ig_results['data'][post]['comments']['count']
    batch_results.append(ig_likes_in_batch)
    batch_results.append(ig_comments_in_batch)
    return batch_results

def get_media(current_ig_id1, max_id1, ig_get_media_results1):
    """ get_media """
    ig_gmr_local = ig_get_media_results1
    if max_id1 == 0:
        try:
            ig_posts_r = requests.get(INSTAGRAM_API+"users/"+current_ig_id1+\
                                    "/media/recent/?count=20&" \
                                    +INSTAGRAM_TOKEN, verify=False)
        except Exception as E:
            sql_log(E)
    else:
        try:
            ig_posts_r = requests.get(INSTAGRAM_API+"users/"+current_ig_id1+"/media/recent/?count=20&" \
                                    +"max_id="+max_id1+"&"+INSTAGRAM_TOKEN, verify=False)
        except Exception as E:
            sql_log(E)
    ig_posts_r_d = json.loads(ig_posts_r.text)
    if ig_posts_r.headers['X-Ratelimit-Remaining'] == 1:
        time.sleep(60*60)
    batch_results = count_ig_engagements_in_batch(ig_posts_r_d)
    ig_gmr_local[0] += batch_results[0]
    ig_gmr_local[1] += batch_results[1]
    if ig_posts_r_d['pagination'] == {}:
        return ig_gmr_local
    else:
        n_returned_ig_posts = len(ig_posts_r_d['data'])
        last_ig_returned_post = ig_posts_r_d['data'][(n_returned_ig_posts-1)]
        maxid2 = ig_posts_r_d['pagination']['next_max_id']
        if datetime.datetime.fromtimestamp(int(last_ig_returned_post['created_time'])) < FROM_:
            return ig_gmr_local
        else:
            return get_media(current_ig_id1, maxid2, ig_gmr_local)

def get_mentions(search_term, max_id2, n_mentions):
    """ get_mentions """
    try:
        results = TWITTER.search(q=search_term,
                                 count=100,
                                 max_id=max_id2,
                                 result_type="recent",
                                 until=TODAY)
    except Exception as Exc:
        sql_log(Exc)
        sql_log("about to hit TWITTER rate limit")
        time.sleep(15*60)
        return get_mentions(search_term, max_id2, n_mentions)
    rate_limit_mentions = TWITTER.get_lastfunction_header('x-rate-limit-remaining')
    if not isinstance(rate_limit_mentions, str):
        rate_limit_mentions = 2
    else:
        rate_limit_mentions = int(rate_limit_mentions)
    if rate_limit_mentions == 2:
        sql_log("about to hit TWITTER rate limit")
        time.sleep(15*60)
    n_mentions = n_mentions + count_in_batch(results)
    if results['statuses'] == []:
        return n_mentions
    else:
        n_returned_tweets = len(results['statuses'])
        last_returned_tweet = results['statuses'][(n_returned_tweets-1)]
        max_id_tw = last_returned_tweet['id'] - 1
        if datetime.datetime.strptime(last_returned_tweet['created_at'] \
                                      , "%a %b %d %H:%M:%S +0000 %Y") \
                                      < FROM_:
            return n_mentions
        else:
            return get_mentions(search_term, max_id_tw, n_mentions)

def count_in_batch(results):
    """ count_in_batch """
    n_mentions_in_batch = 0
    for tweet in range(len(results['statuses'])):
        tweet_time = datetime.datetime.strptime(results['statuses'][tweet]['created_at'] \
                                               , "%a %b %d %H:%M:%S +0000 %Y")
        start = FROM_
        end = UNTIL
        if end > tweet_time > start:
            n_mentions_in_batch = n_mentions_in_batch +1
    return n_mentions_in_batch

sys.excepthook = runtime

#time variables
NOW = datetime.datetime.now()
CURRENT_DATETIME = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
ONE_DAY = datetime.timedelta(days=1)
YESTERDAY = (NOW - ONE_DAY)
DAY_BEFORE_YEST = YESTERDAY - ONE_DAY
YESTERDAY_STR_FORMAT = (datetime.datetime.now()-ONE_DAY).strftime('%Y-%m-%d')
TODAY = (datetime.datetime.now()).strftime('%Y-%m-%d')
FROM_ = DAY_BEFORE_YEST.replace(hour=22, minute=0, second=0, microsecond=0)
UNTIL = YESTERDAY.replace(hour=22, minute=0, second=0, microsecond=0)

#for ssl verif
requests.packages.urllib3.disable_warnings()
urllib3.contrib.pyopenssl.inject_into_urllib3()

#database connection
MYSQL_FILE = file('mysql.cfg')
MYSQL_CONF = Config(MYSQL_FILE)

DB = MySQLdb.connect(MYSQL_CONF.db_address,
                     MYSQL_CONF.db_user,
                     MYSQL_CONF.db_password,
                     MYSQL_CONF.db_name)

#prepare CURSOR
CURSOR = DB.cursor()

#log set up
CREATE_LOG = """CREATE TABLE IF NOT EXISTS log
                (Tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                record text)"""
sql_insert(CREATE_LOG)

#insert script started into db
sql_log("Script has started")

#make backup of most recent table before adding data
BACKUP_TABLE_NAMES = ["backup_dailyfbdata", "backup_dailytwdata", "backup_dailyigdata"]
TABLE_NAMES = ["dailyfbdata", "dailytwdata", "dailyigdata"]
sql_log("BDD save ==> BACKUP")
for i, item in enumerate(TABLE_NAMES):
    drop_existing_backup = "drop table if exists %s" % (BACKUP_TABLE_NAMES[i])
    make_backup = "create table %s select * from %s" % (BACKUP_TABLE_NAMES[i], item)
    sql_insert(drop_existing_backup)
    sql_insert(make_backup)

#api variables and connection
FB_ACCESS_TOKEN = "***" #should be encrypted
GRAPH = "https://graph.facebook.com/v2.5/"
FB_POST_HTTP = "/posts?fields="\
                            "likes.limit(0).summary(true)"\
                            ",comments.limit(0).summary(true)"\
                            ",shares"\
                            ",created_time"\
                            "&limit=100"
APP_KEY = "***" #refers to Twitter app key, should be encrypyed
APP_SECRET = "***" #refers to Twitter app secret, should be encrypyed
ACCESS_TOKEN = "***" #refers to Twitter access token, should be encrypted
INSTAGRAM_API = "https://api.instagram.com/v1/"
INSTAGRAM_TOKEN = "***" #should be encrypted
TWITTER = Twython(APP_KEY, access_token=ACCESS_TOKEN)

#lists of data
IDS2 = []
FB_USERNAMES_LIST = []
PAGE_LIKES = []
POST_LIKES = []
POST_COMMENTS = []
POST_SHARES = []
N_FB_POSTS = []
MENTIONS_LIST = []
FOLLOWER_LIST = []
N_TWEETS_LIST = []
IG_USERNAMES_LIST = []
IG_USER_IDS_LIST = []
IG_FOLLOWERS_LIST = []
IG_N_POSTS_LIST = []
IG_POST_LIKES_LIST = []
IG_POST_COMMENTS_LIST = []

#read in ids from DB
READ_IN = "select name, party, twitter_id, facebook_id, instagram_id from presidentiables"
try:
    CURSOR.execute(READ_IN)
    READ_IN_RESULTS = CURSOR.fetchall()
    for row in READ_IN_RESULTS:
        name = row[0]
        party = row[1]
        twitter_id = row[2]
        facebook_id = row[3]
        instagram_id = row[4]
        IDS2.append([name, party, twitter_id, facebook_id, instagram_id])
except Exception as e:
    sql_log(e)
    sql_log("Couldn't read in candidates")

#create list of people w/instagram accounts
for x, item in enumerate(IDS2):
    if item[4] != "-":
        IG_USERNAMES_LIST.append(item[4])
sql_log("IG account list created")

#create list of people with facebook accounts
for x, item in enumerate(IDS2):
    if item[3] != "-":
        FB_USERNAMES_LIST.append(item[3])
sql_log("FB account list created")

# get number of twitter followers, statuses, and mentions
for n, item in enumerate(IDS2):
    if item[2] == "-":          #applies when person does not have TW handle
        sql_log("TW data ==> "+item[2])
        FOLLOWER_LIST.append(0)
        N_TWEETS_LIST.append(0)
        twitter_searchterm = "%22"+str(item[0])+"%22"
        sql_log("TW mentions 1 ==> "+item[0])
        MENTIONS_LIST.append(get_mentions(twitter_searchterm, 0, 0))
    else:
        sql_log("TW data ==> "+item[2])
        try:
            usersr = TWITTER.lookup_user(screen_name=item[2])   #user data request
        except Exception as E:
            sql_log(E)
        FOLLOWER_LIST.append(usersr[0]['followers_count'])
        N_TWEETS_LIST.append(usersr[0]['statuses_count'])
        rate_limit_mentions1 = TWITTER.get_lastfunction_header('x-rate-limit-remaining')
        if not isinstance(rate_limit_mentions1, str):
            rate_limit_mentions1 = 2
        else:
            rate_limit_mentions1 = int(rate_limit_mentions1)
        if int(TWITTER.get_lastfunction_header('x-rate-limit-remaining')) == 2:
            sql_log("about to hit TWITTER user rate limit")
            time.sleep(15*60)
        searchterm1 = ("%40"+str(item[2]))
        searchterm2 = "%22"+str(item[0])+"%22"
        twitter_searchterm = searchterm1+" OR "+searchterm2
        sql_log("TW mentions 2 ==> "+item[0])
        MENTIONS_LIST.append(get_mentions(twitter_searchterm, 0, 0))
        #MENTIONS_LIST.append(100)
sql_log("==> TWITTER user info and mentions retreived")


#get page likes, post likes, comments and shares from facebook
for i, item in enumerate(FB_USERNAMES_LIST):
    #page likes
    sql_log("FB likes ==> "+item)
    try:
        fbpager = requests.get(GRAPH+item+"?fields=likes"+FB_ACCESS_TOKEN
                               , verify=False).json()
    except Exception as e:
        sql_log(e)
    if "error" in fbpager:
        PAGE_LIKES.append(0)
        sql_log("error finding fb pagelikes for %s" % (item))
    else:
        PAGE_LIKES.append(fbpager['likes'])
    try:
        fbpostr = requests.get(GRAPH+
                               item+
                               FB_POST_HTTP+
                               "&until="+str(totimestamp(UNTIL))+
                               "&since="+str(totimestamp(FROM_))+
                               FB_ACCESS_TOKEN,
                               verify=False)
    except Exception as e:
        sql_log(e)
    #post data
    sql_log("FB post ==> "+item)
    fbpostrd = json.loads(fbpostr.text)
    if fbpostrd['data'] == []:
        POST_LIKES.append(0)
        POST_COMMENTS.append(0)
        POST_SHARES.append(0)
        N_FB_POSTS.append(0)
    else:
        total_posts = 0
        total_post_likes = 0
        total_post_comments = 0
        total_post_shares = 0
        for n in range(len(fbpostrd['data'])):
            total_posts = total_posts+1
            total_post_likes = total_post_likes +\
                               fbpostrd['data'][n]['likes']['summary']['total_count']
            total_post_comments = total_post_comments +\
                                  fbpostrd['data'][n]['comments']['summary']['total_count']
            if "shares" in fbpostrd['data'][n]:
                total_post_shares = total_post_shares + fbpostrd['data'][n]['shares']['count']
        POST_LIKES.append(total_post_likes)
        POST_COMMENTS.append(total_post_comments)
        POST_SHARES.append(total_post_shares)
        N_FB_POSTS.append(total_posts)
sql_log("Page likes and post data retreived from Facebook")


#find instagram user ids
for x, item in enumerate(IG_USERNAMES_LIST):
    current_ig_sn = item
    sql_log("INSTA id ==> "+current_ig_sn)
    try:
        ig_id_r = requests.get(INSTAGRAM_API+"users/search?q="+current_ig_sn+"&"+INSTAGRAM_TOKEN,
                               verify=False)
    except Exception as e:
        sql_log(e)
    ig_id_r_d = json.loads(ig_id_r.text)
    if ig_id_r.headers['X-Ratelimit-Remaining'] == 1:
        sql_log("hit instagram rate limit. wait one hour")
        time.sleep(60*60)
    if ig_id_r.status_code != 200:
        #         % (current_ig_sn, ig_id_r_d['meta']['error_message'])
        IG_USER_IDS_LIST.append("?")
    else:
        if ig_id_r_d['data'] == []:
            IG_USER_IDS_LIST.append("?")
        else:
            for y, item in enumerate(ig_id_r_d['data']):
                potential_screenname = item['username']
                if potential_screenname == current_ig_sn:
                    user_id = str(item['id'])
                    IG_USER_IDS_LIST.append(user_id)
sql_log("Instagram user ID's retreived")

#get instagram follower count and n_posts
for x, item in enumerate(IG_USER_IDS_LIST):
    ig_get_media_results = [0, 0]
    current_ig_id = IG_USER_IDS_LIST[x]
    sql_log("INSTA followers ==> "+current_ig_id)
    if current_ig_id == "?":
        IG_FOLLOWERS_LIST.append(0)
        IG_N_POSTS_LIST.append(0)
        IG_POST_LIKES_LIST.append(0)
        IG_POST_COMMENTS_LIST.append(0)
    else:
        try:
            ig_user_info_r = requests.get(INSTAGRAM_API+"users/"+current_ig_id+"/?"+INSTAGRAM_TOKEN,
                                      verify=False)
        except Exception as e:
            sql_log(e)
        ig_user_info_r_d = json.loads(ig_user_info_r.text)
        if ig_user_info_r.headers['X-Ratelimit-Remaining'] == 1:
            sql_log("hit instagram rate limit. wait one hour")
            time.sleep(60*60)
        if ig_user_info_r.status_code != 200:
            #        % (current_ig_id, ig_user_info_r_d['meta']['error_message'])
            IG_FOLLOWERS_LIST.append(0)
            IG_N_POSTS_LIST.append(0)
        else:
            IG_FOLLOWERS_LIST.append(ig_user_info_r_d['data']['counts']['followed_by'])
            IG_N_POSTS_LIST.append(ig_user_info_r_d['data']['counts']['media'])
        get_media_results = get_media(current_ig_id, 0, ig_get_media_results)
        IG_POST_LIKES_LIST.append(get_media_results[0])
        IG_POST_COMMENTS_LIST.append(get_media_results[1])

sql_log("Instagram user info and engagements retreived")


sql_log("BDD save ==> FB")
for i, item in enumerate(FB_USERNAMES_LIST):
    if PAGE_LIKES[i] == "error":
        insertdata_e = "insert into dailyfbdata (facebook_id, asof) values ('%s', '%s')"\
                        %(item, CURRENT_DATETIME)
        sql_insert(insertdata_e)
    else:
        insertdata = "insert into dailyfbdata (date, facebook_id, FB_page_likes, FB_post_likes,\
                        FB_post_comments, FB_post_shares, FB_n_posts, asof)\
                        values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"\
                        %(YESTERDAY_STR_FORMAT, IDS2[i][3],
                          PAGE_LIKES[i],
                          POST_LIKES[i],
                          POST_COMMENTS[i],
                          POST_SHARES[i],
                          N_FB_POSTS[i],
                          CURRENT_DATETIME)
        dailydelta = """
            update dailyfbdata a
                left outer join dailyfbdata b
                on a.facebook_id= b.facebook_id
                and datediff(a.date, b.date)=1
            set a.FB_daily_likes_delta= (a.fb_page_likes - ifnull(b.fb_page_likes, NULL))
            where a.date='%s'""" % (YESTERDAY_STR_FORMAT)
        sql_insert(insertdata)
        sql_insert(dailydelta)

#insert tw data
sql_log("BDD save ==> TW")
for i, item in enumerate(IDS2):
    insertdata_twitter = "insert into dailytwdata (date, twitter_id, tw_followers, tw_n_tweets,\
                            tw_mentions, asof) values ('%s', '%s', '%s', '%s', '%s', '%s')"\
                            %(YESTERDAY_STR_FORMAT,
                              item[2],
                              FOLLOWER_LIST[i],
                              N_TWEETS_LIST[i],
                              MENTIONS_LIST[i],
                              CURRENT_DATETIME)
    dailydelta_tw_f = """
    update dailytwdata a
        left outer join dailytwdata b
        on a.twitter_id= b.twitter_id
        and datediff(a.date, b.date)=1
    set a.tw_daily_followers_delta= (a.tw_followers - ifnull(b.tw_followers, NULL))
    where a.date='%s'""" % (YESTERDAY_STR_FORMAT)
    dailydelta_tws = """
    update dailytwdata a
        left outer join dailytwdata b
        on a.twitter_id= b.twitter_id
        and datediff(a.date, b.date)=1
    set a.tw_daily_tweets_delta= (a.tw_n_tweets - ifnull(b.tw_n_tweets, NULL))
    where a.date='%s'""" % (YESTERDAY_STR_FORMAT)
    sql_insert(insertdata_twitter)
    sql_insert(dailydelta_tw_f)
    sql_insert(dailydelta_tws)

#insert ig data
sql_log("BDD save ==> INSTA")
for i, item in enumerate(IG_USERNAMES_LIST):
    insertdata_ig = "insert into dailyigdata (date, ig_username, ig_id, ig_followers, \
                        ig_posts, ig_post_likes, ig_post_comments, asof) values \
                        ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                        (YESTERDAY_STR_FORMAT, item, IG_USER_IDS_LIST[i], \
                         IG_FOLLOWERS_LIST[i], IG_N_POSTS_LIST[i], IG_POST_LIKES_LIST[i], \
                         IG_POST_COMMENTS_LIST[i], CURRENT_DATETIME)
    dailydelta_ig_f = """
        update dailyigdata a
            left outer join dailyigdata b
            on a.ig_id= b.ig_id
            and datediff(a.date, b.date)=1
        set a.ig_daily_followers_delta = (a.ig_followers - ifnull(b.ig_followers, NULL))
        where a.date='%s'""" % (YESTERDAY_STR_FORMAT)
    dailydelta_igs = """
        update dailyigdata a
            left outer join dailyigdata b
            on a.ig_id= b.ig_id
            and datediff(a.date, b.date)=1
        set a.ig_daily_posts_delta = (a.ig_posts - ifnull(b.ig_posts, NULL))
        where a.date='%s'""" % (YESTERDAY_STR_FORMAT)
    sql_insert(insertdata_ig)
    sql_insert(dailydelta_ig_f)
    sql_insert(dailydelta_igs)

#insert script finished into db
sql_log("Script has succeeded")

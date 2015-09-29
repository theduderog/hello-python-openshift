import os

from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems

import multiprocessing
import time
from TwitterAPI import TwitterAPI, TwitterRestPager
from elasticsearch import Elasticsearch

from yaml import load, dump
import json


CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN_KEY = ''
ACCESS_TOKEN_SECRET = ''

SEARCH_TERM = 'pizza'

es = Elasticsearch(os.environ["ES_URL"])

def worker():
    api = TwitterAPI(CONSUMER_KEY,
                     CONSUMER_SECRET,
                     ACCESS_TOKEN_KEY,
                     ACCESS_TOKEN_SECRET)

    pager = TwitterRestPager.TwitterRestPager(api, 'search/tweets', {'q': SEARCH_TERM})
    for item in pager.get_iterator():
        if 'text' in item:
            tweet = {}
            # tweet['coordinates'] = item['coordinates']
            tweet['@timestamp'] = time.mktime(time.strptime(item['created_at'],"%a %b %d %H:%M:%S +0000 %Y")) * 1000
            # tweet['place'] = item['place']
            # ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(item['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))

            # tweet['@timestamp'] = item['created_at']
            tweet['username'] = item['user']['name']
            tweet['handle'] = item['user']['screen_name']
            tweet['lang'] = item['lang']
            tweet['timezone'] = item['user']['time_zone']
            tweet['followers'] = item['user']['followers_count']
            tweet['location'] = item['user']['location']
            tweet['retweeted'] = item['retweeted']
            tweet['text'] = item['text']
            es.index(index="tweets", doc_type="tweet", body=tweet)
    return

def wsgi_handler(environ, start_response):
    start_response('200 OK', [('Content-Type','text/html')])
    stats = es.indices.stats(index="tweets", human=True)
    num_tweets = stats["_all"]["primaries"]["docs"]["count"]
    KIBANA = [bytes("<iframe src=\"http://route-zeta-kibana.elk-test.apps.demo.os.quantezza.com/#/dashboard/New-Dashboard?embed&_a=(filters:!(),panels:!((col:1,id:%23-Tweets,row:1,size_x:3,size_y:2,type:visualization),(col:4,id:by-location,row:1,size_x:3,size_y:2,type:visualization),(col:7,id:Tweets-by-lang,row:1,size_x:3,size_y:2,type:visualization),(col:1,columns:!(followers,handle,lang,location,retweeted,text,timezone,username),id:all-tweets,row:3,size_x:12,size_y:4,sort:!('@timestamp',desc),type:search)),query:(query_string:(analyze_wildcard:!t,query:'*')),title:'New%20Dashboard')&_g=(refreshInterval:(display:'1%20minute',pause:!f,section:2,value:60000),time:(from:now%2Fd,mode:quick,to:now%2Fd))\" height=\"600\" width=\"800\"></iframe>", "UTF-8")]
    ENV = [bytes("%30s %s <br/>" % (key,os.environ[key]), "UTF-8") for  key in os.environ.keys()]
    return [bytes("Tweets = %s <br/><br/>" % (num_tweets, ), "UTF-8")] + KIBANA + ENV

class StandaloneApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

def webapp():
    StandaloneApplication(wsgi_handler, {'bind': ':8080'}).run()
    # StandaloneApplication(wsgi_handler, {'bind': ':8088'}).run()

if __name__ == '__main__':

    api_key = open('/etc/secret-volume/twitter-secret.yaml')
    # api_key = open('/Users/arrawatia/code/try-openshift/template/twitter-secret.yaml')
    data = load(api_key)
    api_key.close()
    print(data)
    CONSUMER_KEY = data['CONSUMER_KEY']
    CONSUMER_SECRET = data['CONSUMER_SECRET']
    ACCESS_TOKEN_KEY = data['ACCESS_TOKEN_KEY']
    ACCESS_TOKEN_SECRET = data['ACCESS_TOKEN_SECRET']

    # os.environ['secret'] = str(data)
    with open('mapping.json') as mapping_file:
    # mapping_file = open("mapping.json")
        mapping = json.loads(mapping_file.read())
    # mapping_file.close()
        es.indices.delete(index="tweets")
        es.indices.create(index='tweets', ignore=400, body=mapping)
    jobs = []
    p1 = multiprocessing.Process(target=worker)
    jobs.append(p1)
    p1.start()
    p2 = multiprocessing.Process(target=webapp)
    jobs.append(p2)
    p2.start()
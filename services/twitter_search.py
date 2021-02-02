import os
import requests
import pandas as pd
from app import get_base_path
from dotenv import load_dotenv

load_dotenv()

tweet_fields = "tweet.fields=text,author_id,created_at"
max_results = "max_results=100"


class SearchTwitter:
    def __init__(self):
       
        self.BEARER_TOKEN = os.getenv("BEARER_TOKEN")

    def runner(self, query):
        response = self.search_twitter(query=query)
        self.write_to_csv(response=response, query=query)

    def search_twitter(self, query, tweet_fields=tweet_fields, max_results=max_results):
        bearer_token = self.BEARER_TOKEN
        headers = {"Authorization": "Bearer {}".format(bearer_token)}

        url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(
            query, tweet_fields, max_results
        )
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def write_to_excel(self,response, query):
        tweets = response["data"]
        df = pd.DataFrame(tweets)
        keys = []
        for i in range(len(df)):
            keys.append(query)
        df['key'] = keys

        # df.to_excel('../temp_data/{}_tweets.xlsx'.format(query), sheet_name=query, index=False, encoding="utf-8")
        return df

    def write_to_csv(self,response, query):
        tweets = response["data"]
        df = pd.DataFrame(tweets)
        keys = []
        for i in range(len(df)):
            keys.append(query)
        df['key'] = keys
        df.to_csv('{}/temp_data/{}_tweets.csv'.format(get_base_path(),query), index=False, encoding="utf-8")
        # print(df.columns)
        return df

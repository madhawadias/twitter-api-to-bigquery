import os
import requests, pprint
import pandas as pd
from app import get_base_path
from dotenv import load_dotenv

load_dotenv()

tweet_fields = "tweet.fields=text,author_id,created_at"
user_fields = "user.fields=description,username"
expansions = "expansions=author_id"
max_results = "max_results=100"


class SearchTwitter:
    def __init__(self):
        self.BEARER_TOKEN = os.getenv("BEARER_TOKEN")

    def runner(self, query):
        response = self.search_twitter(query=query)
        # pprint.pprint(response["includes"]["users"])
        self.write_to_csv(response=response, query=query)

    def search_twitter(self, query, tweet_fields=tweet_fields, user_fields=user_fields, expansions=expansions,
                       max_results=max_results):
        bearer_token = self.BEARER_TOKEN
        headers = {"Authorization": "Bearer {}".format(bearer_token)}

        url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}".format(
            query, expansions, tweet_fields, user_fields, max_results
        )
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def write_to_excel(self, response, query):
        tweets = response["data"]
        df = pd.DataFrame(tweets)
        keys = []
        for i in range(len(df)):
            keys.append(query)
        df['key'] = keys

        # df.to_excel('../temp_data/{}_tweets.xlsx'.format(query), sheet_name=query, index=False, encoding="utf-8")
        return df

    def write_to_csv(self, response, query):
        tweets = response["data"]
        users = response["includes"]["users"]
        keys = []

        df = pd.DataFrame(tweets)
        df_user = pd.DataFrame(users)
        df_user.rename(columns={'id': 'author_id'}, inplace=True)
        df = pd.merge(df, df_user, on='author_id', how='left')

        for i in range(len(df)):
            keys.append(query)
        df['key'] = keys
        df.to_csv('{}/temp_data/{}_tweets.csv'.format(get_base_path(), query), index=False, encoding="utf-8")

        return df

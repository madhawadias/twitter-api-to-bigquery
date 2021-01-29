import os
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

BEARER_TOKEN = os.getenv("BEARER_TOKEN")
tweet_fields = "tweet.fields=text,author_id,created_at"
max_results = "max_results=100"


def search_twitter(query, tweet_fields=tweet_fields, bearer_token=BEARER_TOKEN, max_results=max_results):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}".format(
        query, tweet_fields, max_results
    )
    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def write_to_excel(response, query):
    tweets = response["data"]
    df = pd.DataFrame(tweets)
    # df.to_excel('./temp_data/{}_tweets.xlsx'.format(query), sheet_name=query, index=False)
    # print(df.columns)
    return df

def write_to_csv(response, query):
    tweets = response["data"]
    df = pd.DataFrame(tweets)
    df.to_csv('./temp_data/{}_tweets.csv'.format(query), index=False)
    # print(df.columns)
    return df

if __name__ == '__main__':
    query = "pizza"
    response = search_twitter(query=query)
    write_to_csv(response=response, query=query)

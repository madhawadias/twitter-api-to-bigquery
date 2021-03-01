import time,os
import requests, pprint
import pandas as pd
from app import get_base_path
from dotenv import load_dotenv

load_dotenv()

# define search query values
tweet_fields = "tweet.fields=text,author_id,created_at,geo"
user_fields = "user.fields=description,username,location"
place_fields = "place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type"
expansions = "expansions=author_id,geo.place_id"
max_results = "max_results=100"


class SearchTwitter:
    def __init__(self):
        self.BEARER_TOKEN = os.getenv("BEARER_TOKEN")

    def runner(self, query):
        # get the twitter search result by "search_twitter" function
        response = self.search_twitter(query=query)
        # pprint.pprint(response["data"])
        # write the result to the csv by "write-to_csv" function
        self.write_to_csv(response=response, query=query)

    def search_twitter(self, query, tweet_fields=tweet_fields, user_fields=user_fields, expansions=expansions,
                       place_fields=place_fields, max_results=max_results):
        bearer_token = self.BEARER_TOKEN
        headers = {"Authorization": "Bearer {}".format(bearer_token)}

        #  create the twitter api call
        url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}&{}".format(
            query, expansions, tweet_fields, user_fields, place_fields, max_results
        )
        print(url)
        #  send the api request to the twitter and get the response
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
        # seperate the tweets data and user data from the twitter search response
        tweets = response["data"]
        users = response["includes"]["users"]
        # places = {}
        # if "places" in response["includes"]:
        #     places = response["includes"]["places"]
        keys = []
        product_key = query.lower() + "_" + str(int(time.time()))
        product_id = []

        #  create data frame for tweets and users
        df = pd.DataFrame(tweets)
        df_user = pd.DataFrame(users)
        # df_places=[]
        # rename column id of the user dataframe and join both dataframes useing author_id column
        df_user.rename(columns={'id': 'author_id'}, inplace=True)
        df = pd.merge(df, df_user, on='author_id', how='left')

        # if places:
        #     df_places=pd.DataFrame(places)
        #     df_places.rename(columns={'id': 'author_id'}, inplace=True)
        #     df = pd.merge(df, df_places, on='author_id', how='left')

        # add product and product_id to the dataframe
        for i in range(len(df)):
            keys.append(query)
            product_id.append(product_key)
        df['key'] = keys
        df['product_id'] = product_id
        #  creates the csv using the dataframe
        df.to_csv('{}/temp_data/tweets.csv'.format(get_base_path()), index=False, encoding="utf-8")

        return df

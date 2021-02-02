from services.twitter_search import SearchTwitter
from services.upload_to_bigquery import WriteToBigQuery
from helpers.google_entities_helper import GoogleEntitiesHelper
from helpers.google_translate_helper import TranslateHelper
from helpers.sentiment_analysis_global_helper import SentimentAnalysisGlobalHelper
from app import get_base_path
import pandas as pd

class MERGE:
    def __init__(self):
        self.SearchTwitter = SearchTwitter
        self._sentiment_analysis_global_helper = SentimentAnalysisGlobalHelper
        self._google_translate_helper = TranslateHelper
        self._google_entities_helper = GoogleEntitiesHelper
        self.WriteToBigQuery = WriteToBigQuery
        self.get_base_path=get_base_path

    def runner(self, query):
        search = self.SearchTwitter()
        upload = self.WriteToBigQuery()
        translate =self._google_translate_helper()
        sentiment_analysis =self._sentiment_analysis_global_helper()
        extract_entities = self._google_entities_helper()

        print("getting data from twitter")
        search.runner(query=query)
        print("end of the search function")

        print("read written file")
        df = pd.read_csv("{}/temp_data/pizza_tweets.csv".format(self.get_base_path()))
        print("created df with {} rows".format(len(df.index)))
        df = df.dropna(subset=['text'])

        # df = df[:5]
        print("translating")
        df = translate.runner(df, "text")
        print("translate complete")

        df = df.dropna(subset=['translated'])

        print("extract entities")
        df = extract_entities.runner(df, "translated")
        print("extract completed")

        print("start sentiment analysis")
        df = sentiment_analysis.runner(df, "translated")
        print("completed sentiment analysis")
        # print(df["sentiment"])

        print("writing to csv")
        df.to_csv('{}/temp_data/pizza_tweets_analysed.csv'.format(self.get_base_path()), index=False)
        print("csv write complete")

        upload.write_to_big_query()

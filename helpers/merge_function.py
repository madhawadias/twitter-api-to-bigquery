from services.twitter_search import SearchTwitter
from services.upload_to_bigquery import WriteToBigQuery
from helpers.google_entities_helper import GoogleEntitiesHelper
from helpers.google_translate_helper import TranslateHelper
from helpers.sentiment_analysis_global_helper import SentimentAnalysisGlobalHelper
from helpers.job_search_helper import JobSearchHelper
from app import get_base_path
import pandas as pd


class MERGE:
    def __init__(self):
        self.SearchTwitter = SearchTwitter
        self._sentiment_analysis_global_helper = SentimentAnalysisGlobalHelper
        self._google_translate_helper = TranslateHelper
        self._google_entities_helper = GoogleEntitiesHelper
        self._job_search_helper = JobSearchHelper
        self.WriteToBigQuery = WriteToBigQuery
        self.get_base_path = get_base_path

    def runner(self, query):
        # initiate twitter search instance
        search = self.SearchTwitter()
        # initiate bigquery upload instance
        upload = self.WriteToBigQuery()
        # initiate google translate instance
        translate = self._google_translate_helper()
        # initiate google sentiment analysis instance
        sentiment_analysis = self._sentiment_analysis_global_helper()
        # initiate google entity extract instance
        extract_entities = self._google_entities_helper()
        # initiate job search nstance
        extract_jobs = self._job_search_helper()

        # send product name to twitter search function
        print("getting data from twitter")
        search.runner(query=query)
        print("end of the search function")

        #  read the created csv file and create a dataframe
        print("read written file")
        df = pd.read_csv("{}/temp_data/tweets.csv".format(self.get_base_path()))
        print("created df with {} rows".format(len(df.index)))
        df = df.dropna(subset=['text'])
        df = df.dropna(subset=['description'])

        #  extract jobs using user description
        print("extract jobs")
        df = extract_jobs.runner(data=df, input_column="description")
        print("extract completed")

        # translate tweets using google translate
        print("translating")
        df = translate.runner(df, "text")
        print("translate complete")

        # clean translated column
        df = df.dropna(subset=['translated'])

        # extract entities using google entitiy extraction
        print("extract entities")
        df = extract_entities.runner(df, "translated")
        print("extract completed")

        # extract sentiment for tweets
        print("start sentiment analysis")
        df = sentiment_analysis.runner(df, "translated")
        print("completed sentiment analysis")

        # write dataframe with analysed data to a csv
        print("writing to csv")
        df.to_csv('{}/temp_data/tweets_analysed.csv'.format(self.get_base_path()), index=False)
        print("csv write complete")

        # upload to bigQuery
        upload.runner()

        # ----------------------------------------------------------------------------------------
        # df = pd.read_csv("{}/data/complete_jobss_database.csv".format(self.get_base_path()))
        # df["job_id"] = "j_"+df.index.astype(str)
        # print(df["job_id"])
        # df.to_csv('{}/data/complete_jobss_database_with_id.csv'.format(self.get_base_path()), index=False)

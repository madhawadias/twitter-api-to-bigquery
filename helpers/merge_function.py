from services.twitter_search import SearchTwitter
from services.upload_to_bigquery import WriteToBigQuery

class MERGE:
    def __init__(self):
        self.SearchTwitter = SearchTwitter
        self.WriteToBigQuery= WriteToBigQuery

    def runner(self,query):
        search = self.SearchTwitter()
        upload = self.WriteToBigQuery()
        search.runner(query=query)
        upload.write_to_big_query()
from google.cloud import bigquery
import os

class WriteToBigQuery:
    def __init__(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'C:/Users/ashen/Documents/kaliso/twitter_analyse/credentials/big_query_credentials.json '
        os.environ["PROJECT_ID"] = "norse-ward-291509"
        self.PROJECT_ID = 'norse-ward-291509'
        self.path = '../temp_data/pizza_tweets.csv'
        self.dataset_id = 'tweets'

    def write_to_big_query(self):
        filename=self.path
        table_id = 'pizza'
        client = bigquery.Client()

        # dataset = client.create_dataset(self.dataset_id)
        # client.create_table('{}.{}.{}'.format(self.PROJECT_ID, self.dataset_id, table_id))

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.allow_quoted_newlines = True
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()

        print("Loaded {} rows into {}:{}.".format(job.output_rows, self.dataset_id, table_id))

        # table = client.get_table(table_id)  # Make an API request.
        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows, len(table.schema), table_id
        #     )
        # )

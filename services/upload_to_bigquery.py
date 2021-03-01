from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

from app import get_base_path


class WriteToBigQuery:
    def __init__(self):
        # os.environ[
        #     "GOOGLE_APPLICATION_CREDENTIALS"] = '{}/configs/api_keys/big_query_credentials.json'.format(get_base_path())
        # os.environ["PROJECT_ID"] = "norse-ward-291509"
        self.PROJECT_ID = 'norse-ward-291509'
        self.path = '{}/temp_data/tweets_analysed.csv'.format(get_base_path())
        self.dataset_id = 'tweets'

    def write_to_big_query(self):
        filename = self.path
        table_id = 'analytics'
        client = bigquery.Client()

        # dataset = client.create_dataset(self.dataset_id)
        try:
            client.get_table('{}.{}.{}'.format(self.PROJECT_ID, self.dataset_id, table_id))
            print("Table {} already exists.".format(table_id))
        except NotFound:
            print("Table {} is not found.".format(table_id))
            print("creating table {}".format(table_id))
            client.create_table('{}.{}.{}'.format(self.PROJECT_ID, self.dataset_id, table_id))
            print("table crated")

        print("Writing Data To Big Query")

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

    def write_df_to_big_query(self, df, table_id, schema):
        client = bigquery.Client()

        #  check whether table exist in bigQuery and if not create table
        try:
            client.get_table('{}.{}.{}'.format(self.PROJECT_ID, self.dataset_id, table_id))
            print("Table {} already exists.".format(table_id))
        except NotFound:
            print("Table {} is not found.".format(table_id))
            print("creating table {}".format(table_id))
            client.create_table('{}.{}.{}'.format(self.PROJECT_ID, self.dataset_id, table_id))
            print("table crated")

        print("Writing Data To Big Query table {}".format(table_id))

        dataset_ref = client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=schema
        )

        #  write df to the bigQuery table
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()

        print("Loaded {} rows into {}:{}".format(job.output_rows, self.dataset_id, table_id))

    def clean_product_id(self, df):
        client = bigquery.Client()
        global product_id
        product_key = ''
        product_id = []

        #  get products table from bigQuery
        products = client.list_rows("{}.{}.product".format(self.PROJECT_ID, self.dataset_id)).to_dataframe()

        #  validate product id with the id's in product table
        for i, row in products.iterrows():
            if str(products.iloc[i]["product_name"]).lower() == str(df.iloc[0]["key"]).lower():
                product_key = products.iloc[i]["product_id"]
                break
        #  if product exist in database update product_id
        if product_key:
            for i in range(len(df)):
                product_id.append(product_key)
            df['product_id'] = product_id

        return df

    def runner(self):
        #  open analysed csv file as a dataframe
        filename = self.path
        df = pd.read_csv(filename)
        #  validate product id
        df = self.clean_product_id(df=df)
        # create main Text table
        main_df = df.filter(
            ["id", "text", "author_id", "username", "product_id", "key", "job_id", "jobs", "income", "translated",
             "entities", "sentiment", "sentiment magnitude"], axis=1)
        main_df = main_df.rename(
            columns={'id': 'text_id', "text": "tweeted_text", 'author_id': 'user_id', "username": "screen_name",
                     "key": "product_name", "sentiment magnitude": "sentiment_magnitude"})
        main_df = main_df.drop_duplicates()
        table_id = 'center'
        schema = [bigquery.SchemaField("tweeted_text", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("text_id", bigquery.enums.SqlTypeNames.INTEGER),
                  bigquery.SchemaField("screen_name", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.INTEGER),
                  bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("jobs", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("income", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("translated", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("entities", bigquery.enums.SqlTypeNames.STRING),
                  bigquery.SchemaField("sentiment", bigquery.enums.SqlTypeNames.FLOAT),
                  bigquery.SchemaField("sentiment_magnitude", bigquery.enums.SqlTypeNames.FLOAT)
                  ]
        #  upload dataframe to the center table of bigQuery
        self.write_df_to_big_query(df=main_df, table_id=table_id, schema=schema)

        # write Tweet Text table
        # tweets_df = df.filter(["id", "text"], axis=1)
        # tweets_df = tweets_df.rename(columns={'id': 'Text_ID', "text": "Tweeted_Text"})
        # tweets_df = tweets_df.drop_duplicates()
        # table_id = 'Tweet Text'
        # schema = [bigquery.SchemaField("Tweeted_Text", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("Text_ID", bigquery.enums.SqlTypeNames.INTEGER)
        #           ]
        # self.write_df_to_big_query(df=tweets_df, table_id=table_id, schema=schema)

        # write User table
        # user_df = df.filter(["author_id", "username"], axis=1)
        # user_df = user_df.rename(columns={'author_id': 'User_ID', "username": "Screen_Name"})
        # user_df = user_df.drop_duplicates()
        # table_id = 'User'
        # schema = [bigquery.SchemaField("Screen_Name", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("User_ID", bigquery.enums.SqlTypeNames.INTEGER)
        #           ]
        # self.write_df_to_big_query(df=user_df, table_id=table_id, schema=schema)

        # write Products table
        # product_df = df.filter(["product_id","key"], axis=1)
        # product_df = product_df.drop_duplicates()
        # product_df = product_df.rename(columns={"key": "product_name"})
        # table_id = 'Product'
        # schema = [bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.STRING)
        #           ]
        # self.write_df_to_big_query(df=product_df, table_id=table_id, schema=schema)

        # write center table
        # main_df = df.filter(["author_id", "id","product_id","job_id"], axis=1)
        # main_df = main_df.rename(columns={'author_id': 'User_ID', 'id': 'Text_ID'})
        # main_df = main_df.drop_duplicates()
        # table_id = 'main'
        # schema = [bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("User_ID", bigquery.enums.SqlTypeNames.INTEGER),
        #           bigquery.SchemaField("Text_ID", bigquery.enums.SqlTypeNames.INTEGER),
        #           bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.STRING)
        #           ]
        # self.write_df_to_big_query(df=main_df, table_id=table_id, schema=schema)

        # tweets_df = df.filter(["job_id", "Job","Income"], axis=1)
        # table_id = 'jobs'
        # schema = [bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("Job", bigquery.enums.SqlTypeNames.STRING),
        #           bigquery.SchemaField("Income", bigquery.enums.SqlTypeNames.STRING)
        #           ]
        # self.write_df_to_big_query(df=tweets_df, table_id=table_id, schema=schema)

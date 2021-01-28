import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def write_to_big_query():
    credentials = service_account.Credentials.from_service_account_file(
    'credentials/big_query_credentials.json')

    dataset_id = 'tweets'
    client = bigquery.Client()

    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    # dataset_id = "{}.your_dataset".format(client.project)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    df = pd.read_excel(
        'temp_data/pizza_tweets.xlsx'
        )

    # df.columns = [u'Underlying_Script_Code', u'Script_Name', u'ISIN_No', u'Remarks']

    table_id = 'dataset.pizza'
    # job_config = bigquery.LoadJobConfig(
    #     WRITE_TRUNCATE='WRITE_TRUNCATE')
    # job = client.load_table_from_dataframe(
    #     df,
    #     table_id,
    #     job_config=job_config
    #     )

    job = client.load_table_from_dataframe(
        df,
        table_id
        )

if __name__ == '__main__':
    write_to_big_query()

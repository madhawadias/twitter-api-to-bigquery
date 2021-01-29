from google.cloud import bigquery
import os


def write_to_big_query():
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"] = 'C:/Users/ashen/Documents/kaliso/twitter_analyse/credentials' \
                                            '/big_query_credentials.json '
    os.environ["PROJECT_ID"] = "norse-ward-291509"
    PROJECT_ID = 'norse-ward-291509'
    filename = 'temp_data/pizza_tweets.csv'
    dataset_id = 'tweets'
    table_id = 'pizza'
    client = bigquery.Client()

    # dataset = client.create_dataset('tweets')
    # client.create_table('{}.{}.{}'.format(PROJECT_ID, dataset_id, table_id))

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.allow_quoted_newlines = True
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))


if __name__ == '__main__':
    write_to_big_query()

# table = client.get_table(table_id)  # Make an API request.
# print(
#     "Loaded {} rows and {} columns to {}".format(
#         table.num_rows, len(table.schema), table_id
#     )
# )

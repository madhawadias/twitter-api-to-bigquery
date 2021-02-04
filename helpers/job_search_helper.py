from flashtext import KeywordProcessor
from app import get_base_path
from dotenv import load_dotenv
# from exceptions.general_exceptions import GeneralExceptionError
# from configs.analytic_status_codes import ANALYTIC_STATUS_CODES
from services.google_translate import TranslationService
import concurrent.futures
import pandas as pd
import re
import os

load_dotenv()


class JobSearchHelper(object):
    def __init__(self):
        # self._MAX_WORKERS = int(os.environ.get("MAX_WORKERS"))
        self.job1 = pd.read_csv('{}/data/complete_jobss_database.csv'.format(get_base_path()))
        job_list = list(self.job1['Titles'].unique())
        self.processor = KeywordProcessor()
        self.processor.add_keywords_from_list(job_list)
        self._bio_en_column = "bio_en"
        self._bio_en_clean_column = "bio_en_clean"
        self._bio_len_column = "bio_len"
        self._jobs_column = "jobs"
        self._income_column = "income"
        self._job_id = None
        self._unique_job_id = None
        self._is_dummy = None

    def _preprocessing_nltk_ngrm(self, text):
        try:
            text = " ".join(filter(lambda x: x[0] != '@', text.split()))
            text = re.sub(r'http\S+', '', text)
            return text
        except:
            return text

    def _job_search(self, document):
        found = self.processor.extract_keywords(document)
        return found

    def _translate_client(self, description: pd.DataFrame, index, input_column, output_column, row):
        translation = TranslationService()
        try:
            _translated_text = translation.translate_text(row[input_column])
            description.at[index, output_column] = _translated_text
        except Exception as err:
            print(err)

    def _get_translation(self, description, input_column, output_column):
        try:
            _threads = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as runner:
                for index, row in description.iterrows():
                    runner.submit(self._translate_client, description, index, input_column, output_column, row)
            return description
        except Exception as e:
            print(e)

    def runner(self, data, input_column, result=[]):
        try:
            dataframe = data.drop_duplicates()
            dataframe = dataframe.reset_index(drop=True)
            dataframe[self._bio_en_column] = None
            self._get_translation(dataframe, input_column, self._bio_en_column)
            dataframe[self._bio_en_clean_column] = dataframe[self._bio_en_column].apply(
                self._preprocessing_nltk_ngrm)
            dataframe = dataframe[dataframe[self._bio_en_clean_column].notnull()]
            dataframe[self._bio_len_column] = dataframe[self._bio_en_clean_column].apply(len)
            dataframe = dataframe[dataframe[self._bio_len_column] > 10]
            dataframe[self._jobs_column] = dataframe[self._bio_en_column].apply(
                lambda x: self._job_search(x) if (str(x[1]) != 'nan') else x)
            dataframe = dataframe[dataframe[self._jobs_column].str.len() != 0]
            dataframe = dataframe.reset_index(drop=True)
            dataframe[self._income_column] = None
            saleries = []
            for i in range(len(dataframe)):
                try:
                    sal = []
                    for x in (dataframe[self._jobs_column][i]):
                        p = self.job1[self.job1['Titles'] == x]
                        p = p.reset_index(drop=True)
                        sal.append(p['Average salary'][0])
                    # dataframe[self._income_column][i] = sal
                    saleries.append(sal)
                except:
                    pass
            dataframe[self._income_column] = saleries
            dataframe = dataframe[[input_column, self._jobs_column, self._income_column]]
            data = pd.merge(data, dataframe, on=input_column, how='left')
            # result.append(data)
            data = data.dropna(subset=[self._jobs_column])
            data = data.reset_index(drop=True)
            return data
        except Exception as e:
            print(e)
            # raise GeneralExceptionError(e)

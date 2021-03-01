from flashtext import KeywordProcessor
from app import get_base_path
from dotenv import load_dotenv
# from exceptions.general_exceptions import GeneralExceptionError
# from configs.analytic_status_codes import ANALYTIC_STATUS_CODES
from services.google_translate import TranslationService
import concurrent.futures
import pandas as pd
import numpy as np
import re
import os

load_dotenv()


class JobSearchHelper(object):
    def __init__(self):
        # self._MAX_WORKERS = int(os.environ.get("MAX_WORKERS"))
        self.job1 = pd.read_csv('{}/data/complete_jobss_database_with_id.csv'.format(get_base_path()))
        job_list = list(self.job1['Titles'].unique())
        self.processor = KeywordProcessor()
        self.processor.add_keywords_from_list(job_list)
        self._bio_en_column = "bio_en"
        self._bio_en_clean_column = "bio_en_clean"
        self._bio_len_column = "bio_len"
        self._jobs_column = "jobs"
        self._income_column = "income"
        self._job_id = "job_id"
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
        # extract jobs from given text
        found = self.processor.extract_keywords(document)
        return found

    def _translate_client(self, description: pd.DataFrame, index, input_column, output_column, row):
        # translate using google translate api
        translation = TranslationService()
        try:
            _translated_text = translation.translate_text(row[input_column])
            description.at[index, output_column] = _translated_text
        except Exception as err:
            print(err)

    def _get_translation(self, description, input_column, output_column):
        # translate using google translate api
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
            #  drop duplicate rows from dataframe
            dataframe = data.drop_duplicates()
            dataframe = dataframe.reset_index(drop=True)
            #  create empty column to contain english translation of user description
            dataframe[self._bio_en_column] = None
            #  get the translation
            self._get_translation(dataframe, input_column, self._bio_en_column)
            #  clean translated data
            dataframe[self._bio_en_clean_column] = dataframe[self._bio_en_column].apply(
                self._preprocessing_nltk_ngrm)
            dataframe = dataframe[dataframe[self._bio_en_clean_column].notnull()]
            #  get the length of translated user descriptions
            dataframe[self._bio_len_column] = dataframe[self._bio_en_clean_column].apply(len)
            # if length is greater than 10 search for jobs
            dataframe = dataframe[dataframe[self._bio_len_column] > 10]
            dataframe[self._jobs_column] = dataframe[self._bio_en_column].apply(
                lambda x: self._job_search(x) if (str(x[1]) != 'nan') else x)
            dataframe = dataframe[dataframe[self._jobs_column].str.len() != 0]
            dataframe = dataframe.reset_index(drop=True)
            dataframe[self._income_column] = None
            dataframe[self._job_id] = None
            lst_col = self._jobs_column

            #  if there are multiple job predictions for description expand them into new rows
            dataframe = pd.DataFrame({
                col: np.repeat(dataframe[col].values, dataframe[lst_col].str.len())
                for col in dataframe.columns.drop(lst_col)}
            ).assign(**{lst_col: np.concatenate(dataframe[lst_col].values)})[dataframe.columns]

            saleries = []
            job_ids = []

            #  extract the job_id and average salary for jobs
            for i in range(len(dataframe)):
                try:
                    x = dataframe[self._jobs_column][i]
                    p = self.job1[self.job1['Titles'] == x]
                    p = p.reset_index(drop=True)
                    saleries.append(p['Average salary'][0])
                    job_ids.append(p['job_id'][0])
                except:
                    pass

            # for i in range(len(dataframe)):
            #     try:
            #         sal = []
            #         ids = []
            #         for x in (dataframe[self._jobs_column][i]):
            #             p = self.job1[self.job1['Titles'] == x]
            #             p = p.reset_index(drop=True)
            #             saleries.append(p['Average salary'][0])
            #             job_ids.append(p['job_id'][0])
            #         # dataframe[self._income_column][i] = sal
            #         # saleries.append(sal)
            #         # job_ids.append(ids)
            #     except:
            #         pass

            dataframe[self._income_column] = saleries
            dataframe[self._job_id] = job_ids
            dataframe = dataframe[[input_column, self._jobs_column, self._income_column, self._job_id]]
            #  put all the new columns into one dataframe
            data = pd.merge(data, dataframe, on=input_column, how='left')
            # result.append(data)
            # clean null data values
            data.loc[data[self._income_column].isnull(), [self._income_column]] = data.loc[
                data[self._income_column].isnull(), self._income_column].apply(lambda x: "-")
            data.loc[data[self._jobs_column].isnull(), [self._jobs_column]] = data.loc[
                data[self._jobs_column].isnull(), self._jobs_column].apply(lambda x: "-")
            data.loc[data[self._job_id].isnull(), [self._job_id]] = data.loc[
                data[self._job_id].isnull(), self._job_id].apply(lambda x: "-")
            data = data.reset_index(drop=True)
            return data
        except Exception as e:
            print(e)
            # raise GeneralExceptionError(e)

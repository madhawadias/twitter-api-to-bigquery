from google.cloud import translate_v2 as translate
from google.oauth2 import service_account
import google.auth.exceptions as google_auth_exceptions
from google.api_core import exceptions
from langdetect import detect
from app import get_base_path
import time,os


class TranslationService(object):
    def __init__(self):
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}/configs/api_keys/big_query_credentials.json'.format(get_base_path())
            os.environ["PROJECT_ID"] = "norse-ward-291509"
            credentials = service_account.Credentials.from_service_account_file('{}/configs/api_keys/big_query_credentials.json'.format(get_base_path()))
            self.translate_client = translate.Client(credentials=credentials)
        except google_auth_exceptions.GoogleAuthError:
            pass
        except exceptions.ClientError:
            pass

    def get_language(self, text):
        #  detect language
        try:
            lang = detect(text)
            return lang
        except Exception as err:
            pass

    def translate_text(self, text):
        #  calling the google translate api
        try:
            if text is not None:
                language = self.get_language(text)
                if language not in ["en", "und"]:
                    translation = self.translate_client.translate(text, target_language='EN')
                    return translation['translatedText']
                else:
                    return text
        except Exception as err:
            if str(err)[:3] == "403":
                try:
                    time.sleep(60)
                    translation = self.translate_client.translate(text, target_language='EN')
                    return translation['translatedText']
                except Exception as err:
                    pass

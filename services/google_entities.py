import google.auth.exceptions as google_auth_exceptions
from google.api_core import exceptions
from google.cloud import language_v1, vision, automl
from google.cloud import translate_v2 as translate
from google.cloud.language_v1.gapic import enums
from google.oauth2 import service_account
from langdetect import detect

from app import get_base_path


class GoogleEntities(object):
    def __init__(self):
        try:
            # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '{}/configs/api_keys/big_query_credentials.json'.format(
            #     get_base_path())
            # os.environ["PROJECT_ID"] = "norse-ward-291509"
            self.credentials = service_account.Credentials.from_service_account_file(
                '{}/configs/api_keys/big_query_credentials.json'.format(get_base_path()))
            self.params = {'score_threshold': '0.1'}
            self.language_service_client = language_v1.LanguageServiceClient()
            self.prediction_client = automl.PredictionServiceClient(credentials=self.credentials)

            self.translate_client = translate.Client()
            self.vision_client = vision.ImageAnnotatorClient()
        except google_auth_exceptions.GoogleAuthError:
            pass
        except exceptions.ClientError:
            pass

    def extract_entities(self, text):
        #  calling google entity extect api
        type_ = enums.Document.Type.PLAIN_TEXT
        document = {"content": text, "type_": type_}
        encoding_type = enums.EncodingType.UTF8
        response = self.language_service_client.analyze_entities(
            request={'document': document, 'encoding_type': encoding_type})
        entities_name = []
        entities_type = []
        for entity in response.entities:
            entities_name.append(entity.name)
            entities_type.append(enums.Entity.Type(entity.type_).name)
        results = {"name": entities_name, "type": entities_type}
        return results

    def translate_text(self, text):
        try:
            if text is not None:
                lang = detect(text)
                if lang != 'en':
                    translation = self.translate_client.translate(text, target_language='en')
                    return translation['translatedText']
                else:
                    return text
            else:
                return text
        except:
            pass

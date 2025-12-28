import sys
import langdetect
try:
    import vaex
    _VAEX_AVAILABLE = True
except ImportError:
    _VAEX_AVAILABLE = False
    
from lingua import Language, LanguageDetectorBuilder



from deep_translator import (GoogleTranslator,
                             ChatGptTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)
class TranslateAttributesTransformer:
    def __init__(self, idiom):

        self.idiom = idiom
        self.translator = GoogleTranslator(source=idiom, target='english')
        # self.result = self.translator.translate(text='edad')
        # #
        # print(f"Translation using source = {self.translator.source} and target = {self.translator.target} -> {self.result}")

    def detect_language(self, text):
        languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH]
        detector = LanguageDetectorBuilder.from_languages(*languages).build()
        result = detector.detect_language_of(text)

        print(text, result.name)
        try:
            return result.name if result else 'Unknown'
            # return langdetect.detect(text)
        except langdetect.lang_detect_exception.LangDetectException:
            return 'unknown'

    # def detect_language_attributes(self, data):
    #     try:
    #         return langdetect.detect(text)
    #     except langdetect.lang_detect_exception.LangDetectException:
    #         return 'unknown'

    def translate(self, text, dest_language='en'):

        try:
            translation = self.translator.translate(text=text)
            return translation
        except Exception as e:
            print(f"Translation error: {e}")
            return text

    def transform(self, data):
        new_columns = {}
        for column in data.columns:
            detected_lang = self.detect_language(column)
            print(detected_lang, '--------------------------------------------------')
            if detected_lang != 'ENGLISH':
                new_column_name = self.translate(column)
                new_columns[column] = new_column_name

        try:
            data = data.rename(columns=new_columns)
        except Exception as e:
            cont = 0
            for old_name, new_name in new_columns.items():
                if _VAEX_AVAILABLE and isinstance(data, vaex.dataframe.DataFrame):
                     try:
                        data.rename(old_name, new_name)
                     except Exception as rename_error:
                        print(f"Error renaming column '{old_name}' to '{new_name}': {rename_error}")
                else:
                    # Pass or handle other types if needed
                    pass
                cont += 1

        return data
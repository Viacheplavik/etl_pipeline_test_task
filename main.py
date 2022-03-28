import logging
import os
import shutil
import zipfile
from typing import Dict, List

import pandas as pd
import requests
import yaml

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)06d %(module)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

CHUNK_SIZE = 8192


class KingCountyEtlPipeline:
    def __init__(self,
                 scheme: str,
                 host: str,
                 path: str,
                 input_folder: str,
                 unpacked_folder: str,
                 output_folder: str,
                 main_file: str,
                 used_archives: List[str],
                 secondary_files: Dict[str, List[str]],
                 chunk_size: int,
                 required_fields: List[str],
                 encoding: str
                 ):
        self.scheme = scheme
        self.host = host
        self.path = path
        self.input_folder = input_folder
        self.unpacked_folder = unpacked_folder
        self.output_folder = output_folder
        self.main_file = main_file
        self.used_archives = used_archives
        self.secondary_files = secondary_files
        self.chunk_size = chunk_size
        self.required_fields = required_fields
        self.encoding = encoding

    def download_file(self, url: str, input_folder_name: str, input_file_name: str):
        logger.info('Started loading %s...', input_file_name)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            if not os.path.exists(input_folder_name):
                os.mkdir(input_folder_name)
            with open(f'{input_folder_name}/{input_file_name}', 'wb') as f:
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    logger.info('Loaded %s bytes', self.chunk_size)
                    f.write(chunk)

    def exec_download_file(self):
        for file_name in self.used_archives:
            url = self.scheme + self.host + self.path + file_name
            in_file_name = file_name.replace('%20', '_')
            self.download_file(url, self.input_folder, in_file_name)

    def extract_archives(self):
        for zip_file_name in os.listdir(self.input_folder):
            if not os.path.exists(self.unpacked_folder):
                os.mkdir(self.unpacked_folder)
            with zipfile.ZipFile(f'{self.input_folder}/{zip_file_name}') as zip_file:
                logger.info('Started extracting %s content', zip_file_name)
                for member in zip_file.namelist():
                    filename = os.path.basename(member)
                    # skip directories
                    if not filename:
                        continue
                    source = zip_file.open(member)
                    target = open(os.path.join(self.unpacked_folder, filename), "wb")
                    with source, target:
                        shutil.copyfileobj(source, target)
        KingCountyEtlPipeline.clean(self.input_folder)

    @staticmethod
    def clean(folder_path: str):
        logger.info('Cleaning %s directory...', folder_path)
        shutil.rmtree(folder_path)

    @staticmethod
    def fix_id_len(df, required_len_in_column):
        for column in required_len_in_column:
            values_list = df[column].values
            for index, el in enumerate(values_list):
                if len(el) < required_len_in_column[column]:
                    values_list[index] = el.zfill(required_len_in_column[column])

    def transform(self):

        na_rate = dict()

        dict_conv = {'Major': str,
                     'Minor': str,
                     'YrBuilt': int,
                     'Footage': int,
                     'SqFtTotLiving': int}

        main_input_file_path = os.path.join(self.unpacked_folder, self.main_file)
        main_df = pd.read_csv(f'{main_input_file_path}.csv', converters=dict_conv, encoding=self.encoding)

        self.fix_id_len(main_df, {'Major': 6, 'Minor': 4})

        main_df['pin'] = main_df.Major + main_df.Minor

        for target_field in self.secondary_files:
            empty_dataframe = pd.DataFrame({})
            for real_field in self.secondary_files[target_field]:

                for file in self.secondary_files[target_field][real_field]:
                    added_file_path = os.path.join(self.unpacked_folder, file)
                    added_df = pd.read_csv(added_file_path, converters=dict_conv)

                    added_df.rename(columns={real_field: target_field}, inplace=True)

                    empty_dataframe = pd.concat([empty_dataframe, added_df])
            try:
                self.fix_id_len(empty_dataframe, {'Major': 6, 'Minor': 4})
                empty_dataframe['pin'] = empty_dataframe.Major + empty_dataframe.Minor
                main_df = pd.merge(main_df, empty_dataframe, how='left', on='pin', suffixes=('', '_redundant'))
            except AttributeError:
                # doesn't have a Minor id
                self.fix_id_len(empty_dataframe, {'Major': 6})
                main_df = pd.merge(main_df, empty_dataframe, how='left', on='Major', suffixes=('', '_redundant'))

        main_df = main_df[self.required_fields]

        for additional_column in self.secondary_files:
            na_rate[additional_column] = main_df[additional_column].isna().sum() / main_df[additional_column].size * 100
        logger.info('Na rate is %s', na_rate)

        return main_df

    def load(self):
        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder)
        main_output_file_path = os.path.join(self.output_folder, self.main_file)
        result_df = self.transform()

        result_df.to_csv(f'{main_output_file_path}.csv', sep=',', index=False)
        result_df.to_parquet(f'{main_output_file_path}.parquet')
        KingCountyEtlPipeline.clean(self.unpacked_folder)


def main():
    with open('conf.yaml') as file:
        conf = yaml.load(file, yaml.Loader)

    scheme = conf['extract']['scheme']
    host = conf['extract']['host']
    path = conf['extract']['path']

    input_folder = conf['directories']['input_folder']
    unpacking_folder = conf['directories']['unpacking_folder']
    output_folder = conf['directories']['output_folder']

    main_file = conf['files']['main_file']
    used_archives = conf['files']['used_archives']
    secondary_files = conf['files']['secondary_files']

    required_fields = conf['files']['required_fields']

    encoding = conf['files']['encoding']

    king_county_etl_pipeline = KingCountyEtlPipeline(
        scheme=scheme,
        host=host,
        path=path,
        input_folder=input_folder,
        unpacked_folder=unpacking_folder,
        output_folder=output_folder,
        main_file=main_file,
        used_archives=used_archives,
        secondary_files=secondary_files,
        chunk_size=CHUNK_SIZE,
        required_fields=required_fields,
        encoding=encoding
    )

    king_county_etl_pipeline.exec_download_file()
    king_county_etl_pipeline.extract_archives()
    king_county_etl_pipeline.load()


if __name__ == '__main__':
    main()

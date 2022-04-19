import logging
import os
import shutil
import zipfile
from typing import Dict, List
import asyncio
import dask
from dask import delayed
import dask.dataframe as dd
import pandas as pd
import requests
import yaml

from dask.distributed import Client

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
                 # main_file: str,
                 used_archives: List[str],
                 # secondary_files: Dict[str, List[str]],
                 chunk_size: int,
                 # required_fields: List[str],
                 # encoding: str
                 ):
        self.scheme = scheme
        self.host = host
        self.path = path
        self.input_folder = input_folder
        self.unpacked_folder = unpacked_folder
        self.output_folder = output_folder
        # self.main_file = main_file
        self.used_archives = used_archives
        # self.secondary_files = secondary_files
        self.chunk_size = chunk_size
        # self.required_fields = required_fields
        # self.encoding = encoding

    @delayed
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

    async def exec_download_file(self):
        delayed_extraction_files = []
        for file_name in self.used_archives:
            url = self.scheme + self.host + self.path + file_name
            in_file_name = file_name.replace('%20', '_')
            delayed_extraction_files.append(self.download_file(url, self.input_folder, in_file_name))
        dask.compute(delayed_extraction_files)

    @delayed
    def extract_archives(self, zip_file_name):

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
        # KingCountyEtlPipeline.clean(self.input_folder)

    async def exec_file_extraction(self):
        download_task = asyncio.create_task(self.exec_download_file())

        await download_task
        delayed_file_extraction = []
        for zip_file_name in os.listdir(self.input_folder):
            delayed_file_extraction.append(self.extract_archives(zip_file_name))
        dask.compute(delayed_file_extraction)

    @delayed
    def clean(self, folder_path: str):

        logger.info('Cleaning %s directory...', folder_path)
        shutil.rmtree(folder_path)

    def exec_clean(self):
        delayed_cleaning = []
        removing_folders = [self.input_folder]

        for folder in removing_folders:
            delayed_cleaning.append(self.clean(folder))

        dask.compute(delayed_cleaning)


def main():

    client = Client()

    with open('conf.yaml') as file:
        conf = yaml.load(file, yaml.Loader)

    scheme = conf['extract']['scheme']
    host = conf['extract']['host']
    path = conf['extract']['path']

    input_folder = conf['directories']['input_folder']
    unpacking_folder = conf['directories']['unpacking_folder']
    output_folder = conf['directories']['output_folder']

    used_archives = conf['files']['used_archives']

    etl_pipeline = KingCountyEtlPipeline(
        scheme=scheme,
        host=host,
        path=path,
        input_folder=input_folder,
        unpacked_folder=unpacking_folder,
        output_folder=output_folder,
        chunk_size=CHUNK_SIZE,
        used_archives=used_archives

    )
    asyncio.run(etl_pipeline.exec_file_extraction())
    etl_pipeline.exec_clean()


if __name__ == '__main__':
    main()

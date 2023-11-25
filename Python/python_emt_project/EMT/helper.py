import io
import logging
import re
import time
import zipfile

import requests
from typing import List


def csv_from_zip(url: str, **kwargs) -> List[io.StringIO]:
    """
    Extract CSV data from a zip archive located at a given URL and return a list of StringIO objects.

    Args:
        url (str): The URL of the zip archive to retrieve and extract CSV data from.
        max_attempts (int, optional): The maximum number of retry attempts in case of connection errors.
            Default is 3.
        sleep_time (int, optional): The duration (in seconds) to wait between retry attempts. Default is 5.

    Returns:
        List[io.StringIO]: A list of StringIO objects, each containing the CSV data extracted from a file within the zip archive.

    Raises:
        ConnectionError: Raised when there is an error while fetching the file or when the maximum number of retry attempts is reached.
    """  # noqa:E501
    max_attempts = kwargs.get('max_attempts', 3)
    sleep_time = kwargs.get('sleep_time', 5)
    string_io_list: List[io.StringIO] = []

    for attempt in range(max_attempts):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                logging.info('File GET correctly')
                bytes = io.BytesIO(r.content)
                zfile = zipfile.ZipFile(bytes)

                for file in zfile.filelist:
                    if 'MACOSX' in file.filename:
                        break

                    with zfile.open(file.filename) as zip_file:
                        contents = zip_file.read()
                        contentstr = contents.decode('utf-8',
                                                     errors='replace')
                        fstr = io.StringIO(contentstr)
                        string_io_list.append(fstr)

                return string_io_list
            else:
                raise ConnectionError('Error while fetching')
        except Exception as e:
            logging.error(f'There was an error while fetching the file: {e}')

            if attempt == max_attempts - 1:
                raise ConnectionError('Max retries reached')

            logging.info(f'Retrying in {sleep_time} seconds...')
            time.sleep(sleep_time)


def get_links(content: str, pattern) -> List[str]:

    matches = re.findall(pattern, content)

    if not matches:
        logging.warn('No valid url found')

    return matches

#!/usr/bin/env python

from datetime import datetime
from json import dump
from os import makedirs
from os.path import getsize
from functools import wraps
from time import time
from typing import cast, Dict, List

from bs4 import BeautifulSoup  # type: ignore
from requests import get
from requests.exceptions import HTTPError


class HistoryDownloader:
    """Downloads draws history from Mega-Sena website
    and saves in HTML, JSON and CSV formats"""

    def __init__(self, folder: str) -> None:
        """folder: folder where files will be saved"""
        self.url = (
            'http://loterias.caixa.gov.br/wps/portal/loterias/landing/megasena'
            '/!ut/p/a1/04_Sj9CPykssy0xPLMnMz0vMAfGjzOLNDH0MPAzcDbwMPI0sDBxNXAO'
            'MwrzCjA0sjIEKIoEKnN0dPUzMfQwMDEwsjAw8XZw8XMwtfQ0MPM2I02-AAzgaENIf'
            'rh-FqsQ9wNnUwNHfxcnSwBgIDUyhCvA5EawAjxsKckMjDDI9FQE-F4ca/dl5/d5/L'
            '2dBISEvZ0FBIS9nQSEh/pw/Z7_HGK818G0K8DBC0QPVN93KQ10G1/res/id=histo'
            'ricoHTML/c=cacheLevelPage/=/'
        )

        self.path = datetime.now().strftime('ms_%y%m%d_%H%M%S')
        if folder:
            makedirs(folder, exist_ok=True)
            self.path = folder + '/' + self.path

        self.json_data = []  # type: List[Dict[str, str]]
        self.csv_data = ''

    def download_html(self) -> None:
        """Downloads file from url"""
        response = get(self.url)

        file_path = self.path + '.html'

        with open(file_path, 'w', encoding='utf-8') as file:
            try:
                response.raise_for_status()
                file.write(response.text)
            except HTTPError as err:
                raise SystemExit(err)

        print(f'{file_path} ({getsize(file_path):,} bytes) downloaded.')

    def scrape_html(self) -> None:
        """Scrapes HTML file and save data into json_data and csv_data"""
        try:
            with open(self.path + '.html', 'r', encoding='utf-8') as html_file:
                content = html_file.read()
                bs = BeautifulSoup(content, 'lxml')
                table = bs.find('table')
                fields = [item.text.strip() for item in table.find_all('th')]
                rows = [i for i in table.find_all('tr') if not i.attrs]

                all_draws = []
                csv_data = ';'.join(fields) + '\n'
                for row in rows:
                    row_data = [i.text.strip() for i in row.find_all('td')]

                    if row_data:
                        if len(row_data) > len(fields):
                            # Redundancy removal
                            start = 16
                            end = 16 + 2 * len(row_data[15].split('\n\n'))
                            del row_data[start:end]

                            # Separator change
                            cities = row_data[15]
                            cities = cities.replace('\n\n', '|')
                            cities = cities.replace('\n', '/')
                            cities = cities.replace(' /', '/')
                            row_data[15] = cities

                        if len(row_data) == len(fields):
                            row_data = [i.replace('\n', '') for i in row_data]
                            row_dict = {f: d for (f, d)
                                        in zip(fields, row_data)}
                            all_draws.append(row_dict)
                            csv_data += ';'.join(row_data) + '\n'

            self.json_data = all_draws
            self.csv_data = csv_data

        except Exception as error:
            print(error)

    def write_file(self, file_format: str) -> None:
        """Saves CSV or JSON data into a file"""
        file_path = f'{self.path}.{file_format}'

        with open(file_path, 'w', encoding='utf-8') as file:
            if self.csv_data and file_format == 'csv':
                file.write(self.csv_data)

            if self.json_data and file_format == 'json':
                dump(self.json_data, file, ensure_ascii=False, indent=2)

        print(f'{file_path} ({getsize(file_path):,} bytes) created.')

    def check_consistency(self) -> None:
        """Checks if JSON data has all draws.
        If not, prints what is missing"""
        if not self.json_data:
            raise Exception('Empty list of draws.')

        DRAW_KEY = 'Concurso'

        last_draw_number = int(self.json_data[-1][DRAW_KEY])
        expected_draws = {item + 1 for item in range(last_draw_number)}
        found_draws = {int(item[DRAW_KEY]) for item in self.json_data}
        missing_draws = expected_draws - found_draws

        if missing_draws:
            formatted_list = ', '.join(sorted(cast(str, missing_draws)))
            message = (
                f'Incomplete set of draws.'
                f'{len(missing_draws)} missing: {formatted_list}.'
            )
            raise Exception(message)

    def log_info(self) -> None:
        """Prints the number and the date of the last draw"""
        if self.json_data:
            last_draw_number = self.json_data[-1]['Concurso']
            last_draw_date = self.json_data[-1]['Data do Sorteio']

            print(f'Last draw: {last_draw_number} in {last_draw_date}.')


def show_elapsed_time(f):
    """Decorator that prints the elapsed time in seconds"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time()
        result = f(*args, **kwargs)
        end = time()
        interval = round(end - start, 2)
        print(f'Elapsed time: {interval} seconds.')
        return result
    return wrapper


@show_elapsed_time
def main():
    """Downloads HTML file from Mega-Sena website,
    scrapes it and saves data info a JSON and a CSV files
    """
    hd = HistoryDownloader('data')

    hd.download_html()
    hd.scrape_html()
    hd.check_consistency()
    hd.write_file('json')
    hd.write_file('csv')
    hd.log_info()


if __name__ == '__main__':
    main()

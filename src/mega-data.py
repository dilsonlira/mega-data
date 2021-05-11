#!/usr/bin/env python

from csv import DictReader
from datetime import datetime
from json import dump
from os import makedirs
from os.path import getsize
from functools import wraps
from time import time
from typing import cast, Dict, List

from easierlog import log  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
from requests import get
from requests.exceptions import HTTPError

from dbmanager import DatabaseManager


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


def print_file_info(path: str, text: str) -> None:
    """Prints file size"""
    print(f'{path} ({getsize(path):,} bytes) {text}.')


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

        print_file_info(file_path, 'downloaded')

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
                            cities = cities.replace('\n', ', ')
                            cities = cities.replace(' , ', ', ')
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

        print_file_info(file_path, 'created')

    def check_consistency(self) -> None:
        """Checks if JSON data has all draws. If not, prints what is missing"""
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

    def create_db_csv_files(self):
        """Create csv files to load data into database"""
        def read_location(location: str) -> (str, str):
            """Returns city and state from location string"""
            if location == 'IMBITUVA, PR, PR':
                # Data correction
                location = 'IMBITUVA, PR'
            parts = location.split(', ')
            if len(parts) == 0:
                city, state = '', ''
            elif len(parts) == 1:
                city, state = '', location
            elif len(parts) == 2:
                city, state = parts
            else:
                city, state = '', ''
                log(location)
            return city, state

        def convert_date(date: str) -> str:
            """Converts a date string in the opposite order"""
            date = '/'.join(reversed(date.split('/')))
            return date

        def float_from_string(s: str) -> float:
            """Converts to float a number string, removing thousands separators
            and sets comma as deciamal separator
            """
            s = s.replace('.', '')
            s = s.replace(',', '.')
            return float(s)

        def check_bool(bool_as_text: str) -> [int, None]:
            """Returns integer if argument is boolean string.
            Logs argument otherwise
            """
            if bool_as_text == 'SIM':
                return 1
            if bool_as_text == 'NAO':
                return 0
            log(bool_as_text)

        DRAW_KEY = 'Concurso'
        LOCATION_KEY = 'Local'
        DATE_KEY = 'Data do Sorteio'
        NUMBER_1_KEY = 'Coluna 1'
        NUMBER_2_KEY = 'Coluna 2'
        NUMBER_3_KEY = 'Coluna 3'
        NUMBER_4_KEY = 'Coluna 4'
        NUMBER_5_KEY = 'Coluna 5'
        NUMBER_6_KEY = 'Coluna 6'
        WINNERS_6_KEY = 'Ganhadores Faixa 1'
        WINNERS_5_KEY = 'Ganhadores Faixa 2'
        WINNERS_4_KEY = 'Ganhadores Faixa 3'
        PRIZE_6_KEY = 'Rateio Faixa 1'
        PRIZE_5_KEY = 'Rateio Faixa 2'
        PRIZE_4_KEY = 'Rateio Faixa 3'
        WINNERS_LOCATIONS_KEY = 'Cidade'
        COLLECTED_VALUE_KEY = 'Valor Arrecadado'
        NEXT_PRIZE_KEY = 'Estimativa para o próximo concurso'
        TO_NEXT_DRAW_KEY = 'Valor Acumulado Próximo Concurso'
        JACKPOT_KEY = 'Acumulado'
        SPECIAL_DRAW_KEY = 'Sorteio Especial'
        NOTE_KEY = 'Observação'

        draws_header = (
            'draw_number;draw_city;draw_state;date;'
            'number_1;number_2;number_3;number_4;number_5;number_6;'
            'winners_6;winners_5;winners_4;'
            'prize_6;prize_5;prize_4;'
            'collected_value;next_prize;to_next_draw;'
            'jackpot;special_draw;note\n'
        )

        winners_locations_header = 'draw_number;winner_city;winner_state\n'

        source_path = f'{self.path}.csv'
        self.draws_load_path = f'{self.path}_draws_table_load.csv'
        self.winloc_load_path = f'{self.path}_winloc_table_load.csv'

        with open(source_path, mode='r') as csv_file:
            with open(self.draws_load_path, mode='w') as draws_file:
                draws_file.write(draws_header)
                with open(self.winloc_load_path, mode='w') as winloc_file:
                    winloc_file.write(winners_locations_header)

                    rows = DictReader(csv_file, delimiter=';')
                    for row in rows:

                        # Draws table population
                        draw_number = int(row[DRAW_KEY])
                        city, state = read_location(row[LOCATION_KEY])
                        date = convert_date(row[DATE_KEY])
                        number_1 = int(row[NUMBER_1_KEY])
                        number_2 = int(row[NUMBER_2_KEY])
                        number_3 = int(row[NUMBER_3_KEY])
                        number_4 = int(row[NUMBER_4_KEY])
                        number_5 = int(row[NUMBER_5_KEY])
                        number_6 = int(row[NUMBER_6_KEY])
                        winners_6 = int(row[WINNERS_6_KEY])
                        winners_5 = int(row[WINNERS_5_KEY])
                        winners_4 = int(row[WINNERS_4_KEY])
                        prize_6 = float_from_string(row[PRIZE_6_KEY])
                        prize_5 = float_from_string(row[PRIZE_5_KEY])
                        prize_4 = float_from_string(row[PRIZE_4_KEY])
                        collected = float_from_string(row[COLLECTED_VALUE_KEY])
                        next_prize = float_from_string(row[NEXT_PRIZE_KEY])
                        to_next_draw = float_from_string(row[TO_NEXT_DRAW_KEY])
                        jackpot = check_bool(row[JACKPOT_KEY])
                        special_draw = check_bool(row[SPECIAL_DRAW_KEY])
                        note = row[NOTE_KEY]

                        line = (
                            f'{draw_number};{city};{state};{date};'
                            f'{number_1};{number_2};{number_3};'
                            f'{number_4};{number_5};{number_6};'
                            f'{winners_6};{winners_5};{winners_4};'
                            f'{prize_6};{prize_5};{prize_4};'
                            f'{collected};{next_prize};{to_next_draw};'
                            f'{jackpot};{special_draw};{note}\n'
                        )

                        draws_file.write(line)

                        # winners_locations table population
                        winners_locations = row[WINNERS_LOCATIONS_KEY]
                        if winners_locations:
                            locations = winners_locations.split('|')
                            for location in locations:
                                city, state = read_location(location)

                                line = f'{draw_number};{city};{state}\n'

                                winloc_file.write(line)

        print_file_info(self.draws_load_path, 'created')
        print_file_info(self.winloc_load_path, 'created')


@show_elapsed_time
def main():
    """
    Downloads history from Mega-Sena website
    and loads data into a MySQL database
    """

    hd = HistoryDownloader('scraper-data')
    hd.download_html()
    hd.scrape_html()
    hd.check_consistency()
    hd.write_file('json')
    hd.write_file('csv')
    hd.create_db_csv_files()
    hd.log_info()

    dm = DatabaseManager(hd.draws_load_path, hd.winloc_load_path)
    dm.create_tables()
    dm.insert_data()
    dm.make_query()


if __name__ == '__main__':
    main()

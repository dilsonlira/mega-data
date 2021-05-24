#!/usr/bin/env python

from functools import wraps
from time import time

from easierlog import log  # type: ignore

from historydownloader import HistoryDownloader
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
    # dm.make_query()
    log(dm.get_draw_data(2000))
    log(dm.get_draw_data(3000))


if __name__ == '__main__':
    main()

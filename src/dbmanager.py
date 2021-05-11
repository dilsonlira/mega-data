#!/usr/bin/env python

from csv import reader

from mysql.connector import connect  # type: ignore


class DatabaseManager:
    """Connects, performs initial setup and inserts data into MySQL database"""

    def __init__(self, draws_data_path: str, winloc_data_path: str) -> None:
        """ draws_data_path: draws csv file location
            winloc_data_path: winloc csv file location"""

        for path in (draws_data_path, winloc_data_path):
            try:
                with open(path, mode='r') as file:
                    reader(file, delimiter=';')
            except Exception as err:
                raise SystemExit(err)

        self.draws_data_path = draws_data_path
        self.winloc_data_path = winloc_data_path

        HOST = 'db'
        USER = 'root'
        PASSWORD_FILE_PATH = '/run/secrets/db-password'
        DATABASE = 'mega'
        CHARSET = 'latin1'
        COLLATION = 'latin1_general_ci'

        with open(PASSWORD_FILE_PATH, 'r') as password_file:
            self.connection = connect(
                host=HOST,
                user=USER,
                password=password_file.read(),
                database=DATABASE,
                charset=CHARSET,
                collation=COLLATION
            )
        self.cursor = self.connection.cursor()

    def create_tables(self) -> None:
        """Creates database tables"""
        self.cursor.execute('DROP TABLE IF EXISTS winners_locations')
        self.cursor.execute('DROP TABLE IF EXISTS draws')

        self.cursor.execute(
            '''
            CREATE TABLE draws (
                draw_number INT AUTO_INCREMENT PRIMARY KEY,
                draw_city VARCHAR(50) NOT NULL,
                draw_state CHAR(2) NOT NULL,
                date DATE NOT NULL,
                number_1 TINYINT UNSIGNED NOT NULL,
                number_2 TINYINT UNSIGNED NOT NULL,
                number_3 TINYINT UNSIGNED NOT NULL,
                number_4 TINYINT UNSIGNED NOT NULL,
                number_5 TINYINT UNSIGNED NOT NULL,
                number_6 TINYINT UNSIGNED NOT NULL,
                winners_6 INT UNSIGNED NOT NULL,
                winners_5 INT UNSIGNED NOT NULL,
                winners_4 INT UNSIGNED NOT NULL,
                prize_6 DECIMAL(12,2) NOT NULL,
                prize_5 DECIMAL(12,2) NOT NULL,
                prize_4 DECIMAL(12,2) NOT NULL,
                collected_value DECIMAL(13,2) NOT NULL,
                next_prize DECIMAL(12,2) NOT NULL,
                to_next_draw DECIMAL(12,2) NOT NULL,
                jackpot TINYINT UNSIGNED NOT NULL,
                special_draw TINYINT UNSIGNED NOT NULL,
                note VARCHAR(400) NOT NULL
            )
            '''
        )

        self.cursor.execute(
            '''
            CREATE TABLE winners_locations (
                draw_number INT NOT NULL,
                winner_city VARCHAR(50) NOT NULL,
                winner_state CHAR(2) NOT NULL,
                FOREIGN KEY (draw_number) REFERENCES draws (draw_number)
            )
            '''
        )

        self.connection.commit()

    def insert_data(self) -> None:
        """Loads each CSV file into database table"""
        self.cursor.execute('SET GLOBAL local_infile = 1')

        self.cursor.execute(
            f'''
            LOAD DATA LOCAL INFILE '{self.draws_data_path}'
            INTO TABLE draws
            FIELDS TERMINATED BY ';'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
            (
                draw_number,draw_city,draw_state,date,
                number_1,number_2,number_3,number_4,number_5,number_6,
                winners_6,winners_5,winners_4,
                prize_6,prize_5,prize_4,
                collected_value,next_prize,to_next_draw,
                jackpot,special_draw,note
            )
            '''
        )

        self.cursor.execute(
            f'''
            LOAD DATA LOCAL INFILE '{self.winloc_data_path}'
            INTO TABLE winners_locations
            FIELDS TERMINATED BY ';'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS
            (draw_number, winner_city, winner_state)
            '''
        )

        self.connection.commit()

        print('Database load completed.')

    def make_query(self) -> None:
        """Makes a query into database to retrieve the last 10 draws"""
        self.cursor.execute(
            '''
            SELECT * FROM (
                SELECT draw_number, date, winners_6, winners_5, winners_4
                FROM draws
                ORDER BY draw_number DESC LIMIT 10
            ) sub
            ORDER BY draw_number ASC
            '''
        )
        result = self.cursor.fetchall()

        print('Last 10 records:')
        print('(draw_number, date, winners_6, winners_5, winners_4)')
        [print(line) for line in result]  # type: ignore

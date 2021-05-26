#!/usr/bin/env python

from mysql.connector import connect  # type: ignore
from flask import Flask, abort, jsonify


server = Flask(__name__)

HOST = 'db'
USER = 'root'
PASSWORD_FILE_PATH = '/run/secrets/db-password'
DATABASE = 'mega'
CHARSET = 'latin1'
COLLATION = 'latin1_general_ci'

with open(PASSWORD_FILE_PATH, 'r') as password_file:
    connection = connect(
        host=HOST,
        user=USER,
        password=password_file.read(),
        database=DATABASE,
        charset=CHARSET,
        collation=COLLATION
    )
cursor = connection.cursor()


@server.route('/<int:draw_number>')
def get_draw_data(draw_number):
    """Retrieves a JSON containing:
        - draw_number,
        - date,
        - the six numbers of the draw"""
    cursor.execute(
        f'''
        SELECT
            date,
            number_1, number_2, number_3, number_4, number_5, number_6
        FROM draws
        WHERE draw_number = {draw_number}
        '''
    )
    result = cursor.fetchone()

    if result:
        draw_date, *numbers = result
        numbers_list = [str(number).zfill(2) for number in numbers]
        data = {
                'draw_number': draw_number,
                'date': draw_date.strftime('%Y-%m-%d'),
                'numbers': '-'.join(numbers_list)
                }
        return jsonify(data)
    else:
        abort(404)


if __name__ == '__main__':
    server.run(host='0.0.0.0')

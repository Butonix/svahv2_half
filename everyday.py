import vk_api
from vk_api import VkUpload
from vk_api.utils import get_random_id
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import config
import keyboards
import pymysql
import pymysql.cursors
import requests
import threading
import re
import scripts
import time


def get_connection():
    connection = pymysql.connect(host=config.mysqlhost,
                                 user=config.mysqlusername,
                                 password=config.mysqlpassword,
                                 db=config.mysqldbname,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def take_data_every_day():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_user FROM user"
            cursor.execute(sql)
            data = cursor.fetchall()
            rc_all = cursor.rowcount
            sql = "SELECT id_user FROM user WHERE gender = 0"
            cursor.execute(sql)
            data = cursor.fetchall()
            rc_men = cursor.rowcount
            sql = "SELECT id_user FROM user WHERE gender = 1"
            cursor.execute(sql)
            data = cursor.fetchall()
            rc_women = cursor.rowcount
            send_men = rc_women//3
            send_women = rc_men//3
            sql = "UPDATE start_data SET form_number = %s, number_women = %s, number_men = %s, number_send_men = %s," \
                  "number_send_women = %s WHERE idstart = 1"
            cursor.execute(sql, (rc_all, rc_women, rc_men, send_men, send_women))
        connection.commit()
    finally:
        connection.close()
    return

def update_men_women():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT number_send_women, number_send_men FROM start_data WHERE idstart = 1"
            cursor.execute(sql)
            data = cursor.fetchone()
            sql = "UPDATE user SET number_see = %s WHERE gender = 0"
            cursor.execute(sql, (data["number_send_men"]))
            sql = "UPDATE user SET number_see = %s WHERE gender = 1"
            cursor.execute(sql, (data["number_send_women"]))
        connection.commit()
    finally:
        connection.close()
    return

if __name__ == '__main__':
    take_data_every_day()
    update_men_women()

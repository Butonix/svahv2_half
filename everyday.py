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

#def take_data_every_day():
    #удалена функция ежедневного сбора данных

#def update_men_women():
    #удалена функция ежедневного обновления данных

if __name__ == '__main__':
    take_data_every_day()
    update_men_women()

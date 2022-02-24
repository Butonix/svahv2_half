import vk_api
from vk_api import VkUpload
from vk_api.utils import get_random_id
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import config
import pymysql
import pymysql.cursors
import requests
import time


def get_connection():
    connection = pymysql.connect(host=config.mysqlhost,
                                 user=config.mysqlusername,
                                 password=config.mysqlpassword,
                                 db=config.mysqldbname,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def add_new_line(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO user (id_vk, menu_vk, likes, dislikes, reports, reports_admin, sum_reaction, is_don, " \
                  "likes_last, dislikes_last, reports_last) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (id_user, "1", "0", "0", "0", "0", "0", "0", "0", "0", "0"))
        connection.commit()
    finally:
        connection.close()
    return


def take_position(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT menu_vk FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is None:
                return_count = 0
            else:
                return_count = line["menu_vk"]
    finally:
        connection.close()
    return return_count


def check_donuts(donuts_list):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_vk FROM user WHERE is_don = %s AND blacklist != 2 AND blacklist != 3"
            cursor.execute(sql, ("1"))
            row = [item['id_vk'] for item in cursor.fetchall()]
            print(row)
            not_donuts = list(set(row)-set(donuts_list["response"]["items"]))
            for i in range(0, len(not_donuts)):
                sql = "UPDATE user SET is_don = %s WHERE id_vk = %s"
                cursor.execute(sql, ("0", not_donuts[i]))
        connection.commit()
    finally:
        connection.close()
    return


def update_donuts_status(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET is_don = %s WHERE id_vk = %s"
            cursor.execute(sql, ("1", id_user))
        connection.commit()
    finally:
        connection.close()
    return


def update_admin_status():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET is_don = %s WHERE blacklist = 2 OR blacklist = 3"
            cursor.execute(sql, ("1"))
        connection.commit()
    finally:
        connection.close()
    return


#if __name__ == '__main__':
def main_start():
    start_time = time.time()
    session = requests.Session()
    vk_session = vk_api.VkApi(token=config.tokenvk)
    vk = vk_session.get_api()
    upload = VkUpload(vk_session)
    longpoll = VkBotLongPoll(vk_session, config.id_vkgroup)
    try:
        r = requests.get('https://api.vk.com/method/groups.getMembers',
                          params={'access_token': config.token_standalone,
                                  'group_id': "163426674",
                                  'sort': "id_asc",
                                  'offset': "0",
                                  'count': "500",
                                  'filter': "donut",
                                  'v': "5.131"})
    except Exception as err:
        print(err)
    donuts_list = r.json()
    #print(type(donuts_list))
    check_donuts(donuts_list)
    for i in range(len(donuts_list['response']['items'])):
        #print(f'{donuts_list["response"]["items"][i]}')
        number_position = take_position(f'{donuts_list["response"]["items"][i]}')
        if number_position == 0:
            add_new_line(f'{donuts_list["response"]["items"][i]}')
        update_donuts_status(f'{donuts_list["response"]["items"][i]}')
    update_admin_status()
    print("--- %s seconds ---" % (time.time() - start_time))


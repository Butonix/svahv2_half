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


#Тут удалены 5 функций на 80 строк


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
                                  'group_id': "$",
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


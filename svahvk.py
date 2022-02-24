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


#Удалено ~2000 строк кода


def processing_message(id_user, message_text):
    number_position = take_position(id_user)
    if take_blacklist(id_user) == 1:
        send_message(id_user, keyboards.k_start,
                     "К сожалению, Вы заблокированы в системе и не можете пользоваться ботом")
        return

    if number_position == 0:
        send_message_with_photo(id_user, keyboards.k_start, "Тебя приветствует бот знакомств Svah!",
                                config.url_photo_hello)
        send_message(id_user, keyboards.k_start, "Не забудь заполнить анкету в Настройках перед стартом")
        add_new_line(id_user)

    elif number_position == 1:
        if message_text == "поиск собеседника":
            if check_before(id_user) == "0":
                update_position(id_user, "2")
                send_message(id_user, keyboards.k_search, "Приступаем к поиску собеседника")
                choice_man(id_user)
        elif message_text == "настройки":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Мы в настройках")
        else:
            send_message(id_user, keyboards.k_start, "Непонятная команда")

            
#Удалено ~500 строк кода


if __name__ == '__main__':
    while True:
        session = requests.Session()
        vk_session = vk_api.VkApi(token=config.tokenvk)
        vk = vk_session.get_api()
        upload = VkUpload(vk_session)
        longpoll = VkBotLongPoll(vk_session, config.id_vkgroup)
        try:
            for event in longpoll.listen():
                if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
                    threading.Thread(target=processing_message, args=(event.obj.from_id, event.obj.text)).start()
        except Exception:
            pass

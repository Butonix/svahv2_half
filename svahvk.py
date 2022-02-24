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


def add_new_line(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT FROM start_data WHERE idstart = 1"
            sql = "INSERT INTO user (id_vk, menu_vk, likes, dislikes, reports, reports_admin, sum_reaction, is_don, " \
                  "likes_last, dislikes_last, reports_last, want_to_see_again, need_age, min_age, max_age, null_age, " \
                  "shadowban) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,
                           (id_user, "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1", "0"))
        connection.commit()
    finally:
        connection.close()
    return


def number_is_num(num):
    if num.isdigit():
        if 16 <= int(num) <= 150 or int(num) == 0:
            return True
    return False


def check_number(num, variation, user_id):
    if variation == 0:
        if num.isdigit():
            if 16 <= int(num) <= 150:
                return True
        return False
    else:
        if num.isdigit():
            if 16 <= int(num) <= 150:
                connection = get_connection()
                try:
                    with connection.cursor() as cursor:
                        sql = "SELECT min_age FROM user_temp WHERE id_vk = %s"
                        cursor.execute(sql, (user_id))
                        row = cursor.fetchone()
                finally:
                    connection.close()
                if row is not None:
                    if int(row["min_age"]) <= int(num):
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False


def update_position(id_user, new_position):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET menu_vk = %s, menu_vk_temp = %s WHERE id_vk = %s"
            cursor.execute(sql, (new_position, new_position, id_user))
        connection.commit()
    finally:
        connection.close()
    return


def add_new_temp_line(user_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user_temp WHERE id_vk = %s"
            cursor.execute(sql, (user_id))
            row = cursor.fetchone()
            if row is None:
                sql = "INSERT INTO user_temp (id_vk) VALUES (%s)"
                cursor.execute(sql, (user_id))
        connection.commit()
    finally:
        connection.close()
    return


def add_to_temp_data(user_id, text_message, type_element):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            if type_element == 1:
                if len(text_message) > 20:
                    text_message = text_message[0:20]
                text_message = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+'
                                      r'|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)'
                                      r'|[^\s`!()\[\]{};:\'".,<>?«»""‘’]))', '', text_message)
                sql = "UPDATE user_temp SET name = %s WHERE id_vk = %s"
                cursor.execute(sql, (text_message, user_id))
                what_send = "Введи свой возраст (16+, если хочешь без возраста - пиши 0):"
            elif type_element == 2:
                sql = "UPDATE user_temp SET age = %s WHERE id_vk = %s"
                cursor.execute(sql, (text_message, user_id))
                what_send = "Выбери свой пол:\n[Потом ты его не сможешь изменить!]"
            elif type_element == 3:
                sql = "UPDATE user_temp SET gender = %s WHERE id_vk = %s"
                if text_message == 'м':
                    cursor.execute(sql, ('0', user_id))
                if text_message == 'ж':
                    cursor.execute(sql, ('1', user_id))
                what_send = "Выбери, кого ищешь для общения:"
            elif type_element == 4:
                if len(text_message) > 600:
                    text_message = text_message[0:600]
                text_message = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(('
                                      r'[^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()'
                                      r'\[\]{};:\'".,<>?«»""‘’]))', '', text_message)
                sql = "UPDATE user_temp SET description = %s WHERE id_vk = %s"
                cursor.execute(sql, (text_message, user_id))
                what_send = "Твоя анкета будет выглядеть так:"
            elif type_element == 5:
                sql = "UPDATE user_temp SET see_last_read = %s WHERE id_vk = %s"
                if text_message == 'нет':
                    cursor.execute(sql, ('0', user_id))
                if text_message == 'да':
                    cursor.execute(sql, ('1', user_id))
            elif type_element == 6:
                sql = "UPDATE user_temp SET who_search = %s WHERE id_vk = %s"
                if text_message == 'ищу м':
                    cursor.execute(sql, ('0', user_id))
                if text_message == 'ищу ж':
                    cursor.execute(sql, ('1', user_id))
                if text_message == 'без разницы':
                    cursor.execute(sql, ('2', user_id))
                what_send = "Расскажи о себе и/или кого ты ищешь (максимум 600 символов). Для пустой анкеты отправь 0:"
            connection.commit()
    finally:
        connection.close()
    return what_send


def add_from_temp_to_def(user_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user_temp WHERE id_vk = %s"
            cursor.execute(sql, (user_id))
            data = cursor.fetchone()
            sql = "UPDATE user SET name = %s, age = %s, description = %s, gender = %s, who_search = %s, " \
                  "ready_vk = %s, ready_tg = %s, notification_vk = %s, notification_tg = %s WHERE id_vk = %s"
            cursor.execute(sql, (str(data["name"]), str(data["age"]), str(data["description"]), str(data["gender"]),
                                 str(data["who_search"]), "0", "0", "0", "0", user_id))
        connection.commit()
    finally:
        connection.close()
    return


def send_message(id_user, id_keyboard, message_text):
    if id_keyboard == '':
        for i in range(0, 3):
            try:
                vk.messages.send(
                    user_id=id_user,
                    random_id=get_random_id(),
                    message=message_text)
            except Exception as e:
                print(e)
                print("Ошибка отправки сообщения у id" + str(id_user))
                time.sleep(0.5)
                continue
            break
    else:
        for i in range(0, 3):
            try:
                vk.messages.send(
                    user_id=id_user,
                    random_id=get_random_id(),
                    keyboard=open(id_keyboard, 'r', encoding='UTF-8').read(),
                    message=message_text)
            except Exception as e:
                print(e)
                print("Ошибка отправки сообщения у id" + str(id_user))
                time.sleep(0.5)
                continue
            break


def add_url_photo(id_user, url_photo):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET photo_url = %s WHERE id_vk = %s"
            cursor.execute(sql, (url_photo, id_user))
            connection.commit()
    finally:
        connection.close()
    return


def take_photo_url(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT photo_url FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            photo = cursor.fetchone()
            if photo is None or photo["photo_url"] is None or photo["photo_url"] == "0":
                url_photo = "0"
            else:
                url_photo = photo["photo_url"]
    finally:
        connection.close()
    return url_photo


def send_message_with_photo(id_user, id_keyboard, message_text, image_url):
    if image_url == "0":
        send_message(id_user, id_keyboard, message_text)
        send_message(id_user, id_keyboard, "[Тут должна быть фотография]")
        return
    attachments = []
    image = session.get(image_url, stream=True)
    photo = upload.photo_messages(photos=image.raw)[0]
    attachments.append(
        'photo{}_{}'.format(photo['owner_id'], photo['id'])
    )
    for i in range(0, 3):
        try:
            vk.messages.send(
                user_id=id_user,
                random_id=get_random_id(),
                attachment=','.join(attachments),
                keyboard=open(id_keyboard, 'r', encoding='UTF-8').read(),
                message=message_text)
        except:
            print("Ошибка отправки сообщения у id" + str(id_user))
            continue
        break
    return


def del_data(user_id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET name = %s, age = %s, description = %s, photo_url = %s, who_search = %s," \
                  "ready_vk = %s, ready_tg = %s, notification_vk = %s, notification_tg = %s WHERE id_vk = %s"
            cursor.execute(sql, (None, None, None, None, None, None, None, None, None, user_id))
        connection.commit()
    finally:
        connection.close()
    return


def take_data_from_temp(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name FROM user_temp WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            name = cursor.fetchone()
            if name["name"] is None:
                final_text = "[Ваша анкета пуста]"
            else:
                sql = "SELECT gender FROM user_temp WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                gender = cursor.fetchone()
                sql = "SELECT age FROM user_temp WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                age = cursor.fetchone()
                if str(gender["gender"]) == '0':
                    pol = 'М'
                else:
                    pol = 'Ж'
                sql = "SELECT description FROM user_temp WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                description = cursor.fetchone()
                if str(age["age"]) == '0' and str(description["description"]) == '0':
                    final_text = str(name["name"]) + ", " + pol
                elif str(age["age"]) == '0':
                    final_text = str(name["name"]) + ", " + pol + "\n\n" + str(description["description"])
                elif str(description["description"]) == '0':
                    final_text = str(name["name"]) + ", " + pol + ", " + str(age["age"])
                else:
                    final_text = str(name["name"]) + ", " + pol + ", " + str(age["age"]) + "\n\n" + str(
                        description["description"])
    finally:
        connection.close()
    return final_text


def check_before(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name, photo_url, ready_vk FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["name"] is None and line["photo_url"] is None:
                final_text = "У тебя нет фото и анкеты\n\nИзменить фото и анкету ты можешь в Настройках"
            elif line["name"] is None and line["photo_url"] is not None:
                final_text = "У тебя есть фото, но нет анкеты\n\nЗаполнить анкету ты можешь в Настройках"
            elif line["name"] is not None and line["photo_url"] is None:
                final_text = "У тебя есть анкета, но нет фото\n\nЗагрузить фото ты можешь в Настройках"
            elif line["name"] is not None and line["photo_url"] is not None:
                final_text = "0"
            if line["ready_vk"] == 0 and final_text == "0":
                final_text = "Ты не разрешил поиск в Настройках\n\nДля этого зайди в Настройки и выбери " \
                             "пункт «Включить/выключить поиск»"
            if final_text != "0":
                send_message(id_user, keyboards.k_start, final_text)
    finally:
        connection.close()
    return final_text


def take_data(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name, gender, age, description FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["name"] is None:
                final_text = "[Ваша анкета пуста]"
            else:
                if str(line["gender"]) == '0':
                    pol = 'М'
                else:
                    pol = 'Ж'
                if str(line["age"]) == '0' and str(line["description"]) == '0':
                    final_text = str(line["name"]) + ", " + pol
                elif str(line["age"]) == '0':
                    final_text = str(line["name"]) + ", " + pol + "\n\n" + str(line["description"])
                elif str(line["description"]) == '0':
                    final_text = str(line["name"]) + ", " + pol + ", " + str(line["age"])
                else:
                    final_text = str(line["name"]) + ", " + pol + ", " + str(line["age"]) + "\n\n" + str(
                        line["description"])
    finally:
        connection.close()
    return final_text


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


def take_blacklist(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT blacklist FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is None or line["blacklist"] is None or line["blacklist"] == 0:
                return_count = 0
            elif line["blacklist"] == 2:
                return_count = 2
            elif line["blacklist"] == 3:
                return_count = 3
            else:
                return_count = 1
    finally:
        connection.close()
    return return_count


def take_search(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ready_vk, photo_url FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["ready_vk"] is None or line["photo_url"] is None:
                return_count = "У Вас пока нет анкеты и/или фото, чтобы настраивать этот параметр"
            elif str(line["ready_vk"]) == "1":
                return_count = "В данный момент у тебя разрешен поиск"
            elif str(line["ready_vk"]) == "0":
                return_count = "В данный момент у тебя стоит запрет на поиск"
            else:
                return_count = "Произошла какая-то ошибка, обратитесь к админу"
    finally:
        connection.close()
    return return_count


def take_notification(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT notification_vk FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["notification_vk"] is None:
                return_count = "У Вас пока нет анкеты, чтобы настраивать этот параметр"
            elif str(line["notification_vk"]) == "1":
                return_count = "В данный момент у тебя разрешены уведомления"
            elif str(line["notification_vk"]) == "0":
                return_count = "В данный момент у тебя стоит запрет на уведомления"
            else:
                return_count = "Произошла какая-то ошибка, обратитесь к админу"
    finally:
        connection.close()
    return return_count


def take_notification_reports(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT notification_report FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["notification_report"] is None:
                return_count = "В данный момент у тебя стоит запрет на уведомления"
            elif str(line["notification_report"]) == "1":
                return_count = "В данный момент у тебя разрешены уведомления"
            elif str(line["notification_report"]) == "0":
                return_count = "В данный момент у тебя стоит запрет на уведомления"
            else:
                return_count = "Произошла какая-то ошибка, обратитесь к админу"
    finally:
        connection.close()
    return return_count


def update_search(id_user, new_search):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name, photo_url, ready_vk FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["ready_vk"] is None or line["photo_url"] is None or line["name"] is None:
                return_count = "У Вас пока нет анкеты или фото, чтобы настраивать этот параметр"
            else:
                sql = "UPDATE user SET ready_vk = %s WHERE id_vk = %s"
                cursor.execute(sql, (new_search, id_user))
                if new_search == "0":
                    return_count = "Запретили поиск"
                else:
                    return_count = "Разрешили поиск"
        connection.commit()
    finally:
        connection.close()
    return return_count


def update_notification(id_user, new_notification):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT notification_vk FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line["notification_vk"] is None:
                return_count = "У Вас пока нет анкеты, чтобы настраивать этот параметр"
            else:
                sql = "UPDATE user SET notification_vk  = %s WHERE id_vk = %s"
                cursor.execute(sql, (new_notification, id_user))
                if new_notification == "0":
                    return_count = "Запретили уведомления"
                else:
                    return_count = "Разрешили уведомления"
        connection.commit()
    finally:
        connection.close()
    return return_count


def update_dislikes(id_user, new_notification):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET want_to_see_again = %s WHERE id_vk = %s"
            cursor.execute(sql, (new_notification, id_user))
            if new_notification == "0":
                return_count = "Больше не показываемся тем, кто задизлайкал"
            elif new_notification == "1":
                return_count = "Теперь показываемся тем, кто задизлайкал"
            else:
                return_count = "Произошла какая-то ошибка"
        connection.commit()
    finally:
        connection.close()
    return return_count


def update_notification_report(id_user, new_notification):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET notification_report  = %s WHERE id_vk = %s"
            cursor.execute(sql, (new_notification, id_user))
            if new_notification == "0":
                return_count = "Запретили уведомления"
            else:
                return_count = "Разрешили уведомления"
        connection.commit()
    finally:
        connection.close()
    return return_count


def check_reaction(id_user, id_potential):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT reaction FROM relations WHERE id_one = %s AND id_two = %s"
            cursor.execute(sql, (id_potential, id_user))
            line = cursor.fetchone()
            if line is None:
                final_count = "0"
            elif line["reaction"] == 1:
                final_count = "1"
            else:
                final_count = "0"
        connection.commit()
    finally:
        connection.close()
    return final_count


def choice_man(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT who_search, gender, is_don, age, need_age, min_age, max_age, null_age, shadowban, number_see FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            sql = None
            if line["shadowban"] == 1:
                if line["gender"] == 0 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '1' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '0' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.shadowban = '1' " \
                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                if sql is not None:
                    cursor.execute(sql, (id_user, id_user))
                    row = cursor.fetchone()
                if row is not None:
                    sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                    cursor.execute(sql, (row["id_vk"], id_user))
                    send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                    send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                            take_photo_url(row["id_vk"]))
                else:
                    sql = "UPDATE user SET last_choice = NULL WHERE id_vk = %s"
                    cursor.execute(sql, (id_user))
                    send_message(id_user, keyboards.k_search,
                                 "Прости, но для тебя вариантов больше нет\nЗаходи позже")
            elif line["shadowban"] == 0 and line["is_don"] == 1 and line["need_age"] == 1:
                if line["gender"] == 0 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '1' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '0' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                if sql is not None:
                    cursor.execute(sql, (line["min_age"], line["max_age"], line["null_age"], id_user, id_user))
                    row = cursor.fetchone()
                if row is not None:
                    sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                    cursor.execute(sql, (row["id_vk"], id_user))
                    send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                    send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                            take_photo_url(row["id_vk"]))
                else:
                    if line["gender"] == 0 and line["who_search"] == 0:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                              "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 0 and line["who_search"] == 1:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '0') OR " \
                              "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 0 and line["who_search"] == 2:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                              "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                              "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                              "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                              "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 0:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '1') OR " \
                              "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 1:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                              "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 2:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                              "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                              "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                              "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                              "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                              "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    if sql is not None:
                        cursor.execute(sql, (line["min_age"], line["max_age"], line["null_age"], line["age"],
                                             line["age"], line["age"], id_user, id_user))
                        row = cursor.fetchone()
                    if row is not None:
                        sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                        cursor.execute(sql, (row["id_vk"], id_user))
                        send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                        send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                take_photo_url(row["id_vk"]))
                    else:
                        if line["gender"] == 0 and line["who_search"] == 0:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                  "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 0 and line["who_search"] == 1:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                  "AND ((user.gender = '1' AND user.who_search = '0') OR " \
                                  "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 0 and line["who_search"] == 2:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                  "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                  "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                  "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                  "user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 0:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                  "AND ((user.gender = '0' AND user.who_search = '1') OR " \
                                  "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 1:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                  "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                  "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 2:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                  "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                  "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                  "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                  "user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        if sql is not None:
                            cursor.execute(sql, (line["min_age"], line["max_age"], line["null_age"],
                                                 id_user, id_user, id_user))
                            row = cursor.fetchone()
                        if row is not None:
                            sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                            cursor.execute(sql, (row["id_vk"], id_user))
                            send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                            send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                    take_photo_url(row["id_vk"]))
                        else:
                            if line["gender"] == 0 and line["who_search"] == 0:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                      "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 0 and line["who_search"] == 1:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '0') OR " \
                                      "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 0 and line["who_search"] == 2:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                      "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                      "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                      "user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 0:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '1') OR " \
                                      "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 1:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                      "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 2:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) " \
                                      "AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                      "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                      "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                      "user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            if sql is not None:
                                cursor.execute(sql, (line["min_age"], line["max_age"], line["null_age"],
                                                     line["age"], line["age"], line["age"], id_user, id_user, id_user))
                                row = cursor.fetchone()
                            if row is not None:
                                sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                                cursor.execute(sql, (row["id_vk"], id_user))
                                send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                                send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                        take_photo_url(row["id_vk"]))
                            else:
                                if line["gender"] == 0 and line["who_search"] == 0:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '0' " \
                                          "AND user.who_search = '0') OR " \
                                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 0 and line["who_search"] == 1:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '1' " \
                                          "AND user.who_search = '0') OR " \
                                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 0 and line["who_search"] == 2:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '0' " \
                                          "AND user.who_search = '0') OR " \
                                          "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 0:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '0' " \
                                          "AND user.who_search = '1') OR " \
                                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 1:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '1' " \
                                          "AND user.who_search = '1') OR " \
                                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 2:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.age >= %s AND user.age <= %s) OR user.age = %s) AND ((user.gender = '1' " \
                                          "AND user.who_search = '1') OR " \
                                          "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                if sql is not None:
                                    cursor.execute(sql, (
                                    line["min_age"], line["max_age"], line["null_age"], id_user, id_user))
                                    row = cursor.fetchone()
                                if row is not None:
                                    sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                                    cursor.execute(sql, (row["id_vk"], id_user))
                                    send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                                    send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                            take_photo_url(row["id_vk"]))
                                else:
                                    sql = "UPDATE user SET last_choice = NULL WHERE id_vk = %s"
                                    cursor.execute(sql, (id_user))
                                    send_message(id_user, keyboards.k_search,
                                                 "Прости, но для тебя вариантов больше нет\nЗаходи позже")
            elif line["is_don"] == 1 or line["number_see"] > 0:
                if line["gender"] == 0 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 0 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '0') OR " \
                          "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 0:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 1:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                elif line["gender"] == 1 and line["who_search"] == 2:
                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                          "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '1') OR " \
                          "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                if sql is not None:
                    cursor.execute(sql, (id_user, id_user))
                    row = cursor.fetchone()
                if row is not None:
                    sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                    cursor.execute(sql, (row["id_vk"], id_user))
                    send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                    send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                            take_photo_url(row["id_vk"]))
                else:
                    if line["gender"] == 0 and line["who_search"] == 0:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                              "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 0 and line["who_search"] == 1:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '0') OR " \
                              "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 0 and line["who_search"] == 2:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                              "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                              "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                              "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                              "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 0:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '1') OR " \
                              "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 1:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                              "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                              "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                              "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    elif line["gender"] == 1 and line["who_search"] == 2:
                        sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                              "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                              "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                              "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                              "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                              "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                              "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                    if sql is not None:
                        cursor.execute(sql, (line["age"], line["age"], line["age"], id_user, id_user))
                        row = cursor.fetchone()
                    if row is not None:
                        sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                        cursor.execute(sql, (row["id_vk"], id_user))
                        send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                        send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                take_photo_url(row["id_vk"]))
                    else:
                        if line["gender"] == 0 and line["who_search"] == 0:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                  "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 0 and line["who_search"] == 1:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '0') OR " \
                                  "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 0 and line["who_search"] == 2:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                  "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                  "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                  "user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 0:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '0' AND user.who_search = '1') OR " \
                                  "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 1:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                  "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        elif line["gender"] == 1 and line["who_search"] == 2:
                            sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                  "AND user.need_age = '0' AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                  "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                  "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                  "user.id_vk != %s AND EXISTS " \
                                  "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                  "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                  "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                  "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                  "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                  "ORDER  BY RAND() LIMIT  1"
                        if sql is not None:
                            cursor.execute(sql, (id_user, id_user, id_user))
                            row = cursor.fetchone()
                        if row is not None:
                            sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                            cursor.execute(sql, (row["id_vk"], id_user))
                            send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                            send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                    take_photo_url(row["id_vk"]))
                        else:
                            if line["gender"] == 0 and line["who_search"] == 0:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age == %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                      "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 0 and line["who_search"] == 1:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '0') OR " \
                                      "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 0 and line["who_search"] == 2:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                      "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                      "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                      "user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 0:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '0' AND user.who_search = '1') OR " \
                                      "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 1:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                      "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            elif line["gender"] == 1 and line["who_search"] == 2:
                                sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '1' " \
                                      "AND user.need_age = '1' AND ((user.min_age <= %s AND user.max_age >= %s) OR " \
                                      "(user.null_age = %s)) AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                      "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                      "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                      "user.id_vk != %s AND EXISTS " \
                                      "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                      "user.id_vk AND (relations.reaction = '0' OR relations.reaction = '2') AND " \
                                      "timestampdiff(HOUR, relations.date, now()) > 168 AND user.want_to_see_again = '1')) " \
                                      "AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = user.id_vk AND " \
                                      "relations.id_two = %s AND (relations.reaction = '2' OR relations.reaction = '0'))) " \
                                      "ORDER  BY RAND() LIMIT  1"
                            if sql is not None:
                                cursor.execute(sql, (line["age"], line["age"], line["age"], id_user, id_user, id_user))
                                row = cursor.fetchone()
                            if row is not None:
                                sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                                cursor.execute(sql, (row["id_vk"], id_user))
                                send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                                send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                        take_photo_url(row["id_vk"]))
                            else:
                                if line["gender"] == 0 and line["who_search"] == 0:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 0 and line["who_search"] == 1:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '1' AND user.who_search = '0') OR " \
                                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 0 and line["who_search"] == 2:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '0' AND user.who_search = '0') OR " \
                                          "(user.gender = '1' AND user.who_search = '0') OR (user.gender = '0' AND " \
                                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 0:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '0' AND user.who_search = '1') OR " \
                                          "(user.gender = '0' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 1:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                          "(user.gender = '1' AND user.who_search = '2')) AND user.id_vk != %s AND NOT EXISTS " \
                                          "(SELECT * FROM relations WHERE (relations.id_one = %s AND relations.id_two = " \
                                          "user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                elif line["gender"] == 1 and line["who_search"] == 2:
                                    sql = "SELECT DISTINCT user.id_vk FROM user WHERE user.ready_vk = '1' AND user.is_don = '0' " \
                                          "AND ((user.gender = '1' AND user.who_search = '1') OR " \
                                          "(user.gender = '0' AND user.who_search = '1') OR (user.gender = '0' AND " \
                                          "user.who_search = '2') OR (user.gender = '1' AND user.who_search = '2')) AND " \
                                          "user.id_vk != %s AND NOT EXISTS (SELECT * FROM relations WHERE (relations.id_one = %s " \
                                          "AND relations.id_two = user.id_vk)) ORDER  BY RAND() LIMIT  1"
                                if sql is not None:
                                    cursor.execute(sql, (id_user, id_user))
                                    row = cursor.fetchone()
                                if row is not None:
                                    sql = "UPDATE user SET last_choice = %s, number_see = number_see - 1 WHERE id_vk = %s"
                                    cursor.execute(sql, (row["id_vk"], id_user))
                                    send_message(id_user, keyboards.k_search, "Как тебе такой вариант:")
                                    send_message_with_photo(id_user, keyboards.k_search, take_data(row["id_vk"]),
                                                            take_photo_url(row["id_vk"]))
                                else:
                                    sql = "UPDATE user SET last_choice = NULL WHERE id_vk = %s"
                                    cursor.execute(sql, (id_user))
                                    send_message(id_user, keyboards.k_search,
                                                 "Прости, но для тебя вариантов больше нет\nЗаходи позже")
            else:
                sql = "UPDATE user SET last_choice = NULL WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                send_message(id_user, keyboards.k_search, "Прости, но для тебя вариантов больше нет\nЗаходи позже")
        connection.commit()
    finally:
        connection.close()
    return


def check_age(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT need_age, min_age, max_age, null_age FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is not None:
                if line["need_age"] == 0:
                    return_count = "У Вас не стоит ограничения на возраст"
                elif line["need_age"] == 1:
                    return_count = "Ваш возрастной диапазон анкет: [" + str(line["min_age"]) + "-" + str(
                        line["max_age"]) \
                                   + "]\n Включены в диапазон те, кто не указал возраст: "
                    if line["null_age"] == 0:
                        return_count += "Да"
                    else:
                        return_count += "Нет"
                elif line["need_age"] is None:
                    return_count = "У вас не стоит ограничения по возрасту"
                else:
                    return_count = "Какая-то ошибка, обратитесь к администратору"
            else:
                return_count = "У Вас не стоит ограничения на возраст"
    finally:
        connection.close()
    return return_count


def send_admins(id_potential):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_vk FROM user WHERE notification_report = 1 AND blacklist = 2"
            cursor.execute(sql)
            line = cursor.fetchall()
            if line is not None:
                for row in line:
                    send_message(row["id_vk"], "", "Новый репорт на анкету пользователя: vk.com/id"
                                 + str(id_potential) + "\nЗагляни в админку")
    finally:
        connection.close()
    return


def place_reaction(id_user, id_potential, reaction):
    if id_potential is None:
        return
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM relations WHERE id_one = %s AND id_two = %s"
            cursor.execute(sql, (id_user, id_potential))
            rc = cursor.rowcount
            if str(rc) == '0':
                sql = "INSERT INTO relations (id_one, id_two, reaction, date) VALUES (%s, %s, %s, now())"
                cursor.execute(sql, (id_user, id_potential, reaction))
                if id_user != id_potential:
                    if reaction == '0':
                        sql = "UPDATE user SET dislikes = dislikes + 1, sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    elif reaction == '1':
                        sql = "UPDATE user SET likes = likes + 1, sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    elif reaction == '2':
                        sql = "UPDATE user SET reports = reports + 1, reports_admin = reports_admin + 1, " \
                              "sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    cursor.execute(sql, (id_potential))
            else:
                sql = "UPDATE relations SET reaction = %s, date = now() WHERE id_one = %s AND id_two = %s"
                cursor.execute(sql, (reaction, id_user, id_potential))
                if id_user != id_potential:
                    if reaction == '0':
                        sql = "UPDATE user SET dislikes = dislikes + 1, sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    elif reaction == '1':
                        sql = "UPDATE user SET likes = likes + 1, sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    elif reaction == '2':
                        sql = "UPDATE user SET reports = reports + 1, reports_admin = reports_admin + 1, " \
                              "sum_reaction = sum_reaction + 1 WHERE id_vk = %s"
                    cursor.execute(sql, (id_potential))
        connection.commit()
    finally:
        connection.close()
        if reaction == '2':
            send_admins(id_potential)
    return


def update_last_choice(id_user, id_potential):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET last_choice = %s WHERE id_vk = %s"
            cursor.execute(sql, (id_potential, id_user))
        connection.commit()
    finally:
        connection.close()
    return


def take_reciprocity(id_user, id_potential):
    if id_potential is None:
        return True
    return_count = True
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT reaction FROM relations WHERE id_one = %s AND id_two = %s"
            cursor.execute(sql, (id_potential, id_user))
            line = cursor.fetchone()
            if line is None:
                sql = "SELECT notification_vk FROM user WHERE id_vk = %s"
                cursor.execute(sql, (id_potential))
                line = cursor.fetchone()
                if line["notification_vk"] == 1:
                    update_position(id_potential, "14")
                    send_message(id_potential, keyboards.k_sees,
                                 "Твоя анкета понравилась кое-кому! Показать этого человека?")
                    update_last_choice(id_potential, id_user)
            elif line is not None and line["reaction"] == 1:
                return_count = False
                print("Мы тут")
                update_position(id_potential, "15")
                update_position(id_user, "15")
                print("Мы тут 2")
                print(str(id_user))
                print(str(id_potential))
                send_message(id_user, keyboards.k_thanks, "У тебя взаимная симпатия с этой анкетой!\nСкорее пиши по "
                                                          "ссылке: https://vk.com/id" + str(id_potential))
                send_message(id_potential, keyboards.k_thanks, "Тебе ответил взаимностью человек с этой анкетой!")
                send_message_with_photo(id_potential, keyboards.k_thanks, take_data(id_user), take_photo_url(id_user))
                send_message(id_potential, keyboards.k_thanks,
                             "Скорее пиши по ссылке: https://vk.com/id" + str(id_user))
        connection.commit()
    finally:
        connection.close()
    return return_count


def check_don(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT is_don FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is None or line["is_don"] == 0:
                return_count = False
            elif line["is_don"] == 1:
                return_count = True
            else:
                return_count = False
    finally:
        connection.close()
    return return_count


def check_dislike(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT want_to_see_again FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is None:
                return_count = "У Вас выключен показ анкеты тем, кто дизлайкнул"
            elif line["want_to_see_again"] == 1:
                return_count = "У Вас включен показ анкеты тем, кто дизлайкнул"
            else:
                return_count = "У Вас выключен показ анкеты тем, кто дизлайкнул"
    finally:
        connection.close()
    return return_count


def check_pol(id_user, message_text):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT gender FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            row = cursor.fetchone()
            if row is not None and row["gender"] is not None:
                if message_text == 'ж':
                    if row['gender'] == 1:
                        return_count = True
                    else:
                        return_count = False
                else:
                    if row['gender'] == 0:
                        return_count = True
                    else:
                        return_count = False
            else:
                return_count = True
    finally:
        connection.close()
    return return_count


def take_statistics(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT likes, dislikes, likes_last, dislikes_last, last_update FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            row = cursor.fetchone()
            if row is not None:
                row_str = "Ваша статистика.\nЛайки последней анкеты: " + str(
                    row["likes_last"]) + "\nДизлайки последней анкеты: " + \
                          str(row["dislikes_last"]) + "\nДата изменения анкеты: " + str(
                    row["last_update"]) + "\n\nСтатистика за всё время на вашем аккаунте:\nЛайки общие: " + \
                          str(row["likes"]) + "\nДизлайки общие: " + str(row["dislikes"])
            else:
                row_str = "Статистика пока пуста"
    finally:
        connection.close()
    return row_str


def update_last_time(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET last_update = now() WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
        connection.commit()
    finally:
        connection.close()
    return


def shadowban_reports(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT report_ban_last FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is not None:
                if line["report_ban_last"] is not None:
                    sql = "UPDATE user SET shadowban = 1, reports_admin = 0 WHERE id_vk = %s"
                    cursor.execute(sql, (line["report_ban_last"]))
        connection.commit()
    finally:
        connection.close()
    return


def ban_reports(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT report_ban_last FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is not None:
                if line["report_ban_last"] is not None:
                    sql = "UPDATE user SET blacklist = 1, reports_admin = 0 WHERE id_vk = %s"
                    cursor.execute(sql, (line["report_ban_last"]))
        connection.commit()
    finally:
        connection.close()
    return


def null_reports(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT report_ban_last FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is not None:
                if line["report_ban_last"] is not None:
                    sql = "UPDATE user SET reports_admin = 0 WHERE id_vk = %s"
                    cursor.execute(sql, (line["report_ban_last"]))
        connection.commit()
    finally:
        connection.close()
    return


def take_list_ban():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_vk FROM user WHERE blacklist = 1"
            cursor.execute(sql)
            line = cursor.fetchall()
            if line is None:
                str_result = "Забаненных нет"
            else:
                str_result = "Список забаненных:\n"
                for row in line:
                    user_get = vk.users.get(user_ids=(str(row["id_vk"])))
                    user_get = user_get[0]
                    str_result += "vk.com/id" + str((row["id_vk"])) + " - " + user_get['first_name'] + " " \
                                  + user_get['last_name'] + "\n"
    finally:
        connection.close()
    return str_result


def take_reports(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_vk, reports_admin FROM user WHERE reports_admin >= 1"
            cursor.execute(sql)
            line = cursor.fetchone()
            if line is None:
                send_message(id_user, keyboards.k_ban_report, "Зарепорченных на данный момент нет")
                sql = "UPDATE user SET report_ban_last = NULL WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
            else:
                send_message(id_user, keyboards.k_ban_report, "Количество репортов у пользователя: " +
                             str(line["reports_admin"]) + "\nСсылка на аккаунт: https://vk.com/id"
                             + str(line["id_vk"]) + "\nЕго анкета:")
                send_message_with_photo(id_user, keyboards.k_ban_report, take_data(line["id_vk"]),
                                        take_photo_url(line["id_vk"]))
                sql = "UPDATE user SET report_ban_last = %s WHERE id_vk = %s"
                cursor.execute(sql, (line["id_vk"], id_user))
            connection.commit()
    finally:
        connection.close()
    return


def add_to_ban(message_text):
    numbers = re.search(r'(?<=vk\.com/).*', str(message_text))
    if numbers is None:
        return "Неверно введена ссылка на пользователя, повторите снова"
    elif numbers.group(0)[0:2] == "id" and numbers.group(0)[3].isdigit():
        numbers = re.search(r'[0-9]+', str(numbers.group(0)))
        numbers = numbers.group(0)
        user_get = vk.users.get(user_ids=numbers,
                                name_case="acc")
        user_get = user_get[0]
    else:
        user_get = vk.users.get(user_ids=str(numbers.group(0)),
                                name_case="acc")
        user_get = user_get[0]
        numbers = user_get['id']
    if take_position(numbers) == 0:
        add_new_line(numbers)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET blacklist = 1 WHERE id_vk = %s"
            cursor.execute(sql, (numbers))
            return_count = "Успешно добавили в блеклист " + str(user_get['first_name']) + " " + \
                           str(user_get['last_name'])
        connection.commit()
    finally:
        connection.close()
    return return_count


def add_to_shadowban(message_text):
    numbers = re.search(r'(?<=vk\.com/).*', str(message_text))
    if numbers is None:
        return "Неверно введена ссылка на пользователя, повторите снова"
    elif numbers.group(0)[0:2] == "id" and numbers.group(0)[3].isdigit():
        numbers = re.search(r'[0-9]+', str(numbers.group(0)))
        numbers = numbers.group(0)
        user_get = vk.users.get(user_ids=numbers,
                                name_case="acc")
        user_get = user_get[0]
    else:
        user_get = vk.users.get(user_ids=str(numbers.group(0)),
                                name_case="acc")
        user_get = user_get[0]
        numbers = user_get['id']
    if take_position(numbers) == 0:
        add_new_line(numbers)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET shadowban = 1 WHERE id_vk = %s"
            cursor.execute(sql, (numbers))
            return_count = "Успешно добавили в шадоубан-лист " + str(user_get['first_name']) + " " + \
                           str(user_get['last_name'])
        connection.commit()
    finally:
        connection.close()
    return return_count


def del_from_ban(message_text):
    numbers = re.search(r'(?<=vk\.com/).*', str(message_text))
    if numbers is None:
        return "Неверно введена ссылка на пользователя, повторите снова"
    elif numbers.group(0)[0:2] == "id" and numbers.group(0)[3].isdigit():
        numbers = re.search(r'[0-9]+', str(numbers.group(0)))
        numbers = numbers.group(0)
        user_get = vk.users.get(user_ids=numbers,
                                name_case="acc")
        user_get = user_get[0]
    else:
        user_get = vk.users.get(user_ids=str(numbers.group(0)),
                                name_case="acc")
        user_get = user_get[0]
        numbers = user_get['id']
    if take_position(numbers) == 0:
        add_new_line(numbers)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET blacklist = 0 WHERE id_vk = %s"
            cursor.execute(sql, (numbers))
            return_count = "Успешно удалили из блеклиста " + str(user_get['first_name']) + " " + \
                           str(user_get['last_name'])
        connection.commit()
    finally:
        connection.close()
    return return_count


def del_from_shadowban(message_text):
    numbers = re.search(r'(?<=vk\.com/).*', str(message_text))
    if numbers is None:
        return "Неверно введена ссылка на пользователя, повторите снова"
    elif numbers.group(0)[0:2] == "id" and numbers.group(0)[3].isdigit():
        numbers = re.search(r'[0-9]+', str(numbers.group(0)))
        numbers = numbers.group(0)
        user_get = vk.users.get(user_ids=numbers,
                                name_case="acc")
        user_get = user_get[0]
    else:
        user_get = vk.users.get(user_ids=str(numbers.group(0)),
                                name_case="acc")
        user_get = user_get[0]
        numbers = user_get['id']
    if take_position(numbers) == 0:
        add_new_line(numbers)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET shadowban = 0 WHERE id_vk = %s"
            cursor.execute(sql, (numbers))
            return_count = "Успешно удалили из шадоубан-листа " + str(user_get['first_name']) + " " + \
                           str(user_get['last_name'])
        connection.commit()
    finally:
        connection.close()
    return return_count


def update_age(id_user, position, number):
    add_new_temp_line(id_user)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            if position == 1 and number == "0":
                sql = "UPDATE user SET need_age = 0 WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                send_message(id_user, keyboards.k_age, "Выключили ограничение по возрасту")
            elif position == 1 and number == "1":
                sql = "UPDATE user_temp SET need_age = 1 WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                send_message(id_user, keyboards.k_stop, "Введите минимальный возраст анкеты "
                                                        "(граничное значение включается в диапазон)")
                update_position(id_user, "29")
            elif position == 2 and check_number(number, 0, id_user):
                sql = "UPDATE user_temp SET min_age = %s WHERE id_vk = %s"
                cursor.execute(sql, (number, id_user))
                send_message(id_user, keyboards.k_stop, "Введите максимальный возраст анкеты "
                                                        "(граничное значение включается в диапазон)")
                update_position(id_user, "30")
            elif position == 2:
                send_message(id_user, keyboards.k_stop, "Введите адекватный минимальный возраст анкеты:")
            elif position == 3 and check_number(number, 1, id_user):
                sql = "UPDATE user_temp SET max_age = %s WHERE id_vk = %s"
                cursor.execute(sql, (number, id_user))
                send_message(id_user, keyboards.k_yes_no, "Включать в поиск людей без обозначения возраста?")
                update_position(id_user, "31")
            elif position == 3:
                send_message(id_user, keyboards.k_stop, "Введите адекватный максимальный возраст анкеты "
                                                        "(он также должен быть больше, либо равен минимальному):")
            elif position == 4:
                sql = "UPDATE user_temp SET null_age = %s WHERE id_vk = %s"
                cursor.execute(sql, (number, id_user))
                update_position(id_user, "32")
                sql = "SELECT min_age, max_age, null_age FROM user_temp WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                line = cursor.fetchone()
                return_count = "Ваш возрастной диапазон анкет: [" + str(line["min_age"]) + "-" + str(line["max_age"]) + \
                               "]\nВключать анкеты без обозначения возраста: "
                if line["null_age"] == 0:
                    return_count += "Да"
                else:
                    return_count += "Нет"
                send_message(id_user, keyboards.k_save, return_count)
            elif position == 5:
                sql = "SELECT min_age, max_age, null_age FROM user_temp WHERE id_vk = %s"
                cursor.execute(sql, (id_user))
                line = cursor.fetchone()
                if line is not None:
                    sql = "UPDATE user SET null_age = %s, min_age = %s, max_age = %s, need_age = 1 WHERE id_vk = %s"
                    cursor.execute(sql, (str(line["null_age"]), str(line["min_age"]), str(line["max_age"]), id_user))
                    send_message(id_user, keyboards.k_age, "Сохранили")
                else:
                    update_position(id_user, "27")
                    send_message(id_user, keyboards.k_age, "Произошла какая-то ошибка, обратитесь к администратору")
            else:
                update_position(id_user, "27")
                send_message(id_user, keyboards.k_age, "Произошла какая-то ошибка, обратитесь к администратору")
            connection.commit()
    finally:
        connection.close()
    return


def take_list_shadowban():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id_vk FROM user WHERE shadowban = 1"
            cursor.execute(sql)
            line = cursor.fetchall()
            if line is None:
                str_result = "Зашадоубаненных нет"
            else:
                str_result = "Список зашадоубанных:\n"
                for row in line:
                    user_get = vk.users.get(user_ids=(str(row["id_vk"])))
                    user_get = user_get[0]
                    str_result += "vk.com/id" + str(row["id_vk"]) + " - " + user_get['first_name'] + " " \
                                  + user_get['last_name'] + "\n"
    finally:
        connection.close()
    return str_result


def take_last_choice(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT last_choice FROM user WHERE id_vk = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
    finally:
        connection.close()
    return line["last_choice"]


def processing_message(id_user, message_text):
    number_position = take_position(id_user)
    print("number_pos " + str(number_position))
    if number_position != 6 and number_position != 12:
        message_text = message_text.lower()
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
        elif message_text == "админка":
            if take_blacklist(id_user) == 2:
                update_position(id_user, "18")
                send_message(id_user, keyboards.k_admin, "Здравия желаю, товарищ адмем")
        else:
            send_message(id_user, keyboards.k_start, "Непонятная команда")

    elif number_position == 2:
        if message_text == "остановить поиск":
            update_position(id_user, "1")
            send_message(id_user, keyboards.k_start, "Выходим в главное меню")
        elif message_text == "❤":
            place_reaction(id_user, take_last_choice(id_user), '1')
            if take_reciprocity(id_user, take_last_choice(id_user)):
                choice_man(id_user)
        elif message_text == "жалоба":
            place_reaction(id_user, take_last_choice(id_user), '2')
            choice_man(id_user)
        elif message_text == "👎":
            place_reaction(id_user, take_last_choice(id_user), '0')
            choice_man(id_user)
        else:
            send_message(id_user, keyboards.k_search, "Непонятная команда")

    elif number_position == 3:
        if message_text == "вернуться в главное меню":
            update_position(id_user, "1")
            send_message(id_user, keyboards.k_start, "Выходим в главное меню")
        elif message_text == "вкл/выкл поиск":
            update_position(id_user, "4")
            send_message(id_user, keyboards.k_open, "Включение поиска активирует поиск для тебя, а также поиск "
                                                    "твоей анкеты для остальных.")
            send_message(id_user, keyboards.k_open, take_search(id_user))
        elif message_text == "вкл/выкл уведомления":
            update_position(id_user, "5")
            send_message(id_user, keyboards.k_alarm, "Включение уведомлений будет информировать тебя о новых лайках "
                                                     "твоей анкеты, когда ты оффлайн")
            send_message(id_user, keyboards.k_alarm, take_notification(id_user))
        elif message_text == "изменить анкету":
            update_position(id_user, "6")
            send_message(id_user, keyboards.k_stop, "Как мне тебя называть?\n[Максимум 20 символов]")
        elif message_text == "изменить фото":
            update_position(id_user, "7")
            send_message_with_photo(id_user, keyboards.k_stop, "Твоё текущее фото:", take_photo_url(id_user))
            send_message(id_user, keyboards.k_stop, "Загрузи фотографию далее, если хочешь её изменить:\n"
                                                    "(За неподобающие фотографии - перманентный бан)")
        elif message_text == "посмотреть/удалить анкету и фото":
            update_position(id_user, "8")
            send_message(id_user, keyboards.k_delete, "Твоя анкета выглядит так:")
            send_message_with_photo(id_user, keyboards.k_delete, take_data(id_user), take_photo_url(id_user))
        elif message_text == "настройки для донов":
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, "Заходим в настройки для донов")
        else:
            send_message(id_user, keyboards.k_settings, "Непонятная команда")

    elif number_position == 4:
        if message_text == "включить поиск":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, update_search(id_user, "1"))
        elif message_text == "выключить поиск":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, update_search(id_user, "0"))
        elif message_text == "выйти в настройки":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Мы в настройках")
        else:
            send_message(id_user, keyboards.k_open, "Непонятная команда")

    elif number_position == 5:
        if message_text == "включить уведомления":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, update_notification(id_user, "1"))
        elif message_text == "выключить уведомления":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, update_notification(id_user, "0"))
        elif message_text == "выйти в настройки":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Мы в настройках")
        else:
            send_message(id_user, keyboards.k_open, "Непонятная команда")

    elif number_position == 6:
        if message_text == 'СТОП':
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Останавливаем заполнение анкеты")
        elif message_text == '':
            send_message(id_user, keyboards.k_stop, "Пустое имя не принимается, попробуй ещё раз")
        else:
            update_position(id_user, "9")
            add_new_temp_line(id_user)
            send_message(id_user, keyboards.k_stop, add_to_temp_data(id_user, message_text, 1))

    elif number_position == 7:
        atchs = event.object['attachments']
        if atchs:
            for atch in atchs:
                if atch['type'] == 'photo':
                    photo = atch['photo']
                    url = photo['sizes'][-1]['url']
                    add_url_photo(id_user, url)
                    update_position(id_user, "3")
                    send_message(id_user, keyboards.k_settings, "Фотография изменена")
                    update_last_time(id_user)
        elif message_text == "стоп":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Выходим в настройки")
        else:
            send_message(id_user, keyboards.k_stop, "Непонятная команда")

    elif number_position == 8:
        if message_text == "удалить анкету и фото":
            update_position(id_user, "3")
            del_data(id_user)
            send_message(id_user, keyboards.k_settings, "Удалили анкету и фото")
        elif message_text == "оставить анкету и фото":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Оставили текущую анкету и фото")
        else:
            send_message(id_user, keyboards.k_delete, "Непонятная команда")

    elif number_position == 9:
        if message_text == "стоп":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Останавливаем заполнение анкеты")
        elif message_text == '':
            send_message(id_user, keyboards.k_stop, "Пустое поле возраста не принимается, если не хочется указывать -"
                                                    " введите 0")
        elif number_is_num(message_text):
            update_position(id_user, "10")
            send_message(id_user, keyboards.k_pol, add_to_temp_data(id_user, message_text, 2))
        else:
            send_message(id_user, keyboards.k_stop, "Введите число, без каких-либо символов и букв и в адекватном"
                                                    " диапазоне")

    elif number_position == 10:
        if message_text == "стоп":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Останавливаем заполнение анкеты")
        elif message_text == "м" or message_text == "ж":
            if check_pol(id_user, message_text):
                update_position(id_user, "11")
                send_message(id_user, keyboards.k_poisk, add_to_temp_data(id_user, message_text, 3))
            else:
                send_message(id_user, keyboards.k_pol, "Вы должны сделать тот же пол, что и на старте")
        else:
            send_message(id_user, keyboards.k_pol, "Непонятен Ваш пол, повторите")

    elif number_position == 11:
        if message_text == "стоп":
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Останавливаем заполнение анкеты")
        elif message_text == "ищу м" or message_text == "ищу ж" or message_text == "без разницы":
            update_position(id_user, "12")
            send_message(id_user, keyboards.k_stop, add_to_temp_data(id_user, message_text, 6))
        else:
            send_message(id_user, keyboards.k_poisk, "Непонятно кого Вы ищите, повторите")

    elif number_position == 12:
        if message_text == 'СТОП':
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Останавливаем заполнение анкеты")
        elif message_text == '':
            send_message(id_user, keyboards.k_stop, "Пустое поле описания не принимается, если не хочется указывать -"
                                                    " введите 0")
        else:
            update_position(id_user, "13")
            send_message(id_user, keyboards.k_save, add_to_temp_data(id_user, message_text, 4))
            send_message_with_photo(id_user, keyboards.k_save, take_data_from_temp(id_user), take_photo_url(id_user))

    elif number_position == 13:
        if message_text == 'сохранить':
            update_position(id_user, "3")
            add_from_temp_to_def(id_user)
            send_message(id_user, keyboards.k_settings, "Сохранили анкету")
            send_message(id_user, keyboards.k_settings, "После сохранения ваша анкета пропадает из поиска.\n"
                                                        "Не забудьте включить её в разделе «Вкл/выкл поиск»")
            update_last_time(id_user)
        elif message_text == 'выйти без сохранения':
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Вышли без сохранения")
        else:
            send_message(id_user, keyboards.k_save, "Непонятная команда")

    elif number_position == 14:
        if message_text == 'да':
            update_position(id_user, "2")
            send_message(id_user, keyboards.k_search, "Анкета, для которой ты краш:")
            send_message_with_photo(id_user, keyboards.k_search, take_data(take_last_choice(id_user)),
                                    take_photo_url(take_last_choice(id_user)))
        elif message_text == 'не хочу':
            update_position(id_user, "1")
            send_message(id_user, keyboards.k_start, "Ладно, выходим в главное меню")
        else:
            send_message(id_user, keyboards.k_sees, "Непонятная команда")

    elif number_position == 15:
        if message_text == 'спасибо, но верни главное меню':
            update_position(id_user, "1")
            send_message(id_user, keyboards.k_start, "Вернули в главное меню")
        else:
            send_message(id_user, keyboards.k_thanks, "Непонятная команда")

    elif number_position == 16:
        if message_text == 'статистика анкеты':
            if check_don(id_user):
                update_position(id_user, "17")
                send_message(id_user, keyboards.k_statistics, take_statistics(id_user))
            else:
                send_message(id_user, keyboards.k_donuts, "Вы не являетесь доном.\nЕсли это ошибка - обновите статус "
                                                          "командой 'Обновить статус' в этом меню и попробуйте снова.")
        elif message_text == 'определенный возраст':
            if check_don(id_user):
                update_position(id_user, "27")
                send_message(id_user, keyboards.k_age,
                             "В этих настройках Вы задаёте возрастной диапазон у анкет, которые"
                             " видите Вы и которым Вы показываетесь")
                send_message(id_user, keyboards.k_age, check_age(id_user))
            else:
                send_message(id_user, keyboards.k_donuts, "Вы не являетесь доном.\nЕсли это ошибка - обновите статус "
                                                          "командой 'Обновить статус' в этом меню и попробуйте снова.")
        elif message_text == 'показываться опять тем, кто дизлайкнул':
            if check_don(id_user):
                update_position(id_user, "28")
                send_message(id_user, keyboards.k_dislikes, "Этот параметр включает возможность показываться анкетам, "
                                                            "которые Вы лайкнули и которые задизлайкали Вас.\nУсловие - "
                                                            "прошла минимум неделя после их реакции на Вас.")
                send_message(id_user, keyboards.k_dislikes, check_dislike(id_user))
            else:
                send_message(id_user, keyboards.k_donuts, "Вы не являетесь доном.\nЕсли это ошибка - обновите статус "
                                                          "командой 'Обновить статус' в этом меню и попробуйте снова.")
        elif message_text == 'обновить статус дона':
            send_message(id_user, keyboards.k_donuts, "Обновляем статус...")
            scripts.main_start()
            if check_don(id_user):
                send_message(id_user, keyboards.k_donuts, "Обновили статус.\nВы являетесь доном.")
            else:
                send_message(id_user, keyboards.k_donuts, "Обновили статус.\nВы не являетесь доном.\nОбратитесь "
                                                          "к администратору, если считаете что произошла ошибка.")
        elif message_text == 'вернуться в настройки':
            update_position(id_user, "3")
            send_message(id_user, keyboards.k_settings, "Возвращаемся в настройки")
        else:
            send_message(id_user, keyboards.k_donuts, "Непонятная команда")

    elif number_position == 17:
        if message_text == 'верни меня в настройки донов':
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, "Возвращаемся в настройки донов")
        else:
            send_message(id_user, keyboards.k_statistics, "Непонятная команда")

    elif number_position == 18:
        if message_text == 'посмотреть список бан':
            update_position(id_user, "19")
            send_message(id_user, keyboards.k_return_admin, take_list_ban())
        elif message_text == 'посмотреть список шадоубан':
            update_position(id_user, "20")
            send_message(id_user, keyboards.k_return_admin, take_list_shadowban())
        elif message_text == 'добавить в список бан':
            update_position(id_user, "21")
            send_message(id_user, keyboards.k_return_admin, "Введите ссылку того, кого хотите добавить в бан")
        elif message_text == 'добавить в список шадоубан':
            update_position(id_user, "22")
            send_message(id_user, keyboards.k_return_admin, "Введите ссылку того, кого хотите добавить в шадоубан")
        elif message_text == 'убрать из списка бан':
            update_position(id_user, "23")
            send_message(id_user, keyboards.k_return_admin, "Введите ссылку того, кого хотите убрать из бана")
        elif message_text == 'убрать из списка шадоубан':
            update_position(id_user, "24")
            send_message(id_user, keyboards.k_return_admin, "Введите ссылку того, кого хотите убрать из шадоубана")
        elif message_text == 'посмотреть зарепорченных':
            update_position(id_user, "25")
            send_message(id_user, keyboards.k_ban_report, "Ищем зарепорченных...")
            take_reports(id_user)
        elif message_text == 'уведомления о репортах':
            update_position(id_user, "26")
            send_message(id_user, keyboards.k_reports, "Включение уведомлений о репортах будет сразу тебя информировать"
                                                       " о новых репортах на пользователей")
            send_message(id_user, keyboards.k_reports, take_notification_reports(id_user))
        elif message_text == 'вернуться в главное меню':
            update_position(id_user, "1")
            send_message(id_user, keyboards.k_start, "Возвращаемся в главное меню")
        else:
            send_message(id_user, keyboards.k_admin, "Непонятная команда")

    elif number_position == 19:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, "Непонятная команда")

    elif number_position == 20:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, "Непонятная команда")

    elif number_position == 21:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, add_to_ban(message_text))

    elif number_position == 22:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, add_to_shadowban(message_text))

    elif number_position == 23:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, del_from_ban(message_text))

    elif number_position == 24:
        if message_text == 'вернуться обратно в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_return_admin, del_from_shadowban(message_text))

    elif number_position == 25:
        if message_text == 'вернуться в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        elif message_text == 'бан':
            ban_reports(id_user)
            take_reports(id_user)
        elif message_text == 'шадоубан':
            shadowban_reports(id_user)
            take_reports(id_user)
        elif message_text == 'обнулить репорты':
            null_reports(id_user)
            take_reports(id_user)
        else:
            send_message(id_user, keyboards.k_ban_report, "Непонятная команда")

    elif number_position == 26:
        if message_text == 'включить уведомления':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, update_notification_report(id_user, "1"))
        elif message_text == 'выключить уведомления':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, update_notification_report(id_user, "0"))
        elif message_text == 'выйти в админку':
            update_position(id_user, "18")
            send_message(id_user, keyboards.k_admin, "Возвращаемся в админку")
        else:
            send_message(id_user, keyboards.k_reports, "Непонятная команда")

    elif number_position == 27:
        if message_text == 'включить ограничение по возрасту':
            update_age(id_user, 1, "1")
        elif message_text == 'выключить ограничение по возрасту':
            update_age(id_user, 1, "0")
        elif message_text == 'выйти в настройки донов':
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, "Выходим в настройки донов")
        else:
            send_message(id_user, keyboards.k_age, "Непонятная команда")

    elif number_position == 28:
        if message_text == 'показываться тем, кто дизлайкнул':
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, update_dislikes(id_user, "1"))
        elif message_text == 'не показываться тем, кто дизлайкнул':
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, update_dislikes(id_user, "0"))
        elif message_text == 'выйти в настройки донов':
            update_position(id_user, "16")
            send_message(id_user, keyboards.k_donuts, "Выходим в настройки донов")
        else:
            send_message(id_user, keyboards.k_dislikes, "Непонятная команда")

    elif number_position == 29:
        if message_text == 'стоп':
            update_position(id_user, "27")
            send_message(id_user, keyboards.k_age, "Окей, возвращаемся в настройки возраста")
        else:
            update_age(id_user, 2, message_text)
    elif number_position == 30:
        if message_text == 'стоп':
            update_position(id_user, "27")
            send_message(id_user, keyboards.k_age, "Окей, возвращаемся в настройки возраста")
        else:
            update_age(id_user, 3, message_text)
    elif number_position == 31:
        if message_text == 'стоп':
            update_position(id_user, "27")
            send_message(id_user, keyboards.k_age, "Окей, возвращаемся в настройки возраста")
        elif message_text == 'нет':
            update_age(id_user, 4, "1")
        elif message_text == 'да':
            update_age(id_user, 4, "0")
        else:
            send_message(id_user, keyboards.k_yes_no, "Непонятная команда")
    elif number_position == 32:
        if message_text == 'сохранить':
            update_position(id_user, "27")
            update_age(id_user, 5, "0")
        elif message_text == 'выйти без сохранения':
            update_position(id_user, "27")
            send_message(id_user, keyboards.k_age, "Окей, возвращаемся в настройки возраста")
        else:
            send_message(id_user, keyboards.k_save, "Непонятная команда")
    else:
        send_message(id_user, keyboards.k_start, "Произошла какая-то ошибка, обратитесь к админу")
    return


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

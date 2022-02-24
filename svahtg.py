import telebot
import config
import strings
import buttons
import pymysql
import pymysql.cursors

bot = telebot.TeleBot(config.tokentg)


def get_connection():
    con = pymysql.connect(host=config.mysqlhost,
                          user=config.mysqlusername,
                          password=config.mysqlpassword,
                          db=config.mysqldbname,
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    return con


@bot.message_handler(commands=['start', 'help'])
def start_message(message):
    bot.send_message(message.chat.id, strings.start, reply_markup=telebot.types.ReplyKeyboardRemove())


@bot.message_handler(content_types=['text'])
def default_test(message):
    button_hi = telebot.types.KeyboardButton(buttons.hi)
    greet_kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    greet_kb.add(button_hi)
    bot.send_message(message.chat.id, str(message.chat.id), reply_markup=greet_kb)
    bot.send_photo(message.chat.id, caption='вау', photo='https://telegram.org/img/t_logo.png')


if __name__ == '__main__':
    bot.polling(none_stop=True)

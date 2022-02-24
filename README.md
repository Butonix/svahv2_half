# svahv2_half
# ЭТО ПРИМЕР БОТА, БОЛЬШАЯ ЧАСТЬ КОДА ВЫРЕЗАНА (оставлено примерно 400 строк из 3000)

СВАХ - бот знакомств, реализованный через Bots Long Poll API на сайте VK.com

Бот построен на Python3 + MySQL, многопоточность реализована через встроенную функцию threading

Реализованные функции в боте:
- Выбор пола в своей анкете
- Выбор пола человека, которого ты ищешь для общения, либо можно даже выбрать пункт "Без разницы"
- Можно устанавливать "запрет на поиск" твоей анкеты, чтобы не терять свою анкету, но временно выпасть из знакомств
- Возможность удаления анкеты
- После изменения анкеты и/или фото тебе нужно будет выключать функцию "запрет на поиск", которая будет каждый раз включаться автоматически (на всякий случай)
- Можно делать пустыми графы возраста и описания своей анкеты
- Анкеты будут выпадать полностью рандомно, а не по порядку (но приоритет пользователям-донам)
- Пользователи будут видеть 
- Функция уведомления о лайке пользователя, если пользователь в данный момент не онлайн в боте
- Возможность монетизации бота через VK Donut

Дополнительные возможности для донатеров:
- Возможность показываться людям, кто тебя дизлайкнул, но кого лайкнул донатер (спустя неделю после реакции дизлайка)
- Возможность показываться первым в списках
- Возможность показывать ограниченному списку людей и видеть ограниченный список людей (по возрасту)

Функции против троллей и спамеров:
- Нельзя изменить изначально выбранный пол
- Нельзя приступать к поиску, если не заполнена анкета с фото и не включено разрешение на поиск твоей анкеты
- Ссылки в имени и в описании будут удаляться
- Есть кнопка жалобы на пользователей (человек не сможет пользоваться ботом вообще, бан будет перманентным)
- Человека можно отправить в шадоубан к таким же пользователям

Функции для админов:
- По умолчанию те же самые возможности, что и для донатеров
- Возможность блокировать пользователей через админку
- Возможность отправлять пользователей в шадоубан
- Возможность получать уведомления в момент отправки репортов на анкеты
- Возможность просматривать список зарепорченных и выбирать - бан, шадоубан или пропуск наказания пользователя (в таком случае обнуляются репорты)

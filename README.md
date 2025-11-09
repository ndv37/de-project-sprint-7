# Проект 7-го спринта

файл geo.csv положил сюда /user/naumovdv/data/geo.csv


## задание 1 - Витрина в разрезе пользователей 
(пользовательские функции в файле src/user_function.py, код для расчета витрины src/dm_users.py)

dm_users
 - user_id - идентификатор пользователя
 - act_city - актуальный адрес
 - home_city - домашний адрес
 - travel_count - количество посещенных городов
 - travel_array - список городов в порядке посещения

пробую запустить через spark-submit



## задание 2 - Витрина в разрезе зон

dm_zones
 - month — месяц расчёта
 - week — неделя расчёта
 - zone_id — идентификатор зоны (города)
 - week_message — количество сообщений за неделю
 - week_reaction — количество реакций за неделю
 - week_subscription — количество подписок за неделю
 - week_user — количество регистраций за неделю
 - month_message — количество сообщений за месяц
 - month_reaction
 - month_subscription
 - month_user

 пробую запустить через spark-submit

 ## задание 3 - витрина для рекомендации друзей

dm_recs
 - user_left
 - user_right
 - processed_dttm
 - zone_id
 - local_time

 ## задание 4 - DAG

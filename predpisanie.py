#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Введите свой путь к папке откуда надо взять акты
folder_path = '2024 - Январь/'
#Введите свой путь к папке откуда надо взять excel файл
folder_path_excel = "Information/finalWithChapters20240116afternoon - pred.xlsx"
# Спросить enviroment_state
ENVIROMENT_STATE = 2865
# Спросить id проекта
project_id = 56


# In[ ]:


import requests
from collections import Counter
import re
from pandas import Timestamp,Timedelta
s = requests.Session()
r = s.request('POST','https://ma.gasoilcenter.ru/api/token/obtain/',json = {"username":"admin","password":"Yasin1367!"})
m = (eval(r.text))["access"]
response = s.get(f'https://ma.gasoilcenter.ru/api/master-of-acts/acts/?environment_state__project={project_id}', headers={'Authorization':f"JWT {m}"})
all_acts = (eval(response.text.replace('null','""').replace("true","True").replace("false","False")))
m = []
for i in all_acts:
    try:
        m.append(i["raw_content"]["dateAkt"])
    except:
        pass


# In[ ]:


true_data = sorted(m, reverse=True)[0]
true_data


# ### Основные функции

# In[2]:


import warnings
import numpy as np
warnings.filterwarnings('ignore')


# In[3]:


def convert_zaknamestep1(matched_value):
    if matched_value is None:
        return "ТПП «Повхнефтегаз»"

    zaknamestep = [
        {"id": "0", "value": "ТПП «Когалымнефтегаз»", "payload": {"city": "Когалым"}},
        {"id": "1", "value": "ТПП «Повхнефтегаз»", "payload": {"city": "Когалым"}}
    ]

    for d in zaknamestep:
        if matched_value in d["value"]:
            return d

    return "ТПП «Повхнефтегаз»"

def convert_zaknamestep1_content(matched_value):

    zaknamestep = [
        {"id": "0", "value": "ТПП «Когалымнефтегаз»"},
        {"id": "1", "value": "ТПП «Повхнефтегаз»"}
    ]

    for d in zaknamestep:
        if matched_value in d["value"]:
            return d["value"]

    return "ТПП «Повхнефтегаз»"


# ### Выгрузка данных

# In[4]:


import pandas as pd 
import re
import os
import json
from datetime import datetime
from babel.dates import format_date

df_start = pd.DataFrame()
data_frames = []
columns_data = []


for path, dirs, files in os.walk(folder_path):
    for file_name in files:
        if file_name.endswith(".xlsx") or file_name.endswith(".XLSX"):
            file_path = os.path.join(path, file_name)
            
            # Извлечение zaknamestep1
            df_row = pd.read_excel(file_path)
            zaknamestep1 = df_row.iloc[0][4]
            curr = ["Повхнефтегаз", "Когалымнефтегаз"]
            matched_value = "ТПП «Повхнефтегаз»"
            for i in curr:
                if i in zaknamestep1:
                    matched_value = i
                    break
            zaknamestep1 = convert_zaknamestep1(matched_value)
            zaknamestep1_content = convert_zaknamestep1_content(matched_value)

            # Обработка всего датафрейма
            df = pd.read_excel(file_path, skiprows=2)
            df = df.rename(columns = dict(zip(df.columns.tolist(),[" ".join(i.split()).replace("/ ", "/").replace("/ ", "\\") for i in df.columns.tolist()])))
            if 'Выявленные нарушения по ОТ и ТБ, ПБ, ООС' in df.columns or 'Выявленные нарушения по технологии работ.' in df.columns or 'Куст/скважина Месторождение Супервайзер' in df.columns:
                df = df.rename(columns={
                    'Выявленные нарушения по ОТ и ТБ, ПБ, ООС': 'Выявленные нарушения по ОТ,ПБ',
                    'Выявленные нарушения по технологии работ.': 'Выявленные нарушения по технологии работ',
                    'Куст/скважина Месторождение Супервайзер': 'Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер'
                })
            df = df.loc[:, ~df.columns.str.startswith('Unnamed:')]
            df = df.drop(labels = [0],axis = 0)
            df['zaknamestep1'] = df.shape[0]*[zaknamestep1]
            df['zaknamestep1_content'] = df.shape[0]*[zaknamestep1_content]
            if "№ п/п" not in str(df.columns[0]):
                df = pd.read_excel(file_path, skiprows=3)
                df = df.rename(columns = dict(zip(df.columns.tolist(),[" ".join(i.split()).replace("/ ", "/").replace("/ ", "\\") for i in df.columns.tolist()])))
                df = df.loc[:, ~df.columns.str.startswith('Unnamed:')]
                df = df.drop(labels = [0],axis = 0)
                if 'Выявленные нарушения по ОТ и ТБ, ПБ, ООС' in df.columns or 'Выявленные нарушения по технологии работ.' in df.columns or 'Куст/скважина Месторождение Супервайзер' in df.columns:
                    df = df.rename(columns={
                        'Выявленные нарушения по ОТ и ТБ, ПБ, ООС': 'Выявленные нарушения по ОТ,ПБ',
                        'Выявленные нарушения по технологии работ.': 'Выявленные нарушения по технологии работ',
                        'Куст/скважина Месторождение Супервайзер': 'Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер'
                    })
                df['zaknamestep1'] = df.shape[0]*[zaknamestep1]
                df['zaknamestep1_content'] = df.shape[0]*[zaknamestep1_content]

            data_frames.append(df)

df_start = pd.concat(data_frames, ignore_index=True)


# In[5]:


df_start.head()


# In[6]:


df_start.shape


# In[7]:


#Копируем датафрейм, чтобы не пришлось снова считывать файлы
df_test = df_start.copy()
df_test.head()


# In[8]:


df_test.shape


# ### Подготовка данных

# In[9]:


# Убираем пропуски
df_test = df_test.dropna(subset='№ п/п')


# In[10]:


# Преобразуем столбец в числовой формат
df_test['№ п/п'] = pd.to_numeric(df_test['№ п/п'], errors='coerce')

# Убираем все значения, кроме чисел
df_test['№ п/п'] = df_test['№ п/п'].apply(lambda x: x if pd.notnull(x) else None)


# In[11]:


# Опять убираем пропуски после удаления ненужных строк
df_test = df_test.dropna(subset='№ п/п')


# In[12]:


# Убираем лишний столбец
df_test = df_test.drop(columns='№ п/п')


# In[13]:


# Заменяем пропуски на нужные строки
df_test["Выявленные нарушения по ОТ,ПБ"] = df_test["Выявленные нарушения по ОТ,ПБ"].dropna()


# In[14]:


# Ищем нужные нам типы актов 
df_test["Принятые меры"] = df_test["Принятые меры"].fillna("Отсутствуют")
df_test = df_test[df_test['Принятые меры'].str.contains('предп', case=False)]


# In[15]:


# Преобразуем дату в строку
df_test = df_test.dropna(subset='Дата, время проверки')
df_test["Дата, время проверки"] = df_test["Дата, время проверки"].apply(lambda x: str(x) if x is not None else x)


# In[16]:


df_test.shape


# In[17]:


# Создадим словарь для добавления туда ненайденных данных
dict_not_find = {}


# In[18]:


# Работаем с новым датафреймом и преоборазуем дату
not_find = []
def convert_to_datetime(s):
    try:
        s = s.strip()
        full_date = s.split("г")
        if len(full_date) == 2:
            date_part, other = s.split("г")
            end_time = other.strip().split("-")[1].rstrip(".")
            if "." in end_time:
                end_time = end_time.replace('.', ':')
            if end_time == "24:00":
                end_time = "00:00"
        elif len(full_date) == 1:
            date_part, other = re.split(r"\s+", s)
            end_time = other.strip().split("-")[1].rstrip(".")
            if "." in end_time:
                end_time = end_time.replace('.', ':')
            if end_time == "24:00":
                end_time = "00:00"
        
        datetime_str = f"{date_part} {end_time}"

        return pd.to_datetime(datetime_str, format='%d.%m.%Y %H:%M')
    except Exception as e:
        not_find.append(s)
        return None
df_test["Дата"] = df_test["Дата, время проверки"].apply(convert_to_datetime)


# In[19]:


dict_not_find["Дата"] = not_find


# In[20]:


df_test["Дата"].isnull().sum()


# In[21]:


# Ищем ФИО супервайзера
not_find = []
def convert_to_fio(row):
    try:
        pattern = r"[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]"
        families = re.findall(pattern, row)
        # if not families:
        #     s_with_space = re.sub(r'([а-яА-Я]+)([А-Я])', r'\1 \2', row)
        #     return s_with_space
        return families[0]
        
    except:
        not_find.append(row)
        pass
df_test["ФИО супервайзера"] = df_test["Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер"].apply(convert_to_fio)


# In[22]:


dict_not_find["ФИО супервайзера"] = not_find


# In[23]:


df_test["ФИО супервайзера"].isnull().sum()


# In[24]:


# Ищем куст
not_find = []
def convert_to_kust(row):
    try:
        pattern = r"\b(\d+\w*)"
        kust = re.search(pattern, row).group(0)
        return kust
    except:
        not_find.append(row)
        pass

df_test["Куст"] = df_test["Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер"].apply(convert_to_kust)


# In[25]:


dict_not_find["Куст"] = not_find


# In[26]:


df_test["Куст"].isnull().sum()


# In[27]:


# Ищем скважину
not_find = []
def convert_to_well(row):
    try:
        pattern = r"/\b(\d+\w*)"
        well = re.search(pattern, row).group(1)
        return well
    except:
        not_find.append(row)
        return ""

df_test["Скважина"] = df_test["Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер"].apply(convert_to_well)


# In[28]:


dict_not_find["Скважина"] = not_find


# In[29]:


df_test["Скважина"].isnull().sum()


# In[30]:


# Ищем месторождение
not_find = []
def convert_to_fields(row):
    try:
        pattern = r"\b[\w-]*(ое|овх|ун|ого|ор)\b"
        field = re.search(pattern, row).group(0)
        return field    
    except:
        not_find.append(row)
        return ""
    
df_test["Месторождение"] = df_test["Куст/скважина Месторождение ГНО/Qж/Qн Супервайзер"].apply(convert_to_fields)


# In[31]:


dict_not_find["Месторождение"] = not_find


# In[32]:


df_test["Месторождение"].isnull().sum()


# In[33]:


# Сопоставляем месторождения с теми, что есть в базе данных
not_find = []
def convert_to_fields_names(row):
    choices_fields = [{"value": "Абино-Украинское"}, {"value": "Абрамовское"}, {"value": "Аганское"}, {"value": "Акташское"}, {"value": "Алексеевская"}, {"value": "Алисовское"}, {"value": "Амдермаельское"}, {"value": "Андреевское"}, {"value": "Антиповско-Балыклейская"}, {"value": "Аригольское"}, {"value": "Аспинское"}, {"value": "Атамановское"}, {"value": "Ачимовское"}, {"value": "Ашальчинское"}, {"value": "Баганское"}, {"value": "Баклановское"}, {"value": "Барсуковское"}, {"value": "Батырбайское"}, {"value": "Бахиловское"}, {"value": "Бахиловское"}, {"value": "Бахтияровское"}, {"value": "Бельское"}, {"value": "Береговое"}, {"value": "Бобровское"}, {"value": "Бобровское"}, {"value": "Бобровское"}, {"value": "Боголюбовское"}, {"value": "Больше-Каменское"}, {"value": "Бузулукское"}, {"value": "В-Волостновское"}, {"value": "В-боголюбовское"}, {"value": "В. Сарутаюсское"}, {"value": "В.Капитоновское"}, {"value": "В.Малаховское"}, {"value": "Ван еганское"}, {"value": "Ван-Еганское"}, {"value": "Ван-Еганское бур."}, {"value": "Ванкор"}, {"value": "Варьёганское"}, {"value": "Ватинское"}, {"value": "Ватинское"}, {"value": "Ватинское"}, {"value": "Вать-Ёганское"}, {"value": "Вать-Еганское"}, {"value": "Ватьеганское"}, {"value": "Вахитовское"}, {"value": "Верхнеколик-Еганское"}, {"value": "Верхнеколик-Еганское"}, {"value": "Видное"}, {"value": "Викторинское"}, {"value": "Винниковское"}, {"value": "Вишневское"}, {"value": "Возей"}, {"value": "Возейское"}, {"value": "Волостновское"}, {"value": "Воробьевское"}, {"value": "Восточно- Мастерьельское"}, {"value": "Восточно-Икилорское"}, {"value": "Восточно-Икилорское"}, {"value": "Восточно-Кустовое"}, {"value": "Восточно-Макаровское"}, {"value": "Восточно-Перевальное"}, {"value": "Восточно-Перевальное"}, {"value": "Восточно-Правдинское"}, {"value": "Восточно-Придорожное"}, {"value": "Восточно-Придорожное"}, {"value": "Восточно-Придорожное"}, {"value": "Восточно-Пякутинское"}, {"value": "Восточно-Сарутаюское"}, {"value": "Восточно-Сургутское"}, {"value": "Восточно-Токайское"}, {"value": "Восточно-Ягунское"}, {"value": "Восточнро-Придорожное"}, {"value": "Встречное"}, {"value": "Вынгаяхинское"}, {"value": "Гаршинское"}, {"value": "Герасимовское"}, {"value": "Геркулесовское"}, {"value": "Гондыревское"}, {"value": "Горное"}, {"value": "Графское"}, {"value": "Даниловское"}, {"value": "Демаельская"}, {"value": "Довыдовское"}, {"value": "Дозорцевское"}, {"value": "Долговское"}, {"value": "Долговское"}, {"value": "Долговское"}, {"value": "Дон-Сыртовское"}, {"value": "Донская"}, {"value": "Дороховское"}, {"value": "Дружное"}, {"value": "Дружное"}, {"value": "Е.Зыковское"}, {"value": "Енапаевское"}, {"value": "Енорусскинское"}, {"value": "Етыпуровское"}, {"value": "Жилинское"}, {"value": "Журавское"}, {"value": "Загорское"}, {"value": "Залесское"}, {"value": "Зап-Угутское"}, {"value": "Зап-Усть -Былыкское"}, {"value": "Западно-Асомкинское"}, {"value": "Западно-Бимское"}, {"value": "Западно-Варьёганское"}, {"value": "Западно-Икилорское"}, {"value": "Западно-Катыльгинское"}, {"value": "Западно-Кулагинское"}, {"value": "Западно-Малобалыкское"}, {"value": "Западно-Пурпейское"}, {"value": "Западно-Степановское"}, {"value": "Западно-Тугровское"}, {"value": "Западно-Эргинское"}, {"value": "Западное Сюрхаратинское"}, {"value": "Западный Могутлор"}, {"value": "Зимнее"}, {"value": "Ивановское"}, {"value": "Икилорское"}, {"value": "Икилорское"}, {"value": "Ильичевское"}, {"value": "Имилорское"}, {"value": "Инзырейское"}, {"value": "Ининское"}, {"value": "Ининское"}, {"value": "Ипатское"}, {"value": "Ишуевское"}, {"value": "Казыгашевское"}, {"value": "Калиннинковское"}, {"value": "Калмиярское"}, {"value": "Камеликское"}, {"value": "Каменское"}, {"value": "Каменское"}, {"value": "Кетовское"}, {"value": "Кечимовское"}, {"value": "Киндельское"}, {"value": "Кинзельское"}, {"value": "Киняминское"}, {"value": "Кичкасское"}, {"value": "Киязлинское"}, {"value": "Ключевое"}, {"value": "Ковыктинское ГКМ"}, {"value": "Кодяковское"}, {"value": "Колвинское"}, {"value": "Командишорское"}, {"value": "Комсомольское"}, {"value": "Кондинское"}, {"value": "Корниловское"}, {"value": "Кочевское"}, {"value": "Кочевское"}, {"value": "Кошильское"}, {"value": "Крайнее"}, {"value": "Крапивинское"}, {"value": "Красное"}, {"value": "Красноленинское"}, {"value": "Красноленинское"}, {"value": "Краснонивское"}, {"value": "Красноярско-Куединское"}, {"value": "Красноярское"}, {"value": "Кристальное"}, {"value": "Крузенштернское"}, {"value": "Кузоваткинское"}, {"value": "Кукуштанское"}, {"value": "Кулагинское"}, {"value": "Курманаевское"}, {"value": "Кустовое"}, {"value": "Кустовое"}, {"value": "Кутулукское"}, {"value": "Куюмбинское"}, {"value": "Кыртаельское"}, {"value": "Кэралайское"}, {"value": "Лабаганское"}, {"value": "Лас-Еганское"}, {"value": "Лачаель"}, {"value": "Лебяжинское"}, {"value": "Лебяжинское"}, {"value": "Леккерское"}, {"value": "Лекхарьягинское"}, {"value": "Лесное"}, {"value": "Лобановское"}, {"value": "Локосовское"}, {"value": "Луговое"}, {"value": "Луньвожпальское"}, {"value": "Лыаельское"}, {"value": "Мало-Балыкское"}, {"value": "Малобалыкское"}, {"value": "Мамалаевское"}, {"value": "Мамонтовское"}, {"value": "Мастерьельское"}, {"value": "Мегионское"}, {"value": "Мельниковское"}, {"value": "Мензелинское"}, {"value": "Минибаевское"}, {"value": "Моргуновское"}, {"value": "Мортымья-Тетеревское"}, {"value": "Мортымья-Тетеревское"}, {"value": "Мортымья-Тетеревское"}, {"value": "Московцева"}, {"value": "Москудьинское"}, {"value": "Мушакское"}, {"value": "Мыхпайское"}, {"value": "Мядсейское"}, {"value": "Н-Кудренское"}, {"value": "Н-Любимовское"}, {"value": "Надейю"}, {"value": "Натальинское"}, {"value": "Натальинское"}, {"value": "Нерутынское"}, {"value": "Нивагальское"}, {"value": "Ново-Боголюбовское"}, {"value": "Ново-Дмитриевское"}, {"value": "Ново-Жедринское"}, {"value": "Ново-Землянское"}, {"value": "Ново-Малаховское"}, {"value": "Ново-Покурское"}, {"value": "Ново-Пурпейское"}, {"value": "Ново-Федоровское"}, {"value": "Новокрасинская"}, {"value": "Новомостовское"}, {"value": "Новоортъягунское"}, {"value": "Новосибирское"}, {"value": "Нонг-Еганское"}, {"value": "Ольгинское"}, {"value": "Ольховское"}, {"value": "Ольховское"}, {"value": "Омбинское"}, {"value": "Орехо-Ермаковское"}, {"value": "Орехово-Ермаковское"}, {"value": "Орехово-Ермаковское"}, {"value": "Островное"}, {"value": "Ошское"}, {"value": "П.Сорочинское"}, {"value": "Павловское"}, {"value": "Памятно-Сасовское"}, {"value": "Пачгинское"}, {"value": "Пашнинское"}, {"value": "Первомайское"}, {"value": "Перевозное"}, {"value": "Пермяковское"}, {"value": "Петелинское"}, {"value": "Пихтовое"}, {"value": "Пихтовское"}, {"value": "Пихтовское"}, {"value": "Повховское"}, {"value": "Повховское"}, {"value": "Пожвинское"}, {"value": "Покачевское"}, {"value": "Покомасовское"}, {"value": "Покрово-Сорочинское"}, {"value": "Покровское"}, {"value": "Потанай-Картопьинское"}, {"value": "Поточное"}, {"value": "Правдинское"}, {"value": "Правдинское"}, {"value": "Придорожное"}, {"value": "Пример месторождения"}, {"value": "Приобское"}, {"value": "Приобское"}, {"value": "Приразломное"}, {"value": "Приразломное"}, {"value": "Присклоновое"}, {"value": "Присклоновое"}, {"value": "Пробное"}, {"value": "Пронькинское"}, {"value": "Пыжельское"}, {"value": "Пякяхинское"}, {"value": "Р-Тевлинское"}, {"value": "Р/Конновское"}, {"value": "Равенское"}, {"value": "Равенское"}, {"value": "Радовское"}, {"value": "Рассохинское"}, {"value": "Расьюское"}, {"value": "Речное"}, {"value": "Ржавское"}, {"value": "Родинское"}, {"value": "Родниковское"}, {"value": "Романовское"}, {"value": "Рославльское"}, {"value": "Россихинское"}, {"value": "Росташинское"}, {"value": "Рыбкинское"}, {"value": "Рябиновое"}, {"value": "С. Макарихинское"}, {"value": "С.Краснояровское"}, {"value": "С.Никольское"}, {"value": "Савиноборское"}, {"value": "Саврушинское"}, {"value": "Сакадинское"}, {"value": "Салымское"}, {"value": "Самодуровское"}, {"value": "Самотлорское"}, {"value": "Самотлорское 13"}, {"value": "Самотлорское 14"}, {"value": "Самотлорское 2"}, {"value": "Самотлорское 3"}, {"value": "Свободное"}, {"value": "Северный Баган"}, {"value": "Северный Ванкор"}, {"value": "Северо Губкинское"}, {"value": "Северо- Ипатское"}, {"value": "Северо-Варьеганское"}, {"value": "Северо-Варьёганское"}, {"value": "Северо-Губкинское"}, {"value": "Северо-Даниловское"}, {"value": "Северо-Конитлорское"}, {"value": "Северо-Кочевское"}, {"value": "Северо-Кочевское"}, {"value": "Северо-Ореховское"}, {"value": "Северо-Островное"}, {"value": "Северо-Покачевское"}, {"value": "Северо-Покровское"}, {"value": "Северо-Покурское"}, {"value": "Северо-Поточное"}, {"value": "Северо-Савиноборское"}, {"value": "Северо-Сарембой"}, {"value": "Северо-Хохряковское"}, {"value": "Северо-Янгтинское"}, {"value": "Скворцовское"}, {"value": "Слободское"}, {"value": "Случайное"}, {"value": "Солдатовское"}, {"value": "Солдатовское"}, {"value": "Солкинское"}, {"value": "Сорочинск-Никольское"}, {"value": "Сорочинско-Никольское"}, {"value": "Сосновское"}, {"value": "Софьинское"}, {"value": "Спиридоновское"}, {"value": "Средне - Балыкское"}, {"value": "Средне-Мичаельское"}, {"value": "Средне-Угутское"}, {"value": "Средне-Харьягинское"}, {"value": "Степноозерское"}, {"value": "Суборское"}, {"value": "Сугмутское"}, {"value": "Султан-Заглядинское"}, {"value": "Суторминское"}, {"value": "Сухаревское"}, {"value": "Сюрхаратинское"}, {"value": "Тагринское"}, {"value": "Тайлаковское"}, {"value": "Тананыкское"}, {"value": "Таращанское"}, {"value": "Тевлино-Русскинское"}, {"value": "Тевлинско-Русскинское"}, {"value": "Тединское"}, {"value": "Тепловское"}, {"value": "Тестовое"}, {"value": "Титова"}, {"value": "Тобойское"}, {"value": "Токское"}, {"value": "Толумское"}, {"value": "Торовейское"}, {"value": "Требса"}, {"value": "Трубецкое"}, {"value": "Турчаниновское"}, {"value": "Угутское"}, {"value": "Узунское"}, {"value": "Умирское"}, {"value": "Умсейское"}, {"value": "Урьевское"}, {"value": "Усинское"}, {"value": "Усинское"}, {"value": "Усть-Балыкское"}, {"value": "Усть-Котухтинское"}, {"value": "Устьевое"}, {"value": "Федотовская площадь"}, {"value": "Хальмерпоютинское"}, {"value": "Хантос"}, {"value": "Харьягинское"}, {"value": "Хасырейское"}, {"value": "Хыльчаюское"}, {"value": "Чаяндинское"}, {"value": "Чекалдинское"}, {"value": "Чернушинское"}, {"value": "Черпаю"}, {"value": "Чистинное"}, {"value": "Чишминская"}, {"value": "Чумпасское"}, {"value": "Чупальское"}, {"value": "Чураковское"}, {"value": "Шароновское"}, {"value": "Шейгурчинское"}, {"value": "Школьное"}, {"value": "Шулаевское"}, {"value": "Экилорское"}, {"value": "Энтельское"}, {"value": "Ю-Выинтойское"}, {"value": "Ю-Султангуловское"}, {"value": "Ю-Урьевское"}, {"value": "Ю.Сперидоновское"}, {"value": "Юбилейное"}, {"value": "Южинское"}, {"value": "Южно Ипатское"}, {"value": "Южно Лыжского"}, {"value": "Южно Юрьяхинское"}, {"value": "Южно--Ягунское"}, {"value": "Южно-Аганское"}, {"value": "Южно-Баганское"}, {"value": "Южно-Балыкское"}, {"value": "Южно-Выинтойское"}, {"value": "Южно-Выйнтой"}, {"value": "Южно-Киняминское"}, {"value": "Южно-Кустовое"}, {"value": "Южно-Островное"}, {"value": "Южно-Покамасовское"}, {"value": "Южно-Покачевское"}, {"value": "Южно-Приобское"}, {"value": "Южно-Тарасовское"}, {"value": "Южно-Тарасовское"}, {"value": "Южно-Ягунское"}, {"value": "Южно-Ягунское"}, {"value": "Южно-арасовское"}, {"value": "Южчно-Кустовое"}, {"value": "Юрхаровское"}, {"value": "Юрчукское"}, {"value": "Ямбургское"}, {"value": "Ярегское"}, {"value": "Яреюское"}, {"value": "Яркое"}, {"value": "без названия"}, {"value": "им. А.Титова"}, {"value": "им. Алабушина"}, {"value": "им. Московцева"}, {"value": "им. Р. Требса"}, {"value": "им. Россихина"}, {"value": "скв 29956 залежь 221"}]
    try:
        for field in choices_fields:
            if row in field["value"]:
                return field["value"]
            else:
                not_find.append(row)
    except:
        pass
    
df_test["Месторождение"] = df_test["Месторождение"].apply(lambda x: convert_to_fields_names(x))


# In[34]:


dict_not_find["Месторождение"] = list(set(not_find))


# In[35]:


df_test["Месторождение"].isnull().sum()


# In[36]:


# Ищем подрядчика 
not_find = []
def convert_to_podr(row):
    choices_podr = [{"вэлл": "ООО «ВэллСервис»", "велл": "ООО «ВэллСервис»", "бке": "ООО «БКЕ» ФРС", "мастернефть": "ООО «Мастернефтьсервис»", "инс": "ООО «Мастернефтьсервис»", \
                    "евразия": "ООО «БКЕ» ФРС", "мстернефть": "ООО «Мастернефтьсервис»", "мастер-нефть": "ООО «Мастернефтьсервис»", \
                    "импульс-нефтесервис": "ООО «Мастернефтьсервис»", "инпус-нефтесервис": "ООО «Мастернефтьсервис»", "импульс нефтесервис": "ООО «Мастернефтьсервис»", \
                    "мастер - нефть": "ООО «Мастернефтьсервис»", "«мастернефтесервис»": "ООО «Мастернефтьсервис»", "мастер нефть":"ООО «Мастернефтьсервис»"}]
    try:
        if row:
            row_cleaned = re.sub(r'\s+', ' ', row.lower()) # Удаление лишних пробелов и приведение к нижнему регистру
            ans = []
            for i in choices_podr:
                for j in i.keys():
                    if j in row_cleaned:
                        ans.append(i[j])
                        return i[j]
            if not ans:
                not_find.append(row)
    except:
        pass
        

df_test["Наименование подрядчика"] = df_test["Подрядчик № бригады Мастер"].apply(convert_to_podr)


# In[37]:


dict_not_find["Наименование подрядчика"] = not_find


# In[38]:


df_test["Наименование подрядчика"].isnull().sum()


# In[39]:


# Ищем ФИО мастера бригады
not_find = []
def convert_to_master(row):
    try:
        pattern = r"[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.|[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.|[А-ЯЁ][а-яё]+\s+[А-ЯЁ]"
        families = re.findall(pattern, row)
        return families[0]
    except:
        not_find.append(row)
        return ""
    
df_test["Мастер бригады"] = df_test["Подрядчик № бригады Мастер"].apply(convert_to_master)


# In[40]:


dict_not_find["Мастер бригады"] = not_find


# In[41]:


df_test["Мастер бригады"].isnull().sum()


# In[42]:


# Ищем номер бригады
not_find = []
def extract_last_number(string):
    try:
        results = re.findall(r'\s*№?\s*(\d+)', string, re.IGNORECASE)
        return results[-1]  # Возвращаем последнее совпадение
    except:
        not_find.append(string)
        pass
df_test["Номер бригады"] = df_test["Подрядчик № бригады Мастер"].apply(extract_last_number)


# In[43]:


dict_not_find["Номер бригады"] = not_find


# In[44]:


df_test["Номер бригады"].isnull().sum()


# In[45]:


# Преобразование списков в pd.Series
for key in dict_not_find:
    dict_not_find[key] = pd.Series(dict_not_find[key])

# Создание DataFrame
df_not_find = pd.DataFrame(dict_not_find)


# In[46]:


current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df_not_find.to_excel(f"Ненайденные данные/Ненайденные данные в предписании.xlsx", index=False)


# In[47]:


# Сбрасываем индекс
df_test = df_test.reset_index(drop=True)


# In[48]:


# Переименовываем столбцы для удобства
df_test = df_test.rename(columns={
    "Дата": "dateAkt",
    "ФИО супервайзера": "superFIO",
    "Мастер бригады": "MasterFIO",
    "Скважина": "well",
    "Номер бригады": "numBrigade",
    "Месторождение": "field",
    "Наименование подрядчика": "podrCompanystep1",
    "Куст": "padstep1",
    "Выявленные нарушения по ОТ,ПБ": "violations",
})


# In[49]:


# Преобразуем дату
df_test['dateAkt'] = pd.to_datetime(df_test['dateAkt']).dt.strftime('%Y%m%dT%H:%M:%SZ')


# In[ ]:


df_test['dateAkt'] = pd.to_datetime(df_test['dateAkt'], format='%Y%m%dT%H:%M:%SZ')
given_date = pd.to_datetime(true_data, format='%Y%m%dT%H:%M:%SZ')
# Фильтруем строки, где dateAkt больше заданной даты
df_test = df_test[df_test['dateAkt'] > given_date]


# In[50]:


df_test = df_test[["dateAkt", "superFIO", "padstep1", "well", "field", "podrCompanystep1", \
                   "MasterFIO", "numBrigade", "violations", "zaknamestep1_content", "zaknamestep1"]]


# In[51]:


# Смотрим на кол-во пропусков
df_test.isnull().sum()


# In[52]:


old_shape = df_test.shape[0]


# In[53]:


#Удаляем пропуски
df_test = df_test.dropna()


# In[54]:


k = 100 - (df_test.shape[0] / old_shape * 100)
print("Количество пропусков - {} %".format(k))
if k > 15:
    raise ValueError("Слишком много пропусков - {} %".format(k))


# ### Подготка датафрейма для нарушений

# In[55]:


df_test = df_test.reset_index(drop=True)


# In[56]:


df_new_vio = df_test.copy()
df_new_vio.head()


# In[57]:


ans_name = []
ans_ref = []
for i in df_new_vio["violations"]:
    try:
        pattern = r'\([^()]*\)|\(.*\)'
        df_found = pd.DataFrame(re.findall(pattern, i))
        pattern1 = r"п\.|п \d|п№|п\d|схема.*№|статья.*№|ст\."
        df_found = df_found[df_found[0].str.contains(pattern1)].reset_index(drop=True)
        # print(df_found[0].to_list())
        ans_ref.append(df_found[0].to_list())
        other_text = re.split(r"|".join(df_found[0].to_list()).replace("(","\\(").replace(")","\\)"),i)
        filtered_values = [value for value in other_text if value.strip()]
        ans_name.append(filtered_values)
    except:
        ans_ref.append([])
        ans_name.append([])


# In[58]:


df_new_vio["violations_name"] = ans_name
df_new_vio["violations_reference"] = ans_ref


# In[59]:


df_new_copy = df_new_vio.copy()


# In[68]:


# Датафрейм для определения пунктов и добавления в tbl_contents
df_excel = pd.read_excel(folder_path_excel)
df_excel = df_excel.dropna()
df_excel["references"] = df_excel["references"].apply(lambda x: x.strip())
df_excel["all_items_reference"] = df_excel["all_items_reference"].apply(lambda x: x.strip())
df_excel = df_excel.astype({"all_items_id": "int"})


# In[61]:


# Ищем пункты в датафрейме df_excel
not_found = []
for index, row in df_new_copy.iterrows():
    updated_violations = []
    violations = row["violations_name"]
    references = row["violations_reference"]
    
    # Добавляем проверку на пустоту списков
    if violations and references:
        diff = len(violations) - len(references)
        if diff > 0:
            references.extend([references[-1]] * abs(diff))
        elif diff < 0:
            violations.extend([violations[-1]] * abs(diff))

        for i in range(len(violations)):
            violation = violations[i]
            reference = references[i]

            cnt = 0
            for index_excel, row_excel in df_excel.iterrows():
                if row_excel["all_items_reference"] in reference:
                    cnt += 1
                    d = {
                        "riskLevel": "",
                        "violationName": violation,
                        "violationDescription": [{
                            "id": row_excel["all_items_id"],
                            "reference": row_excel["references"],
                            "shortText": row_excel["short_text"],
                            "normativeDocument": row_excel["normative_document_name"],
                            "normativeChapter": row_excel["chaptername"]
                        }]
                    }
                    updated_violations.append(d)
                    break
    if not updated_violations:
        not_found.append(references)
        
    df_new_copy.at[index, "violations"] = updated_violations


# In[62]:


current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
not_found_final = []
for i in not_found:
    for j in i:
        not_found_final.append(j)


# In[ ]:


df_join = pd.DataFrame({"all_items_reference": list(set(not_found_final))})
result_references = pd.concat([df_excel, df_join], ignore_index=True)


# In[ ]:


result_references.to_excel(f"Ненайденные ссылки/Ненайденные ссылки в предписании {current_date}.xlsx", index=False)


# In[63]:


old_shape = df_new_copy.shape[0]


# In[64]:


df_new_copy = df_new_copy[df_new_copy['violations'].apply(lambda x: len(x) != 0)]


# In[65]:


df_new_copy = df_new_copy.drop(columns=["violations_name", "violations_reference"])


# In[66]:


df_new_copy.shape


# In[67]:


k = 100 - (df_new_copy.shape[0]/old_shape * 100)
print("Кол-во ссылок, которые не нашли - {} %".format(round(k,2)))
if k > 10:
    raise ValueError("Кол-во ссылок, которые не нашли - {} %".format(round(k,2)))


# ### Формирование поля content

# In[252]:


df_new_for_content = df_new_copy.copy()


# In[253]:


from datetime import datetime
from babel.dates import format_date, format_datetime, format_time

def process_dates(date_str):
    date_dt = datetime.strptime(date_str, "%Y%m%dT%H:%M:%SZ")
    date_hour = date_dt.strftime("%H")
    date_minute = date_dt.strftime("%M")
    date_main = format_date(date_dt, "dd MMMM yyyy", locale='ru')
    date_time = format_datetime(date_dt, "HH:mm, dd.MM.yyyy", locale='ru')
    dateTime = format_datetime(date_dt, "dd MMMM HH", locale='ru')
    dateTime2 = format_datetime(date_dt, "dd/MMMM/yy", locale='ru')
    dateAktquotes = date_dt.strftime('%d %m')
    dateAktchbegin = date_dt.strftime('%d')
    return date_hour, date_minute, date_main, date_time, dateTime, dateTime2, dateAktquotes, dateAktchbegin

df_new_for_content[['dateAktHour', 'dateAktMinute', 'dateAktmain', 'dateAkttime', 'dateTime', 'dateTime2', 'dateAktquotes', 'dateAktchbegin']] = df_new_for_content['dateAkt'].apply(
    lambda x: pd.Series(process_dates(x))
)


# In[254]:


df_new_for_content.head()


# In[255]:


# Окончательно формируем поле content
res_content = []
for index, row in df_new_for_content.iterrows():
    dic = {
        "superFIO": row[1],
        "superCompany ": "АО «НИПЦ ГНТ»",
        "superPositionkem ": "Супервайзером",
        "inPresence": "",
        "dateTime": row[15],
        "dateTime2": row[16],
        "dateAktquotes": row[17],
        "dateAktmain": row[13],
        "dateAktchbegin": row[18],
        "dateAkttime": row[14],
        "nomerAktaKomiFreestep1": "",
        "padstep1": row[2],
        "wellstep1": row[3],
        "typeJobstep1": "",
        "superSignat": "",
        "podrSignat": "",
        "tbl_contents": row[8],
        "numBrigade": row[7],
        "cdng": "",
        "findings": "",
        "perfName": "",
        "applicat": "",
        "podrFIO": row[6],
        "podrPosition": "",
        "special_opinion": ""
    }
    res_content.append(dic)


# ### Формируем поле raw_content

# In[256]:


df_new_raw_content = df_new_copy.copy()
df_new_raw_content.head()


# In[257]:


podrCompany = [
    {"id": "0", "value": "ООО «ВэллСервис»"},
    {"id": "1", "value": "ООО «Мастернефтьсервис»"},
    {"id": "2", "value": "ООО «БКЕ» ФРС"}
]
podrCompanyCurr = ["ВэллСервис", "нефть", "БКЕ"]
lookup_dict = {}
for company in podrCompany:
    for curr_name in podrCompanyCurr:
        if curr_name in company["value"]:
            lookup_dict[curr_name] = company              


# In[258]:


def replace_company(row):
    for key, value in lookup_dict.items():
        if key in row["podrCompanystep1"]:
            return value
    return row["podrCompanystep1"]

df_new_raw_content["podrCompanystep1"] = df_new_raw_content.apply(replace_company, axis=1)


# In[259]:


def convert_to_superfio(x):
    d = {
        "value": x,
        "payload": {
            "superFIOkem": x,
            "superCompany": "АО «НИПЦ ГНТ»",
            "superPositionkem": "Супервайзером"
        }
    }
    return d

df_new_raw_content["superFIO"] = df_new_raw_content["superFIO"].apply(lambda x: convert_to_superfio(x))


# In[260]:


def convert_to_field_raw(x):
    fields = [{'choices': [{'value': 'Абино-Украинское', 'id': 0}, {'value': 'Абрамовское', 'id': 1}, {'value': 'Аганское', 'id': 2}, {'value': 'Акташское', 'id': 3}, {'value': 'Алексеевская', 'id': 4}, {'value': 'Алисовское', 'id': 5}, {'value': 'Амдермаельское', 'id': 6}, {'value': 'Андреевское', 'id': 7}, {'value': 'Антиповско-Балыклейская', 'id': 8}, {'value': 'Аригольское', 'id': 9}, {'value': 'Аспинское', 'id': 10}, {'value': 'Атамановское', 'id': 11}, {'value': 'Ачимовское', 'id': 12}, {'value': 'Ашальчинское', 'id': 13}, {'value': 'Баганское', 'id': 14}, {'value': 'Баклановское', 'id': 15}, {'value': 'Барсуковское', 'id': 16}, {'value': 'Батырбайское', 'id': 17}, {'value': 'Бахиловское', 'id': 18}, {'value': 'Бахиловское', 'id': 19}, {'value': 'Бахтияровское', 'id': 20}, {'value': 'Бельское', 'id': 21}, {'value': 'Береговое', 'id': 22}, {'value': 'Бобровское', 'id': 23}, {'value': 'Бобровское', 'id': 24}, {'value': 'Бобровское', 'id': 25}, {'value': 'Боголюбовское', 'id': 26}, {'value': 'Больше-Каменское', 'id': 27}, {'value': 'Бузулукское', 'id': 28}, {'value': 'В-Волостновское', 'id': 29}, {'value': 'В-боголюбовское', 'id': 30}, {'value': 'В. Сарутаюсское', 'id': 31}, {'value': 'В.Капитоновское', 'id': 32}, {'value': 'В.Малаховское', 'id': 33}, {'value': 'Ван еганское', 'id': 34}, {'value': 'Ван-Еганское', 'id': 35}, {'value': 'Ван-Еганское бур.', 'id': 36}, {'value': 'Ванкор', 'id': 37}, {'value': 'Варьёганское', 'id': 38}, {'value': 'Ватинское', 'id': 39}, {'value': 'Ватинское', 'id': 40}, {'value': 'Ватинское', 'id': 41}, {'value': 'Вать-Ёганское', 'id': 42}, {'value': 'Вать-Еганское', 'id': 43}, {'value': 'Ватьеганское', 'id': 44}, {'value': 'Вахитовское', 'id': 45}, {'value': 'Верхнеколик-Еганское', 'id': 46}, {'value': 'Верхнеколик-Еганское', 'id': 47}, {'value': 'Видное', 'id': 48}, {'value': 'Викторинское', 'id': 49}, {'value': 'Винниковское', 'id': 50}, {'value': 'Вишневское', 'id': 51}, {'value': 'Возей', 'id': 52}, {'value': 'Возейское', 'id': 53}, {'value': 'Волостновское', 'id': 54}, {'value': 'Воробьевское', 'id': 55}, {'value': 'Восточно- Мастерьельское', 'id': 56}, {'value': 'Восточно-Икилорское', 'id': 57}, {'value': 'Восточно-Икилорское', 'id': 58}, {'value': 'Восточно-Кустовое', 'id': 59}, {'value': 'Восточно-Макаровское', 'id': 60}, {'value': 'Восточно-Перевальное', 'id': 61}, {'value': 'Восточно-Перевальное', 'id': 62}, {'value': 'Восточно-Правдинское', 'id': 63}, {'value': 'Восточно-Придорожное', 'id': 64}, {'value': 'Восточно-Придорожное', 'id': 65}, {'value': 'Восточно-Придорожное', 'id': 66}, {'value': 'Восточно-Пякутинское', 'id': 67}, {'value': 'Восточно-Сарутаюское', 'id': 68}, {'value': 'Восточно-Сургутское', 'id': 69}, {'value': 'Восточно-Токайское', 'id': 70}, {'value': 'Восточно-Ягунское', 'id': 71}, {'value': 'Восточнро-Придорожное', 'id': 72}, {'value': 'Встречное', 'id': 73}, {'value': 'Вынгаяхинское', 'id': 74}, {'value': 'Гаршинское', 'id': 75}, {'value': 'Герасимовское', 'id': 76}, {'value': 'Геркулесовское', 'id': 77}, {'value': 'Гондыревское', 'id': 78}, {'value': 'Горное', 'id': 79}, {'value': 'Графское', 'id': 80}, {'value': 'Даниловское', 'id': 81}, {'value': 'Демаельская', 'id': 82}, {'value': 'Довыдовское', 'id': 83}, {'value': 'Дозорцевское', 'id': 84}, {'value': 'Долговское', 'id': 85}, {'value': 'Долговское', 'id': 86}, {'value': 'Долговское', 'id': 87}, {'value': 'Дон-Сыртовское', 'id': 88}, {'value': 'Донская', 'id': 89}, {'value': 'Дороховское', 'id': 90}, {'value': 'Дружное', 'id': 91}, {'value': 'Дружное', 'id': 92}, {'value': 'Е.Зыковское', 'id': 93}, {'value': 'Енапаевское', 'id': 94}, {'value': 'Енорусскинское', 'id': 95}, {'value': 'Етыпуровское', 'id': 96}, {'value': 'Жилинское', 'id': 97}, {'value': 'Журавское', 'id': 98}, {'value': 'Загорское', 'id': 99}, {'value': 'Залесское', 'id': 100}, {'value': 'Зап-Угутское', 'id': 101}, {'value': 'Зап-Усть -Былыкское', 'id': 102}, {'value': 'Западно-Асомкинское', 'id': 103}, {'value': 'Западно-Бимское', 'id': 104}, {'value': 'Западно-Варьёганское', 'id': 105}, {'value': 'Западно-Икилорское', 'id': 106}, {'value': 'Западно-Катыльгинское', 'id': 107}, {'value': 'Западно-Кулагинское', 'id': 108}, {'value': 'Западно-Малобалыкское', 'id': 109}, {'value': 'Западно-Пурпейское', 'id': 110}, {'value': 'Западно-Степановское', 'id': 111}, {'value': 'Западно-Тугровское', 'id': 112}, {'value': 'Западно-Эргинское', 'id': 113}, {'value': 'Западное Сюрхаратинское', 'id': 114}, {'value': 'Западный Могутлор', 'id': 115}, {'value': 'Зимнее', 'id': 116}, {'value': 'Ивановское', 'id': 117}, {'value': 'Икилорское', 'id': 118}, {'value': 'Икилорское', 'id': 119}, {'value': 'Ильичевское', 'id': 120}, {'value': 'Имилорское', 'id': 121}, {'value': 'Инзырейское', 'id': 122}, {'value': 'Ининское', 'id': 123}, {'value': 'Ининское', 'id': 124}, {'value': 'Ипатское', 'id': 125}, {'value': 'Ишуевское', 'id': 126}, {'value': 'Казыгашевское', 'id': 127}, {'value': 'Калиннинковское', 'id': 128}, {'value': 'Калмиярское', 'id': 129}, {'value': 'Камеликское', 'id': 130}, {'value': 'Каменское', 'id': 131}, {'value': 'Каменское', 'id': 132}, {'value': 'Кетовское', 'id': 133}, {'value': 'Кечимовское', 'id': 134}, {'value': 'Киндельское', 'id': 135}, {'value': 'Кинзельское', 'id': 136}, {'value': 'Киняминское', 'id': 137}, {'value': 'Кичкасское', 'id': 138}, {'value': 'Киязлинское', 'id': 139}, {'value': 'Ключевое', 'id': 140}, {'value': 'Ковыктинское ГКМ', 'id': 141}, {'value': 'Кодяковское', 'id': 142}, {'value': 'Колвинское', 'id': 143}, {'value': 'Командишорское', 'id': 144}, {'value': 'Комсомольское', 'id': 145}, {'value': 'Кондинское', 'id': 146}, {'value': 'Корниловское', 'id': 147}, {'value': 'Кочевское', 'id': 148}, {'value': 'Кочевское', 'id': 149}, {'value': 'Кошильское', 'id': 150}, {'value': 'Крайнее', 'id': 151}, {'value': 'Крапивинское', 'id': 152}, {'value': 'Красное', 'id': 153}, {'value': 'Красноленинское', 'id': 154}, {'value': 'Красноленинское', 'id': 155}, {'value': 'Краснонивское', 'id': 156}, {'value': 'Красноярско-Куединское', 'id': 157}, {'value': 'Красноярское', 'id': 158}, {'value': 'Кристальное', 'id': 159}, {'value': 'Крузенштернское', 'id': 160}, {'value': 'Кузоваткинское', 'id': 161}, {'value': 'Кукуштанское', 'id': 162}, {'value': 'Кулагинское', 'id': 163}, {'value': 'Курманаевское', 'id': 164}, {'value': 'Кустовое', 'id': 165}, {'value': 'Кустовое', 'id': 166}, {'value': 'Кутулукское', 'id': 167}, {'value': 'Куюмбинское', 'id': 168}, {'value': 'Кыртаельское', 'id': 169}, {'value': 'Кэралайское', 'id': 170}, {'value': 'Лабаганское', 'id': 171}, {'value': 'Лас-Еганское', 'id': 172}, {'value': 'Лачаель', 'id': 173}, {'value': 'Лебяжинское', 'id': 174}, {'value': 'Лебяжинское', 'id': 175}, {'value': 'Леккерское', 'id': 176}, {'value': 'Лекхарьягинское', 'id': 177}, {'value': 'Лесное', 'id': 178}, {'value': 'Лобановское', 'id': 179}, {'value': 'Локосовское', 'id': 180}, {'value': 'Луговое', 'id': 181}, {'value': 'Луньвожпальское', 'id': 182}, {'value': 'Лыаельское', 'id': 183}, {'value': 'Мало-Балыкское', 'id': 184}, {'value': 'Малобалыкское', 'id': 185}, {'value': 'Мамалаевское', 'id': 186}, {'value': 'Мамонтовское', 'id': 187}, {'value': 'Мастерьельское', 'id': 188}, {'value': 'Мегионское', 'id': 189}, {'value': 'Мельниковское', 'id': 190}, {'value': 'Мензелинское', 'id': 191}, {'value': 'Минибаевское', 'id': 192}, {'value': 'Моргуновское', 'id': 193}, {'value': 'Мортымья-Тетеревское', 'id': 194}, {'value': 'Мортымья-Тетеревское', 'id': 195}, {'value': 'Мортымья-Тетеревское', 'id': 196}, {'value': 'Московцева', 'id': 197}, {'value': 'Москудьинское', 'id': 198}, {'value': 'Мушакское', 'id': 199}, {'value': 'Мыхпайское', 'id': 200}, {'value': 'Мядсейское', 'id': 201}, {'value': 'Н-Кудренское', 'id': 202}, {'value': 'Н-Любимовское', 'id': 203}, {'value': 'Надейю', 'id': 204}, {'value': 'Натальинское', 'id': 205}, {'value': 'Натальинское', 'id': 206}, {'value': 'Нерутынское', 'id': 207}, {'value': 'Нивагальское', 'id': 208}, {'value': 'Ново-Боголюбовское', 'id': 209}, {'value': 'Ново-Дмитриевское', 'id': 210}, {'value': 'Ново-Жедринское', 'id': 211}, {'value': 'Ново-Землянское', 'id': 212}, {'value': 'Ново-Малаховское', 'id': 213}, {'value': 'Ново-Покурское', 'id': 214}, {'value': 'Ново-Пурпейское', 'id': 215}, {'value': 'Ново-Федоровское', 'id': 216}, {'value': 'Новокрасинская', 'id': 217}, {'value': 'Новомостовское', 'id': 218}, {'value': 'Новоортъягунское', 'id': 219}, {'value': 'Новосибирское', 'id': 220}, {'value': 'Нонг-Еганское', 'id': 221}, {'value': 'Ольгинское', 'id': 222}, {'value': 'Ольховское', 'id': 223}, {'value': 'Ольховское', 'id': 224}, {'value': 'Омбинское', 'id': 225}, {'value': 'Орехо-Ермаковское', 'id': 226}, {'value': 'Орехово-Ермаковское', 'id': 227}, {'value': 'Орехово-Ермаковское', 'id': 228}, {'value': 'Островное', 'id': 229}, {'value': 'Ошское', 'id': 230}, {'value': 'П.Сорочинское', 'id': 231}, {'value': 'Павловское', 'id': 232}, {'value': 'Памятно-Сасовское', 'id': 233}, {'value': 'Пачгинское', 'id': 234}, {'value': 'Пашнинское', 'id': 235}, {'value': 'Первомайское', 'id': 236}, {'value': 'Перевозное', 'id': 237}, {'value': 'Пермяковское', 'id': 238}, {'value': 'Петелинское', 'id': 239}, {'value': 'Пихтовое', 'id': 240}, {'value': 'Пихтовское', 'id': 241}, {'value': 'Пихтовское', 'id': 242}, {'value': 'Повховское', 'id': 243}, {'value': 'Повховское', 'id': 244}, {'value': 'Пожвинское', 'id': 245}, {'value': 'Покачевское', 'id': 246}, {'value': 'Покомасовское', 'id': 247}, {'value': 'Покрово-Сорочинское', 'id': 248}, {'value': 'Покровское', 'id': 249}, {'value': 'Потанай-Картопьинское', 'id': 250}, {'value': 'Поточное', 'id': 251}, {'value': 'Правдинское', 'id': 252}, {'value': 'Правдинское', 'id': 253}, {'value': 'Придорожное', 'id': 254}, {'value': 'Пример месторождения', 'id': 255}, {'value': 'Приобское', 'id': 256}, {'value': 'Приобское', 'id': 257}, {'value': 'Приразломное', 'id': 258}, {'value': 'Приразломное', 'id': 259}, {'value': 'Присклоновое', 'id': 260}, {'value': 'Присклоновое', 'id': 261}, {'value': 'Пробное', 'id': 262}, {'value': 'Пронькинское', 'id': 263}, {'value': 'Пыжельское', 'id': 264}, {'value': 'Пякяхинское', 'id': 265}, {'value': 'Р-Тевлинское', 'id': 266}, {'value': 'Р/Конновское', 'id': 267}, {'value': 'Равенское', 'id': 268}, {'value': 'Равенское', 'id': 269}, {'value': 'Радовское', 'id': 270}, {'value': 'Рассохинское', 'id': 271}, {'value': 'Расьюское', 'id': 272}, {'value': 'Речное', 'id': 273}, {'value': 'Ржавское', 'id': 274}, {'value': 'Родинское', 'id': 275}, {'value': 'Родниковское', 'id': 276}, {'value': 'Романовское', 'id': 277}, {'value': 'Рославльское', 'id': 278}, {'value': 'Россихинское', 'id': 279}, {'value': 'Росташинское', 'id': 280}, {'value': 'Рыбкинское', 'id': 281}, {'value': 'Рябиновое', 'id': 282}, {'value': 'С. Макарихинское', 'id': 283}, {'value': 'С.Краснояровское', 'id': 284}, {'value': 'С.Никольское', 'id': 285}, {'value': 'Савиноборское', 'id': 286}, {'value': 'Саврушинское', 'id': 287}, {'value': 'Сакадинское', 'id': 288}, {'value': 'Салымское', 'id': 289}, {'value': 'Самодуровское', 'id': 290}, {'value': 'Самотлорское', 'id': 291}, {'value': 'Самотлорское 13', 'id': 292}, {'value': 'Самотлорское 14', 'id': 293}, {'value': 'Самотлорское 2', 'id': 294}, {'value': 'Самотлорское 3', 'id': 295}, {'value': 'Свободное', 'id': 296}, {'value': 'Северный Баган', 'id': 297}, {'value': 'Северный Ванкор', 'id': 298}, {'value': 'Северо Губкинское', 'id': 299}, {'value': 'Северо- Ипатское', 'id': 300}, {'value': 'Северо-Варьеганское', 'id': 301}, {'value': 'Северо-Варьёганское', 'id': 302}, {'value': 'Северо-Губкинское', 'id': 303}, {'value': 'Северо-Даниловское', 'id': 304}, {'value': 'Северо-Конитлорское', 'id': 305}, {'value': 'Северо-Кочевское', 'id': 306}, {'value': 'Северо-Кочевское', 'id': 307}, {'value': 'Северо-Ореховское', 'id': 308}, {'value': 'Северо-Островное', 'id': 309}, {'value': 'Северо-Покачевское', 'id': 310}, {'value': 'Северо-Покровское', 'id': 311}, {'value': 'Северо-Покурское', 'id': 312}, {'value': 'Северо-Поточное', 'id': 313}, {'value': 'Северо-Савиноборское', 'id': 314}, {'value': 'Северо-Сарембой', 'id': 315}, {'value': 'Северо-Хохряковское', 'id': 316}, {'value': 'Северо-Янгтинское', 'id': 317}, {'value': 'Скворцовское', 'id': 318}, {'value': 'Слободское', 'id': 319}, {'value': 'Случайное', 'id': 320}, {'value': 'Солдатовское', 'id': 321}, {'value': 'Солдатовское', 'id': 322}, {'value': 'Солкинское', 'id': 323}, {'value': 'Сорочинск-Никольское', 'id': 324}, {'value': 'Сорочинско-Никольское', 'id': 325}, {'value': 'Сосновское', 'id': 326}, {'value': 'Софьинское', 'id': 327}, {'value': 'Спиридоновское', 'id': 328}, {'value': 'Средне - Балыкское', 'id': 329}, {'value': 'Средне-Мичаельское', 'id': 330}, {'value': 'Средне-Угутское', 'id': 331}, {'value': 'Средне-Харьягинское', 'id': 332}, {'value': 'Степноозерское', 'id': 333}, {'value': 'Суборское', 'id': 334}, {'value': 'Сугмутское', 'id': 335}, {'value': 'Султан-Заглядинское', 'id': 336}, {'value': 'Суторминское', 'id': 337}, {'value': 'Сухаревское', 'id': 338}, {'value': 'Сюрхаратинское', 'id': 339}, {'value': 'Тагринское', 'id': 340}, {'value': 'Тайлаковское', 'id': 341}, {'value': 'Тананыкское', 'id': 342}, {'value': 'Таращанское', 'id': 343}, {'value': 'Тевлино-Русскинское', 'id': 344}, {'id': 345, 'value': 'Тевлинско-Русскинское'}, {'value': 'Тединское', 'id': 346}, {'value': 'Тепловское', 'id': 347}, {'value': 'Тестовое', 'id': 348}, {'value': 'Титова', 'id': 349}, {'value': 'Тобойское', 'id': 350}, {'value': 'Токское', 'id': 351}, {'value': 'Толумское', 'id': 352}, {'value': 'Торовейское', 'id': 353}, {'value': 'Требса', 'id': 354}, {'value': 'Трубецкое', 'id': 355}, {'value': 'Турчаниновское', 'id': 356}, {'value': 'Угутское', 'id': 357}, {'value': 'Узунское', 'id': 358}, {'value': 'Умирское', 'id': 359}, {'value': 'Умсейское', 'id': 360}, {'value': 'Урьевское', 'id': 361}, {'value': 'Усинское', 'id': 362}, {'value': 'Усинское', 'id': 363}, {'value': 'Усть-Балыкское', 'id': 364}, {'value': 'Усть-Котухтинское', 'id': 365}, {'value': 'Устьевое', 'id': 366}, {'value': 'Федотовская площадь', 'id': 367}, {'value': 'Хальмерпоютинское', 'id': 368}, {'value': 'Хантос', 'id': 369}, {'value': 'Харьягинское', 'id': 370}, {'value': 'Хасырейское', 'id': 371}, {'value': 'Хыльчаюское', 'id': 372}, {'value': 'Чаяндинское', 'id': 373}, {'value': 'Чекалдинское', 'id': 374}, {'value': 'Чернушинское', 'id': 375}, {'value': 'Черпаю', 'id': 376}, {'value': 'Чистинное', 'id': 377}, {'value': 'Чишминская', 'id': 378}, {'value': 'Чумпасское', 'id': 379}, {'value': 'Чупальское', 'id': 380}, {'value': 'Чураковское', 'id': 381}, {'value': 'Шароновское', 'id': 382}, {'value': 'Шейгурчинское', 'id': 383}, {'value': 'Школьное', 'id': 384}, {'value': 'Шулаевское', 'id': 385}, {'value': 'Экилорское', 'id': 386}, {'value': 'Энтельское', 'id': 387}, {'value': 'Ю-Выинтойское', 'id': 388}, {'value': 'Ю-Султангуловское', 'id': 389}, {'value': 'Ю-Урьевское', 'id': 390}, {'value': 'Ю.Сперидоновское', 'id': 391}, {'value': 'Юбилейное', 'id': 392}, {'value': 'Южинское', 'id': 393}, {'value': 'Южно Ипатское', 'id': 394}, {'value': 'Южно Лыжского', 'id': 395}, {'value': 'Южно Юрьяхинское', 'id': 396}, {'value': 'Южно--Ягунское', 'id': 397}, {'value': 'Южно-Аганское', 'id': 398}, {'value': 'Южно-Баганское', 'id': 399}, {'value': 'Южно-Балыкское', 'id': 400}, {'value': 'Южно-Выинтойское', 'id': 401}, {'value': 'Южно-Выйнтой', 'id': 402}, {'value': 'Южно-Киняминское', 'id': 403}, {'value': 'Южно-Кустовое', 'id': 404}, {'value': 'Южно-Островное', 'id': 405}, {'value': 'Южно-Покамасовское', 'id': 406}, {'value': 'Южно-Покачевское', 'id': 407}, {'value': 'Южно-Приобское', 'id': 408}, {'value': 'Южно-Тарасовское', 'id': 409}, {'value': 'Южно-Тарасовское', 'id': 410}, {'value': 'Южно-Ягунское', 'id': 411}, {'value': 'Южно-Ягунское', 'id': 412}, {'value': 'Южно-арасовское', 'id': 413}, {'value': 'Южчно-Кустовое', 'id': 414}, {'value': 'Юрхаровское', 'id': 415}, {'value': 'Юрчукское', 'id': 416}, {'value': 'Ямбургское', 'id': 417}, {'value': 'Ярегское', 'id': 418}, {'value': 'Яреюское', 'id': 419}, {'value': 'Яркое', 'id': 420}, {'value': 'без названия', 'id': 421}, {'value': 'им. А.Титова', 'id': 422}, {'value': 'им. Алабушина', 'id': 423}, {'value': 'им. Московцева', 'id': 424}, {'value': 'им. Р. Требса', 'id': 425}, {'value': 'им. Россихина', 'id': 426}, {'value': 'скв 29956 залежь 221', 'id': 427}]}]
    for field in fields[0]["choices"]:
        if x in field["value"]:
            return field
df_new_raw_content["field"] = df_new_raw_content["field"].apply(lambda x: convert_to_field_raw(x))


# In[261]:


def convert_dateAkt_to_date(date_string):
    date = pd.to_datetime(date_string, format='%Y%m%dT%H:%M:%S%z')
    formatted_date = date.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    return formatted_date

df_new_raw_content["dateAkt"] = df_new_raw_content["dateAkt"].apply(lambda x: convert_dateAkt_to_date(x))


# In[262]:


df_new_raw_content = df_new_raw_content[df_new_raw_content['violations'].apply(lambda x: x != [])]


# In[263]:


df_new_raw_content.head()


# In[264]:


# Окончательно формируем raw_content
res_raw_content = []
for index, row in df_new_raw_content.iterrows():
    dic = {
        "superFIO": row[1],
        "inPresence": "",
        "podrCompanystep1": row[5],
        "dateAkt": row[0],
        "zaknamestep1": row[10],
        "nomerAktaKomiFreestep1": "",
        "field": row[4],
        "padstep1": row[2],
        "wellstep1": row[3],
        "typeJobstep1": "",
        "superSignat": "",
        "podrSignat": "",
        "tbl_contents": row[8],
        "numBrigade": row[7],
        "cdng": "",
        "findings": "",
        "perfName": "",
        "applicat": "",
        "podrFIO": row[6],
        "podrPosition": "",
        "special_opinion": ""
    }
    res_raw_content.append(dic)


# In[265]:


df_new_for_content.shape


# ### Формируем датафрейм для запроса на нарушения

# In[266]:


df_for_violations_request = pd.DataFrame()


# In[267]:


df_for_violations_request["content"] = df_new_for_content.violations


# In[268]:


def add_attachments_to_violations(row):
    try:
        for i in row:
            i["attachments"] = []
        return row
    except:
        pass

df_for_violations_request["raw_content"] = df_for_violations_request["content"].apply(add_attachments_to_violations)


# In[269]:


def normative_articles_to_violations(row):
    try:
        normative_articles = []
        for i in row:
            for j in i["violationDescription"]:
                normative_articles.append(j["id"])
        return normative_articles
    except:
        pass
df_for_violations_request["normative_articles"] = df_for_violations_request.raw_content.apply(normative_articles_to_violations)


# In[270]:


def convert_to_name_violation(row):
    try:
        name = []
        for i in row:
            name.append(i["violationName"])
        return name
    except:
        pass
        
df_for_violations_request["name"] = df_for_violations_request.raw_content.apply(convert_to_name_violation)


# In[271]:


df_for_violations_request = df_for_violations_request.assign(environment_state=ENVIROMENT_STATE)
empty_lists_files = [[] for _ in range(len(df_for_violations_request))]
df_for_violations_request = df_for_violations_request.assign(files=empty_lists_files)


# In[272]:


# Получаем id супервайзеров
import json
import requests
s = requests.Session()
r = s.request('POST','https://ma.gasoilcenter.ru/api/token/obtain/', json = {"username":"admin","password":"Yasin1367!"})
m = (eval(r.text))["access"]
response = s.get('https://ma.gasoilcenter.ru/api/user/get-user/?is_mobile=false', headers={'Authorization':f"JWT {m}"})
response = json.loads(response.text)
users_id = []
users_first_name = []
users_last_name = []
for i in response:
    users_id.append(i["id"])
    users_first_name.append(i["first_name"])
    users_last_name.append(i["last_name"])

df_users_all = pd.DataFrame({
    "id": users_id,
    "last_name": users_last_name,
    "first_name": users_first_name
})


# In[273]:


df_for_violations_request["superFIO"] = df_new_raw_content.superFIO


# In[274]:


df_for_violations_request = df_for_violations_request.dropna()


# In[275]:


# Получаем id супервайзера по его ФИО
users_id_true = []
not_users = []
for record in df_for_violations_request['superFIO']:
    super_fio_full = record["value"]
    if super_fio_full == 'Габуллин Р.Р':
        record["value"] = 'Габдуллин Р.Р'
        super_fio_full = 'Габдуллин Р.Р'
    elif super_fio_full == 'Субботн В.А' or super_fio_full == 'Суботин В.А':
        record["value"] = 'Субботин В.А'
        super_fio_full = 'Субботин В.А'
    elif super_fio_full == 'Королмыцев Н.В':
        record["value"] = 'Коломыцев Н.В.'
        super_fio_full = 'Коломыцев Н.В.'
    super_fio_parts = super_fio_full.split()
    last_name = super_fio_parts[0] if super_fio_parts else None
    start_first_name = super_fio_parts[1][0] if super_fio_parts else None
    try:
        users_id_true.append(df_users_all[(df_users_all["last_name"] == last_name) & (df_users_all["first_name"].str.startswith(start_first_name))].id.iloc[0])
    except: 
        users_id_true.append("нет id в системе")
        not_users.append(super_fio_full)
        # break


# In[276]:


if not_users:
    raise ValueError("Найдены несуществующие пользователи - {}".format(not_users))


# In[277]:


# Проверяем что мы обработали всех супервайзеров
len(users_id_true) == df_for_violations_request.shape[0]


# In[278]:


df_for_violations_request = df_for_violations_request.assign(creator = users_id_true)


# In[279]:


df_for_violations_request = df_for_violations_request.drop(columns="superFIO")


# In[280]:


df_for_violations_request.head()


# In[281]:


df_for_violations_request.shape


# ### Формируем столбцы акта

# In[282]:


def process_dates_raw(date_str):
    date = pd.to_datetime(date_str, format='%Y%m%dT%H:%M:%SZ')
    formatted_date = date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return formatted_date

df_new_for_content['dateAkt'] = df_new_for_content['dateAkt'].apply(
            lambda x: pd.Series(process_dates_raw(x))
        )


# In[283]:


df_new_for_content.head()


# In[284]:


df_final = pd.DataFrame()
df_final["name"] = df_new_for_content['dateAkt'].apply(lambda x: "Предписание ЛЗС-КРС_sign. " + x)


# In[285]:


# Заносим оставшиеся столбцы
df_final = df_final.assign(status="signed")
df_final = df_final.assign(environment_state=ENVIROMENT_STATE)
df_final = df_final.assign(content=res_content)
df_final = df_final.assign(raw_content=res_raw_content)

# Создаем пустые списки для каждой строки в DataFrame
empty_lists_files = [[] for _ in range(len(df_final))]
empty_lists_violations = [[] for _ in range(len(df_final))]

df_final = df_final.assign(files=empty_lists_files)
df_final = df_final.assign(violations=empty_lists_violations)


# In[286]:


# Проверям, что кол-во нарушений совпадает
len(res_content) == len(res_raw_content)


# In[287]:


# Получаем id супервайзера по его ФИО
users_id_true = []
not_users = []
for record in df_final['raw_content']:
    super_fio_full = record["superFIO"]["value"]
    if super_fio_full == 'Габуллин Р.Р':
        record["superFIO"]["value"] = 'Габдуллин Р.Р'
        super_fio_full = 'Габдуллин Р.Р'
    elif super_fio_full == 'Субботн В.А' or super_fio_full == 'Суботин В.А':
        record["superFIO"]["value"] = 'Субботин В.А'
        super_fio_full = 'Субботин В.А'
    super_fio_parts = super_fio_full.split()
    date_akt = record["dateAkt"]
    last_name = super_fio_parts[0] if super_fio_parts else None
    start_first_name = super_fio_parts[1][0] if super_fio_parts else None
    try:
        users_id_true.append(df_users_all[(df_users_all["last_name"] == last_name) & (df_users_all["first_name"].str.startswith(start_first_name))].id.iloc[0])
    except: 
        users_id_true.append("нет id в системе")
        not_users.append(super_fio_full)
        # break


# In[288]:


set(not_users)


# In[289]:


# Проверяем что мы обработали всех супервайзеров
len(users_id_true) == df_final.shape[0]


# In[290]:


df_final = df_final.assign(creator = users_id_true)


# In[291]:


df_final.head()


# ### Загружаем нарушения в базу

# In[292]:


df_final = df_final.reset_index(drop=True)
df_for_violations_request = df_for_violations_request.reset_index(drop=True)


# In[293]:


print(f"Размер итогового датафрейма: {df_final.shape}")


# ### Расскоментировать код снизу после выполнения всей работы и запустить скрипт

# In[294]:


# # Отправляем запрос
# import requests
# s = requests.Session()
# r = s.request('POST','https://ma.gasoilcenter.ru/api/token/obtain/',json = {"username":"admin","password":"Yasin1367!"})
# m = (eval(r.text))["access"]
# for k in range(len(df_final)):
#     responses = []
#     violations = df_for_violations_request.loc[k].to_dict()
#     for i in range(len(violations["content"])):
#         violation = violations.copy()
#         violation["name"] = violations["name"][i]
#         violation["content"] = violations["content"][i]
#         violation["raw_content"] = violations["raw_content"][i]
#         violation["normative_articles"] = [violations["normative_articles"][i]]
#         json_data = json.dumps(violation, ensure_ascii=False).encode('utf8')
#         # Отправляем данные
#         response_violation = s.post('https://ma.gasoilcenter.ru/api/master-of-acts/violations/', headers={'Authorization': f"JWT {m}", 'Content-Type': 'application/json; charset=utf-8'}, data=json_data)
#         response_text_violation = json.loads(response_violation.text)
#         responses.append(response_text_violation)
#     #Получаем id и вставляем в акты для запроса
#     responses_id =[]
#     for i in responses:
#         responses_id.append(i["id"])
#     df_final["violations"].loc[k] = responses_id
#     for i, j in zip(df_final.content[k]["tbl_contents"], responses_id):
#         i["id"] = j
#     for i, j in zip(df_final.content[k]["tbl_contents"], responses_id):
#         i["id"] = j
#     data_final = df_final.loc[k].to_dict()
    
#     # Отправляем данные
#     response = s.post('https://ma.gasoilcenter.ru/api/master-of-acts/acts/', headers={'Authorization': f"JWT {m}"}, json=data_final)
#     response_text = json.loads(response.text)
#     id_response = response_text["id"]
#     # Создаем новый словарь, исключая ненужные ключи
#     filtered_response_text = {key: value for key, value in response_text.items() \
#                               if key not in ["id", "name", "creator", "created_at", "qrcode_web", "qr_phone", "environment_state", "field_contractor"]}
#     # Добавляем необходимые ключи
#     filtered_response_text["content"]["city"] = filtered_response_text["raw_content"]["zaknamestep1"]["payload"]["city"]
#     filtered_response_text["content"]["zaknamestep1"] = filtered_response_text["raw_content"]["zaknamestep1"]["value"]
#     filtered_response_text["content"]["podrCompanystep1"] = filtered_response_text["raw_content"]["podrCompanystep1"]["value"]
#     filtered_response_text["content"]["field"] = filtered_response_text["raw_content"]["field"]["value"]
#     filtered_response_text["content"]["superPositionkem"] = filtered_response_text["raw_content"]["superFIO"]["payload"]["superPositionkem"]
#     filtered_response_text["content"]["superFIOkem"] = filtered_response_text["raw_content"]["superFIO"]["payload"]["superFIOkem"]
#     filtered_response_text["content"]["superCompany"] = filtered_response_text["raw_content"]["superFIO"]["payload"]["superCompany"]
#     response1 = s.patch(f"https://ma.gasoilcenter.ru/api/master-of-acts/acts/{id_response}/", headers={'Authorization':f"JWT {m}"}, json=filtered_response_text)


# In[ ]:





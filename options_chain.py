import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import time
from datetime import date, datetime, timedelta
import pandas as pd
import os
import multiprocessing
from multiprocessing import Manager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException, ElementNotInteractableException, ElementClickInterceptedException



def callback(result):
    print(result)


def download_available_contract_months_job(date, path):
    url = f'https://7desl.com/hkex/data/{date}/hsi-options-months.csv'
    headers = {'Referer':'https://7desl.com/hkex', 'Dnt': '1'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        fn = f'hsi_options_months_{date}.csv'
        filepath = os.path.join(path, fn)
        with open(filepath, 'wb') as file:
            file.write(response.content)

        contract_months = []
        with open(filepath, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                contract_months.append(row[0])

        return date, contract_months

    else:
        print(f'Either {date} is a public holiday and market is closed OR data from {date} is not available.')
        contract_months = []
        return date, contract_months


def get_available_contract_months(from_days_ago, to_days_ago):
    dates = [(datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(to_days_ago, from_days_ago + 1)[::-1] if (datetime.today() - timedelta(days=i)).weekday() not in [5,6]]
    path = 'contract months'
    os.makedirs(path, exist_ok=True)
    filtered_dates = []
    cm_list = []
    for date in dates:
        d, cm = download_available_contract_months_job(date, path)
        if cm != []:
            filtered_dates.append(d)
            cm_list.append(cm)
        time.sleep(0.01)

    return filtered_dates, cm_list


def read_available_contract_months(path):
    files = os.listdir(path)
    dates = [f[-14:-4] for f in files if f[-4:] == '.csv' and f[-14:-4] not in os.listdir('data')]
    cm_list = []
    for f in files:
        filepath = os.path.join(path, f)
        contract_months = []
        with open(filepath, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                contract_months.append(row[0])
        cm_list.append(contract_months)

    return dates, cm_list


def download_result_csv(path, url, fn):
    headers = {'Referer':'https://7desl.com/hkex', 'Dnt': '1'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        filepath = os.path.join(path, fn)
        with open(filepath, 'wb') as file:
            file.write(response.content)
        result_text = f'{fn} successfully downloaded!'

    else:
        result_text = f'{fn} does not exists or connection failed.'

    time.sleep(0.2)
    return result_text


def file_checker():
    path = r'D:\Private\FX Trading\Trading Data\Futu\20240221\futu_hk_hsi_options_20240221.csv'
    df = pd.read_csv(path, encoding='utf-8', header=0)
    values = {'成交量': '0張'}
    df = df.fillna(value=values)
    col = df['成交量'].values.tolist()
    print(col)

    newcol = []
    for item in col:
        print(type(item))
        new = int(item[:-1])
        newcol.append(new)

    print(newcol)
    print(sum(newcol))


def get_optioncharts_data():
    # https://optioncharts.io/async/chain_chart_option_chain.csv?ticker=QQQ&amp;expiration_date=2024-03-08
    pass


def visualize_big_json(path=r'E:\Leads\ShopBack_BF\ShopBack_BF\data\Original\au.json'):
    with open(path, "r") as f:
        data = json.loads(f)







if __name__ == '__main__':

    sleep = 2
    today = datetime.today().strftime('%Y%m%d')
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
    core = 4
    # chrome_path = 'C:\\Users\\user\\Downloads\\chromedriver-win32\\chromedriver.exe'
    # rootdir = 'C:\\Users\\user\\Documents\\#Coding\\HKIEI\\'

    cm_path = 'contract months'
    # dates, cm_values = read_available_contract_months(cm_path)
    dates, cm_values = get_available_contract_months(1, 1)
    print(dates)

    pool = multiprocessing.Pool(processes=core)
    jobs = []
    for date, cm_list in zip(dates, cm_values):
        dpath = f'data/{date}'
        os.makedirs(dpath, exist_ok=True)

        urls = [f'https://7desl.com/hkex/data/{date}/data-hsi-oi.csv', f'https://7desl.com/hkex/data/{date}/data-hsi-futures.csv']
        filenames = [f'data_hsi_oi_{date}.csv', f'data_hsi_futures_{date}.csv']

        options_chain_urls = [f'https://7desl.com/hkex/data/{date}/hsi-options-months-{cm}.csv' for cm in cm_list]
        options_chain_filenames = [f'hsi_options_{cm}_{date}.csv' for cm in cm_list]
        urls.extend(options_chain_urls)
        filenames.extend(options_chain_filenames)

        for url, fn in zip(urls, filenames):
            job = pool.apply_async(download_result_csv, args=(dpath, url, fn), callback=callback)
            jobs.append(job)

    for job in jobs:
        job.get()

    jobs.clear()
    for date, cm_list in zip(dates, cm_values):
        dpath = f'data/{date}'
        os.makedirs(dpath, exist_ok=True)
        all_files = [f'data_hsi_futures_{date}.csv', f'data_hsi_oi_{date}.csv']
        options_chain_filenames = [f'hsi_options_{cm}_{date}.csv' for cm in cm_list]
        all_files.extend(options_chain_filenames)

        urls = [f'https://7desl.com/hkex/data/{date}/data-hsi-oi.csv', f'https://7desl.com/hkex/data/{date}/data-hsi-futures.csv']
        options_chain_urls = [f'https://7desl.com/hkex/data/{date}/hsi-options-months-{cm}.csv' for cm in cm_list]
        urls.extend(options_chain_urls)

        existing_files = os.listdir(dpath)
        if len(existing_files) != len(all_files):
            print(f'Some files are missing for date {date}...')
            for url, fn in zip(urls, all_files):
                if fn not in existing_files:
                    print(f'{fn} is not yet downloaded and retry downloading it now!')
                    result = download_result_csv(dpath, url, fn)
                    print(result)




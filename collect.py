# USAGE
# python collect.py --start 201901 --end 201902

from urllib.request import urlopen
from bs4 import BeautifulSoup
from tqdm import tqdm

import os
import time
import re
import requests
import argparse
import pandas as pd


ROOT_DIR = os.path.abspath(os.path.dirname(__file__))

DATASET_DIR = ROOT_DIR + '/dataset/'
API_URL = 'https://connpass.com/api/v1/event/'

SLEEPING_SECONDS = 5

# 出力フォーマットの定義
df_columns = ['event_id', 'title', 'catch', 'event_url', 'hash_tag', 'limit',
              'address', 'place', 'lat', 'lon', 'accepted', 'waiting',
              'started_at', 'ended_at', 'canceled', 'lottery', 'firstcome',
              'free', 'prepaid', 'postpaid', 'amount']


def main():
    # コマンドライン引数
    psr = argparse.ArgumentParser()
    psr.add_argument('-s', '--start', default=201901, type=int)
    psr.add_argument('-e', '--end', default=201905, type=int)
    args = psr.parse_args()

    # > python aaa.py -s 201111 -e 201905

    # 指定した期間のイベントデータを取得
    df_dataset = get_connpass_dataset(args.start, args.end)
    # csvとして保存
    df_dataset.to_csv(DATASET_DIR + 'dataset.csv')


def get_connpass_dataset(start_ym: int, end_ym: int) -> pd.DataFrame:
    """connpassイベント情報を指定した期間分取得する。

    Parameters
    ----------
    start_ym : int
            イベントの取得範囲（開始年月）。
    end_ym : int
            イベントの取得範囲（終了年月）。

    Returns
    -------
    df : DataFrame
            イベント情報。
    """

    df = pd.DataFrame(columns=df_columns)

    # 年月単位で処理
    for ym in tqdm(get_month_list(start_ym, end_ym)):

        # イベント情報を取得して連結
        df_ym = get_event_data_ym(ym)
        df = pd.concat([df, df_ym])

    return df


def get_event_info(ym: int, start: int = 1, count: int = 100) -> dict:
    """connpassAPIによりイベント情報を取得する。

    Parameters
    ----------
    ym : int
            取得するイベントの開催年月。
    start : int, default 1
            取得開始位置。
    count : int, default 100
            取得件数。

    Returns
    -------
    api_result : dict[str, str]
            イベント情報。
    """

    params = {
        'ym': ym,
        'start': start,
        'count': count,
    }
    return requests.get(API_URL, params=params).json()


def get_event_data(url: str) -> dict:
    """connpassイベントページより追加情報を取得する。

    Parameters
    ----------
    url : str
            connpassイベントのurl。

    Returns
    -------
    event_dict : dict[str, Any]
            イベント情報dict。
    """

    try:
        html = urlopen(url)

    except Exception:
        # アクセス失敗した場合には全てNoneで返す
        event_dict = {
            'canceled': None,
            'lottery': None,
            'firstcome': None,
            'free': None,
            'prepaid': None,
            'postpaid': None,
            'amount': None
        }
        return event_dict

    soup = BeautifulSoup(html, 'html.parser')
    canceled = 0
    cancel = soup.find(href=url + 'participation/#cancelled')
    if cancel is not None:
        canceled = cancel.text[9:-2]

    # 抽選 or 先着順（混在している場合には表示順上位の内容を優先）
    lottery = False
    firstcome = False
    free = False

    participant_decision_list = soup.find_all('p', class_='participants')
    for participant_decision in participant_decision_list:
        if '抽選' in participant_decision.text:
            lottery = True
            break
        elif '先着' in participant_decision.text:
            firstcome = True
            break

    # 抽選でも先着順でもないイベント
    free = not lottery and not firstcome

    # 会場払い or 前払い（混在している場合には表示順上位の内容を優先）
    prepaid = False
    postpaid = False
    # 金額（表示順上位・有料を優先）
    amount = 0

    payment_list = soup.find_all('p', class_='join_fee')
    for payment in payment_list:
        payment_text = payment.text
        if '（前払い）' in payment_text:
            prepaid = True
            amount = re.sub(r'\D', '', payment_text)
            break
        elif '（会場払い）' in payment_text:
            postpaid = True
            amount = re.sub(r'\D', '', payment_text)
            break

    event_dict = {
        'canceled': canceled,
        'lottery': lottery,
        'firstcome': firstcome,
        'free': free,
        'prepaid': prepaid,
        'postpaid': postpaid,
        'amount': amount
    }
    return event_dict


def get_event_data_ym(ym: int, seve_csv: bool = False) -> pd.DataFrame:
    """指定年月のconnpassイベント情報をDataFrameとして返す。

    Parameters
    ----------
    ym : int
            取得するイベントの開催年月。
    save_csv : bool, default False
            取得した情報を保存するか。

    Returns
    -------
    df : DataFrame
            指定年月のconnpassイベント情報。
    """

    df = pd.DataFrame(columns=df_columns)

    # イベント件数
    count = get_event_info(ym, 1, 1)['results_available']

    for i in range((count // 100) + 1):
        # イベント情報取得
        events = get_event_info(ym, (i * 100) + 1)['events']
        time.sleep(SLEEPING_SECONDS)

        for event in events:
            # connpassで受け付けているイベントのみを対象とする
            if event['event_type'] == 'participation':
                # キャンセル数や決済方法を取得
                scraped_dict = get_event_data(event['event_url'])
                time.sleep(SLEEPING_SECONDS)

                # api とスクレイピング結果を結合
                se = pd.Series({**event, **scraped_dict}, index=df.columns)
                df = df.append(se, ignore_index=True)

    if seve_csv:
        # 途中経過をcsv保存
        df.to_csv(DATASET_DIR + 'dataset_temp.csv', mode='a')

    return df


def get_month_list(start_ym: int, end_ym: int) -> list:
    """connpassAPIによりイベント情報を取得する。

    Parameters
    ----------
    start_ym : int
            開始年月。
    end_ym : int
            終了年月。

    Returns
    -------
    month_list : list[int]
            年月リスト。
    """

    month_list = []
    cal = start_ym

    while(cal <= end_ym):
        month_list.append(cal)
        if cal % 100 == 12:
            cal = cal + 100 - 11
        else:
            cal += 1
    return month_list


if __name__ == '__main__':
    main()

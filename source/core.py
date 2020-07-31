from source.settings import TOKEN
import source.players as players
import source.matches as matches

import time
from datetime import datetime, timedelta

import pandas as pd


def init_players_get_entries():

    path = input("folder path: ")
    players.get_entries(path, TOKEN)


def init_players_get_account_info():

    id_list = pd.read_pickle(
        "raw_data/players_pool.pkl")["summonerId"].to_list()

    print(f"size of id_list: {len(id_list)}")

    path = input("folder path: ")

    start = int(input("start index: "))
    end = int(input("end index: "))

    players.get_account_info(id_list, TOKEN, path, start, end)


def init_players_merge_with():
    players_pool = pd.read_pickle("raw_data/players_pool.pkl")
    account_info = pd.read_pickle("raw_data/account_info.pkl")

    path = input("folder path: ")

    players.merge_with(players_pool, account_info, path)


def init_players_get_match_history():

    players_acc_id = pd.read_pickle(
        "raw_data/players_pool_account.pkl")["accountId"].to_list()
    print(f"size of players_id_list: {len(players_acc_id)}")

    days_ago = int(input("How many days ago: "))

    days_ago = datetime.now() - timedelta(days=days_ago)
    days_ago = round(time.mktime(days_ago.timetuple()) * 1000)

    path = input("folder path: ")

    start = int(input("start index: "))
    end = int(input("end index: "))

    players.get_match_history(players_acc_id, TOKEN,
                              path, start, end, begin_time=days_ago)


def init_get_matches_data():
    id_list = pd.read_pickle(f"raw_data/matches_id.pkl")["gameId"].to_list()
    print(f"Number of entries on the list: {len(id_list)}\n")
    path = input("folder path: ")

    start = int(input("start index: "))
    end = int(input("end index: "))

    matches.get_matches(id_list, path, TOKEN, start, end)

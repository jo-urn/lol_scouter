import time

from itertools import chain

import requests as req
import pandas as pd
import numpy as np


def get_entries(path: str, token: str, master_leagues=True, student_leagues=True, queue="RANKED_SOLO_5x5", tiers={"DIAMOND": "I"}):
    # TODO check for cases where only one league is specified.
    n_requests = 0
    # getting the entries on master leagues
    if master_leagues:
        master_players = []
        leagues = [
            "challengerleagues",
            "grandmasterleagues",
            "masterleagues"
        ]
        base_url = "https://euw1.api.riotgames.com/lol/league/v4"

        for league in leagues:
            # control the number of requests
            if n_requests >= 100:
                print(f"Number of Entries so far: {len(student_players)}")
                n_requests = 0
                time.sleep(121)

            url = f"{base_url}/{league}/by-queue/{queue}?api_key={token}"
            res = req.get(url)
            n_requests += 1
            if res.status_code == 200:
                master_players.append(res.json())
            else:
                print(
                    f"Something went wrong at master leagues: {res.status_code}")
                break

         # attaching the tier to each entry
        for x in master_players:
            for y in x["entries"]:
                y["tier"] = x["tier"]

        # flatten all leagues into one list because we already specified the league on each entry
        master_players = list(chain.from_iterable(
            [x["entries"] for x in master_players]))

    # getting entries on student leagues
    if student_leagues:
        base_url = "https://euw1.api.riotgames.com/lol/league/v4/entries"
        student_players = []
        for tier in tiers:
            for division in tiers[tier]:
                pages = 0
                while True:
                    # control the number of requests
                    if n_requests >= 100:
                        print(
                            f"Number of Entries so far: {len(student_players)}")
                        n_requests = 0
                        time.sleep(121)
                    pages += 1
                    url = f"{base_url}/{queue}/{tier}/{division}?page={pages}&api_key={token}"
                    res = req.get(url)
                    n_requests += 1

                    if res.status_code == 200:
                        if len(res.json()) > 0:
                            student_players.append(res.json())
                        else:
                            break
                    else:
                        print(
                            f"Something went wrong at student leagues: {res.status_code}")
                        break

        # flatten student leagues players list
        student_players = list(chain.from_iterable(student_players))

        # turn to data frame and drop columns
        student_players = pd.DataFrame(student_players).drop(
            columns=["leagueId", "miniSeries", "queueType"])

        # concatenate both leagues, drop duplicates, drop useless columns
        players_df = pd.concat([student_players, pd.DataFrame(master_players)])
        players_df = players_df.reset_index().drop(columns="index")
        players_df = players_df.drop_duplicates()
        players_df = players_df.drop(
            columns=["veteran", "inactive", "freshBlood", "hotStreak"])

        # export to pickle
        players_df.to_pickle(f"{path}/players_pool.pkl", protocol=4)

    return print(f"File saved at {path}/players_pool.pkl")


def get_account_info(id_list: list, token: str, path: str, start=0, end=0) -> pd.DataFrame:
    container = []
    base_url = "https://euw1.api.riotgames.com/lol/summoner/v4/summoners"
    n_requests = 0

    # request account info for each summoner_id
    for i in range(start, end):

        while True:
            # control the number of requests
            if n_requests >= 100:
                print(f"Number of Entries so far: {len(container)}")
                n_requests = 0
                time.sleep(121)

            # make request
            url = f"{base_url}/{id_list[i]}?api_key={token}"
            res = req.get(url)

            # update the number of requests
            n_requests += 1

            # check for errors
            if res.status_code == 200:
                container.append(res.json())
                break

            # handle error
            else:
                print(
                    f"Skipping {i}, {res.json()['status']['message']}")
                break

        # make a partial backup
        if i != 0 and i % 100 == 0:
            fragment = pd.DataFrame(container).rename(
                columns={"id": "summonerId"})
            fragment = fragment[["summonerId", "accountId"]]
            fragment.to_pickle(
                f"{path}/fragmented_data/account_info_{start}-{i}.pkl", protocol=4)
            print(
                f"Partial File saved at {path}/fragmented_data/account_info_{start}-{i}.pkl")
    print(f"Done, number of elements: {len(container)}")

    # return useful data
    container = pd.DataFrame(container).rename(columns={"id": "summonerId"})
    container = container[["summonerId", "accountId"]]
    container.to_pickle(f"{path}/account_info.pkl", protocol=4)
    print(f"File saved at {path}/account_info.pkl")


def merge_with(players_pool: pd.DataFrame, container: pd.DataFrame, path: str):
    players_pool = players_pool.merge(container, on="summonerId")
    players_pool.to_pickle(f"{path}/players_pool_account.pkl", protocol=4)
    print(f"File saved at {path}/players_pool_account.pkl")


def get_match_history(players_list: list, token: str, path: str, start=0, end=0, begin_time=1593475200000, queue_id=420):

    container = []
    base_url = f"https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account"
    n_requests = 0

    for i in range(start, end):

        # reset begin index
        begin_index = 0

        # append a new list for each player
        # to keep count of number of entries.
        container.append([])

        while True:
            url = f"{base_url}/{players_list[i]}?queue={queue_id}&api_key={token}&beginTime={begin_time}&beginIndex={begin_index}"

            # control the number of requests
            if n_requests >= 100:
                print(f"Number of Entries so far: {len(container)}")
                n_requests = 0
                time.sleep(121)

            # make request
            res = req.get(url)
            # update number of requests
            n_requests += 1

            # check for errors
            if res.status_code == 200:

                # check if response is not empty
                if len(res.json()["matches"]) > 0:
                    container[len(container) - 1].append(res.json()["matches"])

                    # adjust begin index
                    begin_index += 100
                else:
                    break

            # handle error
            else:
                print(
                    f"Skipping {i} at page {round(begin_index/100)}, {res.json()['status']['message']}")
                break

        # make a partial backup
        if i != 0 and i % 100 == 0:
            fragment = list(chain.from_iterable(
                list(chain.from_iterable(container))))
            fragment = pd.DataFrame(fragment)[["gameId", "timestamp"]]
            fragment.to_pickle(
                f"{path}/fragmented_data/match_ids_{start}-{i}.pkl", protocol=4)
            print(
                f"Partial File saved at {path}/fragmented_data/match_ids_{start}-{i}.pkl")

    print(f"Done, number of elements: {len(container)}")

    # process container and save
    container = list(chain.from_iterable(list(chain.from_iterable(container))))
    container = pd.DataFrame(container)[["gameId", "timestamp"]]
    container.to_pickle(f"{path}/match_ids.pkl", protocol=4)

    print(f"File saved at {path}/match_ids.pkl")

import time

from itertools import chain

import requests as req
import pandas as pd
import numpy as np


# Create a dataframe, and assign the right values to lane variable
def adjust_lane_names(from_list: list, combat=False) -> pd.DataFrame:
    df = pd.DataFrame(from_list)

    support = df.loc[df["role"] == "DUO_SUPPORT"].index
    middle = df.loc[df["role"] == "DUO"].index
    df.loc[df.index[support], 'lane'] = "SUPPORT"
    df.loc[df.index[middle], 'lane'] = "MIDDLE"

    if combat:
        df = (df.assign(
            first_blood=np.where(df["first_blood"], 1, 0),
            first_blood_assist=np.where(df["first_blood_assist"], 1, 0)))

    df = df.drop(columns="role")

    return df


def update_champions_info() -> dict:
    champions = req.get(
        "https://ddragon.leagueoflegends.com/cdn/10.14.1/data/en_US/champion.json").json()
    champion_keys = champions["data"].keys()
    champions_list = {}

    for x in champions["data"]:
        champions_list[int(champions["data"][x]["key"])
                       ] = champions["data"][x]["name"]

    return champions_list


def extract_match_info(raw_matches: list, path: str, start: int, end: int):

    # Basic match info
    match_keys = ["gameId", "platformId",
                  "gameCreation", "gameDuration", "gameVersion"]
    matches_list = [{key: match[key]
                     for key in match if key in match_keys} for match in raw_matches]

    # Winners
    winners = [team["teamId"]
               for x in raw_matches for team in x["teams"] if team["win"] == "Win"]

    # Merge the lists
    matches_df = pd.DataFrame(matches_list)
    matches_df = matches_df.assign(winner=winners)
    matches_df["winner"] = matches_df["winner"].replace(
        {100: "Blue", 200: "Red"})

    # Change gameCreation to DateTime format
    matches_df["gameCreation"] = pd.to_datetime(
        matches_df["gameCreation"], unit="ms").dt.to_period("D")

    # Change column names to a better format
    matches_df.rename(columns={
        "gameId": "match_id",
        "platformId": "region",
        "gameCreation": "date_created",
        "gameDuration": "match_duration",
        "gameVersion": "patch"
    }, inplace=True)

    # Change patch format to a double digit e.g. 10.12
    matches_df["patch"] = matches_df["patch"].str.slice(stop=5)

    return matches_df.to_pickle(f"{path}/match_info_{start}-{end}.pkl", protocol=4)


def extract_champions_data(raw_matches: list, champions_list: list, path: str, start: int, end: int):

    # Get banned champions for each match
    bans = []
    for i, x in enumerate(raw_matches):
        for y in x["teams"]:
            for z in y["bans"]:
                try:
                    bans.append({"champion": z["championId"],
                                 "match_id": x["gameId"],
                                 "banned": 1})
                except:
                    continue
    bans = pd.DataFrame(bans)

    # Get picked champions for each match
    picks = []
    for x in raw_matches:
        for i, y in enumerate(x["participants"]):
            try:
                picks.append({
                    "champion": y["championId"],
                    "match_id": x["gameId"],
                    "region": x["platformId"],
                    "picked": 1,
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "opponent": ([x["championId"]
                                  for x in x["participants"][5:]]
                                 if i < 5
                                 else [x["championId"]
                                       for x in x["participants"][:5]]),
                    "won": y["stats"]["win"],
                    "lost": not y["stats"]["win"],
                })
            except:
                continue

    picks = pd.DataFrame(picks)

    # Replace True, False with 1, 0 respectively
    picks = picks.assign(won=np.where(
        picks["won"], 1, 0), lost=np.where(picks["lost"], 1, 0))

    # Now we need to explode the dataframe to get each individual matchup
    picks = picks.explode(column="opponent").reset_index()
    picks.drop(columns="index", inplace=True)

    picks = adjust_lane_names(picks)

    # export bans and picks
    bans.to_pickle(f"{path}/champion_bans_{start}-{end}.pkl", protocol=4)
    picks.to_pickle(f"{path}/champion_picks_{start}-{end}.pkl", protocol=4)


def extract_players_info(raw_matches: list, champions_list: list, path: str, start: int, end: int):

    # Getting the account info, champion used and role
    participants = []
    for x in raw_matches:
        for i, y in enumerate(x["participants"]):

            try:
                participants.append({
                    "account_id": x["participantIdentities"][i]["player"]["accountId"],
                    "summoner_id": x["participantIdentities"][i]["player"]["summonerId"],
                    "region": x["participantIdentities"][i]["player"]["currentPlatformId"],
                    "name": x["participantIdentities"][i]["player"]["summonerName"],
                    "champion": y["championId"],
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "won": 1 if y["stats"]["win"] == True else 0,
                })
            except:
                continue
    # Create a dataframe, and assign the right values to lane variable
    participants_df = adjust_lane_names(participants)

    # Now lets create frequency colums for each lane per account_id as a new table
    players_lane = participants_df[["account_id", "lane", "won"]]

    # Do the same for champions
    # Creating the DataFrame
    players_champions = participants_df[["account_id", "champion", "won"]]

    # Isolate the participants info
    participants_df = participants_df.drop(columns=["lane", "champion", "won"])
    participants_df = participants_df.drop_duplicates()

    # export
    players_lane.to_pickle(
        f"{path}/players_lanes_{start}-{end}.pkl", protocol=4)
    players_champions.to_pickle(
        f"{path}/players_champions_{start}-{end}.pkl", protocol=4)
    participants_df.to_pickle(
        f"{path}/players_info_{start}-{end}.pkl", protocol=4)


def extract_players_stats(raw_matches: list, champions_list: list, path: str, start: int, end: int):

    # Containers
    laning = []
    combat = []
    flair = []
    objectives = []

    for x in raw_matches:
        for i, y in enumerate(x["participants"]):

            found = True

            try:
                # laning
                laning.append({
                    "match_id": x["gameId"],
                    "account_id": x["participantIdentities"][i]["player"]["accountId"],
                    "region": x["participantIdentities"][i]["player"]["currentPlatformId"],
                    "champion": y["championId"],
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "xppm_10": y["timeline"]["xpPerMinDeltas"]["0-10"],
                    "cspm_10": y["timeline"]["creepsPerMinDeltas"]["0-10"],
                    "goldpm_10": y["timeline"]["goldPerMinDeltas"]["0-10"],
                    "dmg_takenpm_10": y["timeline"]["damageTakenPerMinDeltas"]["0-10"],
                    "won": 1 if y["stats"]["win"] == True else 0,
                })

                # combat
                combat.append({
                    "match_id": x["gameId"],
                    "account_id": x["participantIdentities"][i]["player"]["accountId"],
                    "region": x["participantIdentities"][i]["player"]["currentPlatformId"],
                    "champion": y["championId"],
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "dmg_total": y["stats"]["totalDamageDealtToChampions"],
                    "healing_total": y["stats"]["totalHeal"],
                    "units_healed": y["stats"]["totalUnitsHealed"],
                    "damage_mitigated": y["stats"]["damageSelfMitigated"],
                    "crowd_control": y["stats"]["totalTimeCrowdControlDealt"],
                    "dmg_taken": y["stats"]["totalDamageTaken"],
                    "first_blood": y["stats"]["firstBloodKill"] if found else None,
                    "first_blood_assist": y["stats"]["firstBloodAssist"],
                    "won": 1 if y["stats"]["win"] == True else 0,
                })

                # flair
                flair.append({
                    "match_id": x["gameId"],
                    "account_id": x["participantIdentities"][i]["player"]["accountId"],
                    "region": x["participantIdentities"][i]["player"]["currentPlatformId"],
                    "champion": y["championId"],
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "killing_sprees": y["stats"]["killingSprees"],
                    "longest_time_alive": y["stats"]["longestTimeSpentLiving"],
                    "double_kills": y["stats"]["doubleKills"],
                    "triple_kills": y["stats"]["tripleKills"],
                    "quadra_kills": y["stats"]["quadraKills"],
                    "penta_kills": y["stats"]["pentaKills"],
                    "won": 1 if y["stats"]["win"] == True else 0,
                })

                # objectives
                objectives.append({
                    "match_id": x["gameId"],
                    "account_id": x["participantIdentities"][i]["player"]["accountId"],
                    "region": x["participantIdentities"][i]["player"]["currentPlatformId"],
                    "champion": y["championId"],
                    "lane": y["timeline"]["lane"],
                    "role": y["timeline"]["role"],
                    "dmg_to_objectives": y["stats"]["damageDealtToObjectives"],
                    "dmg_to_turrets": y["stats"]["damageDealtToTurrets"],
                    "total_cs": y["stats"]["totalMinionsKilled"],
                    "jungle_cs": y["stats"]["neutralMinionsKilled"],
                    "jungle_invaded": y["stats"]["neutralMinionsKilledEnemyJungle"],
                    "wards_placed": y["stats"]["wardsPlaced"],
                    "wards_killed": y["stats"]["wardsKilled"],
                    "won": 1 if y["stats"]["win"] == True else 0,
                })
            except:
                continue

    # Creating and processing the dataframe
    laning = adjust_lane_names(laning)
    combat = adjust_lane_names(combat, True)
    flair = adjust_lane_names(flair)
    objectives = adjust_lane_names(objectives)

    # save files
    laning.to_pickle(
        f"{path}/player_laning_stats_{start}-{end}.pkl", protocol=4)
    combat.to_pickle(
        f"{path}/player_combat_stats_{start}-{end}.pkl", protocol=4)
    flair.to_pickle(f"{path}/player_flair_stats_{start}-{end}.pkl", protocol=4)
    objectives.to_pickle(
        f"{path}/player_objective_stats_{start}-{end}.pkl", protocol=4)


def get_matches(id_list: list, path: str, token: str, start: int, end: int):

    n_requests = 0

    # update champions information
    champions_list = update_champions_info()
    raw_matches = []

    for i, id in enumerate(id_list[start:end]):
        # control the number of requests
        if n_requests >= 100:
            print(f"Number of Matches so far: {len(raw_matches)}")
            n_requests = 0
            time.sleep(121)

        res = req.get(
            f"https://euw1.api.riotgames.com/lol/match/v4/matches/{id}?api_key={token}")
        n_requests += 1
        if res.status_code == 200:
            raw_matches.append(res.json())
        else:
            print(
                f"Skipping match at index {i}, {res.json()['status']['message']}")
            continue

        # store backups
        if i != 0 and i % 1000 == 0:
            extract_match_info(
                raw_matches, f"{path}/backups", start, i+start)
            extract_champions_data(
                raw_matches, champions_list, f"{path}/backups", start, i+start)
            extract_players_info(raw_matches, champions_list,
                                 f"{path}/backups", start, i+start)
            extract_players_stats(
                raw_matches, champions_list, f"{path}/backups", start, i+start)

    # process data and save files
    extract_match_info(raw_matches, path, start, end)
    extract_champions_data(raw_matches, champions_list, path, start, end)
    extract_players_info(raw_matches, champions_list, path, start, end)
    extract_players_stats(raw_matches, champions_list, path, start, end)

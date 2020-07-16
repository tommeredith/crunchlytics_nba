import pandas as pd
import numpy as np
import argparse
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

home_coefs = {
    "home_ortg_coef": 1.3,
    "home_efg_coef": 1.2,
    "home_orb_coef": 1,
    "home_tov_coef": 0.9,
    "home_pace_coef": 1
}

away_coefs = {
    "away_ortg_coef": 1.3,
    "away_efg_coef": 1.2,
    "away_orb_coef": 1,
    "away_tov_coef": 1.2,
    "away_pace_coef": 1
}


def fetch_season_game_log():
    conn = psycopg2.connect("host='all-them-stats.chure6gtnama.us-east-1.rds.amazonaws.com' port='5432' "
                            "dbname='stats_data' user='bundesstats' password='bundesstats'")
    select_game_log_statement = 'select * from full_game_log_nba'
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(select_game_log_statement)
    game_log = pd.DataFrame(data=cursor.fetchall())
    conn.commit()
    conn.close()

    return game_log


def build_game(home_team, away_team, teams_dict, index):
    df = pd.DataFrame(
        columns=["home_team_name", "home_team_id", "home_team_score", "away_team_name", "away_team_id",
                 "away_team_score", "game_url", "date", "id", "home_efg", "home_tov", "home_orb", "home_ortg",
                 "home_pace", "away_efg", "away_tov", "away_orb", "away_ortg", "away_pace"]
    )
    home_team_name = teams_dict.loc[teams_dict['team_id'] == home_team, 'team_name'].iloc[0]
    away_team_name = teams_dict.loc[teams_dict['team_id'] == away_team, 'team_name'].iloc[0]

    df.loc[index] = {"home_team_name": home_team_name, "home_team_id": home_team, "home_team_score": None, "away_team_name": away_team_name, "away_team_id": away_team,
                 "away_team_score": None, "game_url": None, "date": None, "id": None, "home_efg": None, "home_tov": None, "home_orb": None, "home_ortg": None,
                 "home_pace": None, "away_efg": None, "away_tov": None, "away_orb": None, "away_ortg": None, "away_pace": None}
    return df


def run_predictions(game_stats, date, manual_home_team, manual_away_team, teams_dict, is_neutral_site):
    sims_to_run = 100000
    df = pd.DataFrame(
        columns=["home_team", "away_team", "home_wins", "away_wins", "home_score", "away_score"])
    # only use games before the game week we want to predict
    historical = game_stats.loc[game_stats["date"] < str(date)]
    # make sure we only use games that have valid scores
    historical = historical.loc[game_stats["home_team_score"] > -1]
    # games to predict
    if is_neutral_site and manual_away_team and manual_home_team:
        first_iteration = build_game(manual_home_team, manual_away_team, teams_dict, 1)
        second_iteration = build_game(manual_away_team, manual_home_team, teams_dict, 2)
        data_frames = [first_iteration, second_iteration]
        to_predict = pd.concat(data_frames)
    elif manual_away_team and manual_home_team:
        to_predict = build_game(manual_home_team, manual_away_team, teams_dict, 1)
    elif is_neutral_site:
        games_on_date = game_stats.loc[game_stats["date"] == str(date)]
        to_predict = pd.DataFrame(columns=["home_team_name", "home_team_id", "home_team_score", "away_team_name", "away_team_id",
                 "away_team_score", "game_url", "date", "id", "home_efg", "home_tov", "home_orb", "home_ortg",
                 "home_pace", "away_efg", "away_tov", "away_orb", "away_ortg", "away_pace"])
        for i in games_on_date.index:
            home_team = games_on_date.loc[i, 'home_team_id']
            away_team = games_on_date.loc[i, 'away_team_id']
            switched_game = build_game(away_team, home_team, teams_dict, 1)
            to_predict = to_predict.append(games_on_date.loc[i], ignore_index=True)
            to_predict = to_predict.append(switched_game.loc[1], ignore_index=True)

    else:
        to_predict = game_stats.loc[game_stats["date"] == str(date)]
    home_avg_ortg = historical['home_ortg'].mean() * home_coefs["home_ortg_coef"]
    home_avg_tov = historical['home_tov'].mean() * home_coefs["home_tov_coef"]
    home_avg_orb = historical['home_orb'].mean() * home_coefs["home_orb_coef"]
    home_avg_efg = historical['home_efg'].mean() * home_coefs["home_efg_coef"]
    home_avg_pace = historical['home_pace'].mean() * home_coefs["home_pace_coef"]
    away_avg_ortg = historical['away_ortg'].mean() * away_coefs["away_ortg_coef"]
    away_avg_tov = historical['away_tov'].mean() * away_coefs["away_tov_coef"]
    away_avg_orb = historical['away_orb'].mean() * away_coefs["away_orb_coef"]
    away_avg_efg = historical['away_efg'].mean() * away_coefs["away_efg_coef"]
    away_avg_pace = historical['away_pace'].mean() * away_coefs["away_pace_coef"]
    # get average home and away scores for entire competition
    home_avg = home_avg_ortg + home_avg_orb + home_avg_efg + home_avg_pace - home_avg_tov
    away_avg = away_avg_ortg + away_avg_orb + away_avg_efg + away_avg_pace - away_avg_tov

    # loop through predicting games
    another_i = 1
    for i in to_predict.index:
        home_team = to_predict.loc[i, "home_team_name"]
        away_team = to_predict.loc[i, "away_team_name"]
        home_team_id = to_predict.loc[i, "home_team_id"]
        away_team_id = to_predict.loc[i, "away_team_id"]

        # average goals scored and goals conceded for home team
        home_team_ortg_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_ortg"].mean() * \
                             home_coefs["home_ortg_coef"]
        home_team_ortg_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_ortg"].mean() * \
                                 away_coefs["away_ortg_coef"]
        home_team_orb_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_orb"].mean() * home_coefs[
            "home_orb_coef"]
        home_team_orb_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_orb"].mean() * \
                                away_coefs["away_orb_coef"]
        home_team_tov_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_tov"].mean() * home_coefs[
            "home_tov_coef"]
        home_team_tov_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_tov"].mean() * \
                                away_coefs["away_tov_coef"]
        home_team_efg_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_efg"].mean() * home_coefs[
            "home_efg_coef"]
        home_team_efg_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_efg"].mean() * \
                                away_coefs["away_efg_coef"]
        home_team_pace_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_pace"].mean() * \
                             home_coefs["home_pace_coef"]
        home_team_pace_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_pace"].mean() * \
                                 away_coefs["away_pace_coef"]

        home_team_offense = home_team_ortg_for + home_team_orb_for + home_team_efg_for + home_team_pace_for - home_team_tov_for
        home_team_defense = home_team_ortg_against + home_team_orb_against + home_team_efg_against + home_team_pace_against - home_team_tov_against

        # average goals scored and goals conceded for away team
        away_team_ortg_for = historical.loc[game_stats["away_team_id"] == away_team_id, "away_ortg"].mean() * \
                             away_coefs["away_ortg_coef"]
        away_team_ortg_against = historical.loc[
                                     game_stats["away_team_id"] == away_team_id, "home_ortg"].mean() * home_coefs[
                                     "home_ortg_coef"]
        away_team_orb_for = historical.loc[
                                game_stats["away_team_id"] == away_team_id, "away_orb"].mean() * away_coefs[
                                "away_orb_coef"]
        away_team_orb_against = historical.loc[
                                    game_stats["away_team_id"] == away_team_id, "home_orb"].mean() * home_coefs[
                                    "home_orb_coef"]
        away_team_tov_for = historical.loc[
                                game_stats["away_team_id"] == away_team_id, "away_tov"].mean() * away_coefs[
                                "away_tov_coef"]
        away_team_tov_against = historical.loc[
                                    game_stats["away_team_id"] == away_team_id, "home_tov"].mean() * home_coefs[
                                    "home_tov_coef"]
        away_team_efg_for = historical.loc[
                                game_stats["away_team_id"] == away_team_id, "away_efg"].mean() * away_coefs[
                                "away_efg_coef"]
        away_team_efg_against = historical.loc[
                                    game_stats["away_team_id"] == away_team_id, "home_efg"].mean() * home_coefs[
                                    "home_efg_coef"]
        away_team_pace_for = historical.loc[
                                 game_stats["away_team_id"] == away_team_id, "away_pace"].mean() * away_coefs[
                                 "away_pace_coef"]
        away_team_pace_against = historical.loc[
                                     game_stats["away_team_id"] == away_team_id, "home_pace"].mean() * home_coefs[
                                     "home_pace_coef"]

        away_team_offense = away_team_ortg_for + away_team_orb_for + away_team_efg_for + away_team_pace_for - away_team_tov_for
        away_team_defense = away_team_ortg_against + away_team_orb_against + away_team_efg_against + away_team_pace_against - away_team_tov_against

        # calculate home and away offense and defense strength
        home_team_offense_strength = home_team_offense / home_avg
        home_team_defense_strength = home_team_defense / away_avg

        away_team_offense_strength = away_team_offense / away_avg
        away_team_defense_strength = away_team_defense / home_avg

        home_team_expected_score = home_team_offense_strength * away_team_defense_strength * home_avg
        away_team_expected_score = away_team_offense_strength * home_team_defense_strength * away_avg

        home_team_poisson = np.random.poisson(home_team_expected_score, sims_to_run)
        away_team_poisson = np.random.poisson(away_team_expected_score, sims_to_run)

        home_wins = np.sum(home_team_poisson > away_team_poisson) / sims_to_run * 100
        away_wins = np.sum(away_team_poisson > home_team_poisson) / sims_to_run * 100

        home_score_actual = to_predict.loc[i, "home_team_score"] if to_predict.loc[i, "home_team_score"] else ""
        away_score_actual = to_predict.loc[i, "away_team_score"] if to_predict.loc[i, "away_team_score"] else ""

        df.loc[another_i] = {
            "home_team": home_team,
            "away_team": away_team,
            "home_wins": home_wins,
            "away_wins": away_wins,
            "home_score": home_score_actual,
            "away_score": away_score_actual
        }
        another_i = another_i + 1
    return df


def print_predictions(predictions_data, is_neutral_site):
    if is_neutral_site:
        for i in predictions_data.index:
            print('=============================')
            print('team one: ', predictions_data.loc[i, "team_one"])
            print('team two: ', predictions_data.loc[i, "team_two"])
            print('team one wins: ', predictions_data.loc[i, "team_one_wins"])
            print('team two wins: ', predictions_data.loc[i, "team_two_wins"])
            print('=============================')
    else:
        for i in predictions_data.index:
            print('=============================')
            print('home team: ', predictions_data.loc[i, "home_team"])
            print('away team: ', predictions_data.loc[i, "away_team"])
            print('home wins: ', predictions_data.loc[i, "home_wins"])
            print('away wins: ', predictions_data.loc[i, "away_wins"])
            print('home score: ', predictions_data.loc[i, "home_score"])
            print('away score: ', predictions_data.loc[i, "away_score"])
            print('=============================')


def run_tests(game_stats, date, manual_home_team, manual_away_team, teams_dict):
    print('running accuracy check')
    best_score = 0
    best_threshold = 0
    games_used = 0
    for threshold in range(45, 95, 5):
        correct = 0
        total_games = 0
        possible_games = 0
        games_totally_wrong = 0
        predict_index = game_stats.loc[game_stats["date"] == date].index[0]
        game_date = ""
        for index in range(90, predict_index):
            game = game_stats.iloc[index]
            if game["date"] == game_date:
                continue
            game_date = game["date"]
            predictions = run_predictions(game_stats, game_date, manual_home_team, manual_away_team, teams_dict, False)

            for i in predictions.index:
                home_score = predictions.loc[i, "home_score"]
                away_score = predictions.loc[i, "away_score"]
                home_win = predictions.loc[i, "home_wins"]
                away_win = predictions.loc[i, "away_wins"]
                if home_score > away_score and home_win > away_win and home_win > threshold:
                    correct += 1
                if away_score > home_score and away_win > home_win and away_win > threshold:
                    correct += 1
                if away_win > threshold or home_win > threshold:
                    total_games += 1

                if home_score > away_score and away_win > home_win and away_win > threshold:
                    games_totally_wrong += 1
                if away_score > home_score and home_win > away_win and home_win > threshold:
                    games_totally_wrong += 1

                possible_games += 1
        if total_games > 0:
            score = correct / total_games * 100
        else:
            score = 0
        print('----------')
        print('threshold: ', threshold)
        print('score: ', score)
        print('games used: ', total_games)
        print('games totally wrong', games_totally_wrong)
        print('----------')

        if (score > best_score or (
                score == best_score and total_games > games_used)) and total_games >= possible_games / 10:
            best_score = score
            best_threshold = threshold
            games_used = total_games

    print('')
    print('')
    print('!!!!!!=====================!!!!!!!!!!!')
    print('best threshold: ', best_threshold)
    print('score: ', best_score)
    print('games used: ', games_used)
    print('!!!!!!=====================!!!!!!!!!!!')


def combine_neutral_site_predictions(predictions):
    df = pd.DataFrame(
        columns=["team_one", "team_two", "team_one_wins", "team_two_wins"])
    another_index = 1
    for i in predictions.index:

        if i % 2 != 0:
            team_one = {
                "team_name": predictions.loc[i, "home_team"],
                "team_wins": (predictions.loc[i, "home_wins"] + predictions.loc[i + 1, "away_wins"]) / 2
            }

            team_two = {
                "team_name": predictions.loc[i + 1, "home_team"],
                "team_wins": (predictions.loc[i + 1, "home_wins"] + predictions.loc[i, "away_wins"]) / 2
            }

            df.loc[another_index] = {
                "team_one": team_one["team_name"],
                "team_two": team_two["team_name"],
                "team_one_wins": team_one["team_wins"],
                "team_two_wins": team_two["team_wins"]
            }
            another_index += 1

    return df


todaysdate = datetime.date.today().strftime("%Y-%m-%d")

# initialise parser object to read from command line
parser = argparse.ArgumentParser()

# script arguments
parser.add_argument('-r', '--run', action='store_true', help='run that shit')
parser.add_argument('-t', '--test', action='store_true', help='run tests to check accuracy')
parser.add_argument("-d", "--date", default=todaysdate, help="need a date lookin like YYYY-MM-DD, eg 2017-09-20")
parser.add_argument('-away', '--away_team', help='away team ID (e.g. BOS, NYK, LAL)')
parser.add_argument('-home', '--home_team', help='home team ID (e.g. LAC, IND, POR)')
parser.add_argument('-m', '--manual', action="store_true", help='flag for manual team input')
parser.add_argument('-n', '--neutral', action="store_true", help="flag for neutral site")
args = parser.parse_args()

run = args.run
test = args.test
date = args.date
manual = args.manual
manual_away_team = args.away_team
manual_home_team = args.home_team
is_neutral_site = args.neutral

game_stats = fetch_season_game_log()
teams_dict = pd.read_csv('teams.csv')
if manual:
    if is_neutral_site:
        predictions = run_predictions(game_stats, date, manual_home_team, manual_away_team, teams_dict, True)
        predictions = combine_neutral_site_predictions(predictions)
        print_predictions(predictions, True)
    else:
        predictions = run_predictions(game_stats, date, manual_home_team, manual_away_team, teams_dict, False)
        print_predictions(predictions, False)
elif run:
    if is_neutral_site:
        predictions = run_predictions(game_stats, date, None, None, teams_dict, True)
        predictions = combine_neutral_site_predictions(predictions)
        print_predictions(predictions, True)
    else:
        predictions = run_predictions(game_stats, date, None, None, teams_dict, False)
        print_predictions(predictions, False)
elif test:
    run_tests(game_stats, date, None, None, teams_dict)
else:
    print('need to add -r to that shit and add a week')

import pandas as pd
import numpy as np
import argparse
import datetime


def run_predictions(game_stats, date):
    sims_to_run = 100000
    df = pd.DataFrame(
        columns=["home_team", "away_team", "home_wins", "away_wins", "home_score", "away_score"])
    # only use games before the game week we want to predict
    historical = game_stats.loc[game_stats["date"] < str(date)]
    # make sure we only use games that have valid scores
    historical = historical.loc[game_stats["home_team_score"] > -1]

    # games to predict
    to_predict = game_stats.loc[game_stats["date"] == str(date)]

    # get average home and away scores for entire competition
    home_avg = historical["home_team_score"].mean()
    away_avg = historical["away_team_score"].mean()

    # loop through predicting games
    another_i = 1
    for i in to_predict.index:
        home_team = to_predict.loc[i, "home_team_name"]
        away_team = to_predict.loc[i, "away_team_name"]
        home_team_id = to_predict.loc[i, "home_team_id"]
        away_team_id = to_predict.loc[i, "away_team_id"]

        # average goals scored and goals conceded for home team
        home_team_average_for = historical.loc[game_stats["home_team_id"] == home_team_id, "home_team_score"].mean()
        home_team_average_against = historical.loc[game_stats["home_team_id"] == home_team_id, "away_team_score"].mean()

        # average goals scored and goals conceded for away team
        away_team_average_for = historical.loc[game_stats["away_team_id"] == away_team_id, "away_team_score"].mean()
        away_team_average_against = historical.loc[game_stats["away_team_id"] == away_team_id, "home_team_score"].mean()

        # calculate home and away offense and defense strength
        home_team_offense_strength = home_team_average_for / home_avg
        home_team_defense_strength = home_team_average_against / away_avg

        away_team_offense_strength = away_team_average_for / away_avg
        away_team_defense_strength = away_team_average_against / home_avg

        home_team_expected_score = home_team_offense_strength * away_team_defense_strength * home_avg
        away_team_expected_score = away_team_offense_strength * home_team_defense_strength * away_avg

        home_team_poisson = np.random.poisson(home_team_expected_score, sims_to_run)
        away_team_poisson = np.random.poisson(away_team_expected_score, sims_to_run)

        home_wins = np.sum(home_team_poisson > away_team_poisson) / sims_to_run * 100
        away_wins = np.sum(away_team_poisson > home_team_poisson) / sims_to_run * 100

        home_score_actual = to_predict.loc[i, "home_team_score"]
        away_score_actual = to_predict.loc[i, "away_team_score"]

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


def print_predictions(predictions_data):
    for i in predictions_data.index:
        print('=============================')
        print('home team: ', predictions_data.loc[i, "home_team"])
        print('away team: ', predictions_data.loc[i, "away_team"])
        print('home wins: ', predictions_data.loc[i, "home_wins"])
        print('away wins: ', predictions_data.loc[i, "away_wins"])
        print('home score: ', predictions_data.loc[i, "home_score"])
        print('away score: ', predictions_data.loc[i, "away_score"])
        print('=============================')


def run_tests(game_stats, date):
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
            predictions = run_predictions(game_stats, game_date)

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


todaysdate = datetime.date.today().strftime("%Y-%m-%d")

# initialise parser object to read from command line
parser = argparse.ArgumentParser()

# script arguments
parser.add_argument('-r', '--run', action='store_true', help='run that shit')
parser.add_argument('-t', '--test', action='store_true', help='run tests to check accuracy')
parser.add_argument("-d", "--date", default=todaysdate, help="need a date lookin like YYYY-MM-DD, eg 2017-09-20")


args = parser.parse_args()

run = args.run
test = args.test
date = args.date

game_stats = pd.read_csv('full_game_log_nba.csv')

if args.run:
    predictions = run_predictions(game_stats, date)
    print_predictions(predictions)
elif args.test:
    run_tests(game_stats, date)
else:
    print('need to add -r to that shit and add a week')

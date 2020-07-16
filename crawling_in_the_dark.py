import requests
from bs4 import BeautifulSoup
import csv
import datetime
from selenium import webdriver
import argparse
from stash_csv_in_db import stash_in_db, stash_standings_in_db


def parse_standings(team):
    team_obj = {}
    team_link = team.find('th', {'data-stat': 'team_name'}).find('a')
    team_id = team_link.get('href').split('/')[2]
    team_name = team_link.get_text()
    team_obj["team_id"] = team_id
    team_obj["team_name"] = team_name
    team_wins = team.find('td', {'data-stat': 'wins'}).get_text()
    team_losses = team.find('td', {'data-stat': 'losses'}).get_text()
    team_obj['wins'] = team_wins
    team_obj['losses'] = team_losses
    team_ppg = team.find('td', {'data-stat': 'pts_per_g'}).get_text()
    team_opp_ppg = team.find('td', {'data-stat': 'opp_pts_per_g'}).get_text()
    team_obj['ppg'] = team_ppg
    team_obj['opp_ppg'] = team_opp_ppg
    return team_obj

def get_league_standings(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    east_league_table = soup.find("table", {"id": "confs_standings_E"})
    west_league_table = soup.find("table", {"id": "confs_standings_W"})
    east_teams = east_league_table.tbody.find_all('tr')
    west_teams = west_league_table.tbody.find_all('tr')
    team_ids = []
    index = 1
    for team in east_teams:
        team_obj = parse_standings(team)
        team_obj["id"] = index
        index += 1
        team_ids.append(team_obj)
    for team in west_teams:
        team_obj = parse_standings(team)
        team_obj["id"] = index
        index += 1
        team_ids.append(team_obj)

    return team_ids



def get_games(page):
    match_table = page.find("table", {"class": "stats_table"})
    matches = match_table.tbody.find_all('tr')

    return matches


def get_game_months(page):
    months = page.find('div', {'class': 'filter'})
    month_urls = months.find_all('a')
    return month_urls


def get_game_score(game):
    game_dict = {}

    date = game.find('th', {'data-stat': 'date_game'}).find('a').get('href').split('?')[1]
    date_segs = date.split('&')
    year = 0
    month = 0
    day = 0
    for seg in date_segs:
        cat = seg.split('=')[0]
        if cat == 'year':
            year += int(seg.split('=')[1])
        if cat == 'month':
            month += int(seg.split('=')[1])
        if cat == 'day':
            day += int(seg.split('=')[1])

    formatted_date = datetime.datetime(year, month, day).date()
    print(formatted_date)
    home_link = game.find('td', {'data-stat': 'home_team_name'}).find('a')
    home_team_name = home_link.get_text()
    home_team_id = home_link.get('href').split('/')[2]
    home_team_score = game.find('td', {'data-stat': 'home_pts'}).get_text() if game.find('td', {'data-stat': 'home_pts'}).get_text() else ''
    away_link = game.find('td', {'data-stat': 'visitor_team_name'}).find('a')
    away_team_name = away_link.get_text()
    away_team_id = away_link.get('href').split('/')[2]
    away_team_score = game.find('td', {'data-stat': 'visitor_pts'}).get_text() if game.find('td', {'data-stat': 'visitor_pts'}).get_text() else ''
    game_url = game.find('td', {'data-stat': 'box_score_text'}).find('a').get('href') if game.find('td', {'data-stat': 'box_score_text'}).find('a') else ''
    game_dict["home_team_name"] = home_team_name
    game_dict["home_team_id"] = home_team_id
    game_dict["home_team_score"] = home_team_score
    game_dict["away_team_name"] = away_team_name
    game_dict["away_team_id"] = away_team_id
    game_dict["away_team_score"] = away_team_score
    game_dict["game_url"] = game_url
    game_dict["date"] = formatted_date

    return game_dict


def scrape_individual_game(browser, game_url, home_team_id, away_team_id):
    if game_url == '':
        home_stats = {"home_efg": '', "home_tov": '', "home_orb": '', "home_ortg": '', "home_pace": ''}
        away_stats = {"away_efg": '', "away_tov": '', "away_orb": '', "away_ortg": '', "away_pace": ''}

        return home_stats, away_stats
    else:
        browser.get("https://www.basketball-reference.com" + game_url)
        html = browser.page_source
        soup = BeautifulSoup(html, 'html.parser')
        four_factors_data = soup.find('table', id='four_factors')

        home_stats = {}
        away_stats = {}
        for row in four_factors_data.tbody.find_all('tr'):

            efg = row.find('td', {'data-stat': 'efg_pct'}).get_text()
            tov = row.find('td', {'data-stat': 'tov_pct'}).get_text()
            orb = row.find('td', {'data-stat': 'orb_pct'}).get_text()
            ortg = row.find('td', {'data-stat': 'off_rtg'}).get_text()
            pace = row.find('td', {'data-stat': 'pace'}).get_text()
            if row.find('th', {'data-stat': 'team_id'}).find('a').get_text() == home_team_id:
                prefix = 'home'
                home_stats.update({
                    prefix + '_efg': efg,
                    prefix + '_tov': tov,
                    prefix + '_orb': orb,
                    prefix + '_ortg': ortg,
                    prefix + '_pace': pace
                })
            if row.find('th', {'data-stat': 'team_id'}).find('a').get_text() == away_team_id:
                prefix = 'away'
                away_stats.update({
                    prefix + '_efg': efg,
                    prefix + '_tov': tov,
                    prefix + '_orb': orb,
                    prefix + '_ortg': ortg,
                    prefix + '_pace': pace
                })

        return home_stats, away_stats


def convert_game_info_to_csv(stats):
    print('============ CONVERTING match stats TO CSV ===============')
    print('')
    print('')
    keys = stats[0].keys()
    with open('full_game_log_nba.csv', 'w') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(stats)


def convert_team_ids_to_csv(team_ids):
    print('============ CONVERTING league table TO CSV ===============')
    print('')
    print('')
    keys = team_ids[0].keys()
    with open('teams.csv', 'w') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(team_ids)


def scrape_games_like_a_thug(games_url, standings_url):
    all_match_stats = []
    print('============== SCRAPING ==================')
    print('')
    print('')
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    chrome_path = r'/usr/local/bin/chromedriver'
    browser = webdriver.Chrome(chrome_path, options=options)
    page = requests.get(games_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    months = get_game_months(soup)
    team_ids = get_league_standings(standings_url)
    outside_index = 1
    for month in months:
        month_url = "https://www.basketball-reference.com" + month.get('href')
        page = requests.get(month_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        games = get_games(soup)

        for index in range(len(games)):
            game_info = get_game_score(games[index])
            game_info["id"] = outside_index
            outside_index += 1
            print('scraping: ', game_info["game_url"])
            print('')
            print('')
            home_stats, away_stats = scrape_individual_game(browser, game_info["game_url"], game_info["home_team_id"], game_info["away_team_id"])
            game_info.update(home_stats)
            game_info.update(away_stats)
            print(game_info)
            all_match_stats.append(game_info)
    browser.quit()
    print('total games: ', len(all_match_stats))
    print('============== DONE SCRAPING ==================')
    print('')
    convert_game_info_to_csv(all_match_stats)
    convert_team_ids_to_csv(team_ids)
    stash_in_db(all_match_stats, 'nba')
    stash_standings_in_db(team_ids, 'nba')
    print('=========== DONEZO ==============')


def just_the_standings(standings_url):
    print('============== SCRAPING STANDINGS ==================')
    print('')
    print('')
    team_ids = get_league_standings(standings_url)
    convert_team_ids_to_csv(team_ids)
    stash_standings_in_db(team_ids, 'nba')
    print('============== DONEZO ==================')

# initialise parser object to read from command line
parser = argparse.ArgumentParser()

# script arguments
parser.add_argument('-s', '--standings', action='store_true', help='just scrape standings')
parser.add_argument('-stash', '--stash', action='store_true', help='stash in db')
args = parser.parse_args()

games_url = 'https://www.basketball-reference.com/leagues/NBA_2020_games.html'
standings_url = 'https://www.basketball-reference.com/leagues/NBA_2020_standings.html'

if args.stash:
    with open("full_game_log_nba.csv", "r") as f:
        reader = csv.DictReader(f)
        game_log_list = list(reader)
        stash_in_db(game_log_list, 'nba')
elif args.standings:
    just_the_standings(standings_url)
else:
    scrape_games_like_a_thug(games_url, standings_url)


from selenium import webdriver
import requests
from bs4 import BeautifulSoup
from crawling_in_the_dark import get_game_score, get_games, get_game_months, get_team_ids
import csv

games_url = 'https://www.basketball-reference.com/leagues/NBA_2020_games.html'
standings_url = 'https://www.basketball-reference.com/leagues/NBA_2020_standings.html'


def convert_player_stats_to_csv(stats):
    print('============ CONVERTING player stats TO CSV ===============')
    print('')
    print('')
    keys = stats[0].keys()
    with open('full_player_stats.csv', 'w') as output:
        dict_writer = csv.DictWriter(output, keys)
        dict_writer.writeheader()
        dict_writer.writerows(stats)


def get_player_stats(browser, game_info):
    browser.get("https://www.basketball-reference.com" + game_info["game_url"])
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    tables = [
        {'table_id': 'box-' + game_info["away_team_id"] + '-game-basic', 'team_id': game_info["away_team_id"], 'opp_team_id': game_info["home_team_id"], "is_advanced": False},
        {'table_id': 'box-' + game_info["away_team_id"] + '-game-advanced', 'team_id': game_info["away_team_id"], 'opp_team_id': game_info["home_team_id"], "is_advanced": True},
        {'table_id': 'box-' + game_info["home_team_id"] + '-game-basic', 'team_id': game_info["home_team_id"], 'opp_team_id': game_info["away_team_id"],  "is_advanced": False},
        {'table_id': 'box-' + game_info["home_team_id"] + '-game-advanced', 'team_id': game_info["home_team_id"], 'opp_team_id': game_info["away_team_id"], "is_advanced": True}
    ]
    player_stats = []
    for table in tables:
        stat_table = soup.find('table', {'id': table["table_id"]})
        stat_table_rows = stat_table.tbody.find_all('tr')
        for player in stat_table_rows:
            if player.has_attr('class') and 'thead' in player['class'] or player.find('td', {'data-stat': 'reason'}):
                continue

            player_info_cell = player.find('th', {'data-stat': 'player'})
            player_name = player_info_cell.find('a').get_text()
            player_id = player_info_cell.get('data-append-csv')
            if table["is_advanced"] is True:
                player_usg = player.find('td', {'data-stat': 'usg_pct'}).get_text()
                player_ast_pct = player.find('td', {'data-stat': 'ast_pct'}).get_text()
                player_reb_pct = player.find('td', {'data-stat': 'trb_pct'}).get_text()
                for player_stat in player_stats:
                    if player_stat["player_id"] == player_id:
                        player_stat["usg"] = player_usg
                        player_stat["ast_pct"] = player_ast_pct
                        player_stat["reb_pct"] = player_reb_pct

            else:
                player_obj = {}
                player_obj["player_name"] = player_name
                player_obj["player_id"] = player_id
                player_fga = player.find('td', {'data-stat': 'fga'}).get_text()
                player_ast = player.find('td', {'data-stat': 'ast'}).get_text()
                player_reb = player.find('td', {'data-stat': 'trb'}).get_text()
                player_obj["fga"] = player_fga
                player_obj["ast"] = player_ast
                player_obj["reb"] = player_reb
                player_obj["opp_team_id"] = table["opp_team_id"]

                player_stats.append(player_obj)

    print('got the player stats')
    print('')
    print('')
    return player_stats


def start_it_up():
    print('============== LES DO IT  ==================')
    print('')
    print('')
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    chrome_path = r'/usr/local/bin/chromedriver'
    browser = webdriver.Chrome(chrome_path, options=options)
    page = requests.get(games_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    months = get_game_months(soup)
    team_ids = get_team_ids(standings_url)
    outside_index = 1
    all_player_stats = []
    for month in months:
        month_url = "https://www.basketball-reference.com" + month.get('href')
        page = requests.get(month_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        games = get_games(soup)
        for index in range(len(games)):
            game_info = get_game_score(games[index])
            print(game_info)
            game_info["id"] = outside_index
            print('scraping: ', game_info["game_url"])
            print('')
            print('')
            if game_info["game_url"] == '':
                continue
            game_player_stats = get_player_stats(browser, game_info)
            all_player_stats.extend(game_player_stats)
    browser.quit()

    convert_player_stats_to_csv(all_player_stats)
    print('=========== DONEZO ==============')


print('we dem boys')
start_it_up()

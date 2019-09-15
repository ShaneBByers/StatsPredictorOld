from WebConnector import WebConnector
from SalaryDataCollector import SalaryDataCollector
from selenium import webdriver


import logging
import time
import datetime
import urllib.request
import requests


class WebDataCollector:

    def __init__(self, logger_name):
        self.connector = WebConnector()
        self.logger = logging.getLogger(logger_name)
        self.salary_parser = SalaryDataCollector(logger_name)
        # logger.info("Web Data Collector created")

    def get_teams(self):
        self.connector.append_string("teams")
        self.logger.info("Get teams request: " + self.connector.request_string)
        json = self.connector.execute()
        return json['teams']

    def get_players(self, season_id):
        self.connector.append_string("teams?expand=team.roster&season=" + str(season_id))
        self.logger.info("Get team rosters request: " + self.connector.request_string)
        team_rosters = self.connector.execute()
        teams = team_rosters['teams']
        players = []
        for team in teams:
            roster = team['roster']['roster']
            for person in roster:
                position = person['position']['code']
                if position != 'G':
                    player_id = person['person']['id']
                    found = False
                    for player in players:
                        if player['id'] == player_id:
                            found = True
                            self.logger.warning("Duplicate player ID: " + str(player_id))
                    if not found:
                        self.connector.append_string("people/" + str(player_id))
                        self.logger.info("Get player request: " + self.connector.request_string)
                        player = self.connector.execute()
                        players.append(player['people'][0])
        return players

    def get_goalies(self, season_id):
        self.connector.append_string("teams?expand=team.roster&season=" + str(season_id))
        self.logger.info("Get team rosters request: " + self.connector.request_string)
        team_rosters = self.connector.execute()
        teams = team_rosters['teams']
        players = []
        for team in teams:
            roster = team['roster']['roster']
            for person in roster:
                position = person['position']['code']
                if position == 'G':
                    player_id = person['person']['id']
                    found = False
                    for player in players:
                        if player['id'] == player_id:
                            found = True
                            self.logger.warning("Duplicate player ID: " + str(player_id))
                    if not found:
                        self.connector.append_string("people/" + str(player_id))
                        self.logger.info("Get player request: " + self.connector.request_string)
                        player = self.connector.execute()
                        players.append(player['people'][0])
        return players

    def get_games(self, start_date, end_date):
        self.connector.append_string("schedule?startDate=" + str(start_date) + "&endDate=" + str(end_date))
        self.logger.info("Get game schedule request: " + self.connector.request_string)
        schedule = self.connector.execute()
        dates = schedule['dates']
        games = []
        for date in dates:
            web_games = date['games']
            for web_game in web_games:
                if web_game['gameType'] == 'R':
                    games.append(web_game)

        return games

    def get_game(self, game_id):
        self.connector.append_string("game/" + str(game_id) + "/boxscore")
        self.logger.info("Get game data request:" + self.connector.request_string)
        game = self.connector.execute()
        return game['teams']

    def get_salaries(self):
        browser = webdriver.Safari()
        browser.maximize_window()
        url = "https://www.fanduel.com/login?cc_success_url=%2Fcontests%2Fnhl%2F14"
        browser.get(url)  # navigate to the page
        time.sleep(3)
        username = browser.find_element_by_id("forms.login.email")  # username form field
        password = browser.find_element_by_id("forms.login.password")  # password form field

        username.send_keys("REDACTED EMAIL")
        password.send_keys("REDACTED PASSWORD")

        submit_button = browser.find_element_by_id("forms.login.submit")
        submit_button.click()

        time.sleep(3)
        inner_html = browser.execute_script("return document.body.innerHTML")

        self.salary_parser.feed(inner_html)
        full_page_id = self.salary_parser.page_id
        self.logger.info("Full Page ID: " + full_page_id)
        first_page_id = full_page_id.split('-')[0]

        tournament_url = "https://www.fanduel.com/games/"
        tournament_url += first_page_id
        tournament_url += "/contests/"
        tournament_url += full_page_id
        tournament_url += "/enter/"

        browser.get(tournament_url)  # navigate to the page
        time.sleep(3)
        download_link = browser.find_element_by_partial_link_text("Download players list")
        download_link.click()

    def get_season(self, years):
        self.connector.append_string("schedule?season=" + str(years))
        self.logger.info("Get season schedule request: " + self.connector.request_string)
        schedule = self.connector.execute()
        dates = schedule['dates']
        first_date = dates[0]['date']
        last_date = dates[0]['date']
        first_date_found = False
        for date in dates:
            for game in date['games']:
                if game['gameType'] == 'R':
                    if not first_date_found:
                        first_date = date['date']
                        first_date_found = True
                    last_date = date['date']
        return first_date, last_date

    def get_seasons(self, starting_year, ending_year):
        seasons = []
        for i in range(starting_year, ending_year):
            season = str(i) + str(i + 1)
            dates = self.get_season(season)
            seasons.append({
                "SEASON_ID": str(i) + str(i + 1),
                "START_DATE": dates[0],
                "END_DATE": dates[1]
            })
        return seasons

    def get_active_statuses(self, players):
        browser = webdriver.Safari()
        browser.maximize_window()
        url = "https://www.fanduel.com/login?cc_success_url=%2Fcontests%2Fnhl%2F14"
        browser.get(url)  # navigate to the page
        time.sleep(3)
        username = browser.find_element_by_id("forms.login.email")  # username form field
        password = browser.find_element_by_id("forms.login.password")  # password form field

        username.send_keys("REDACTED EMAIL")
        password.send_keys("REDACTED PASSWORD")

        submit_button = browser.find_element_by_id("forms.login.submit")
        submit_button.click()

        time.sleep(3)
        inner_html = browser.execute_script("return document.body.innerHTML")

        self.salary_parser.feed(inner_html)
        full_page_id = self.salary_parser.page_id
        self.logger.info("Full Page ID: " + full_page_id)
        first_page_id = full_page_id.split('-')[0]

        tournament_url = "https://www.fanduel.com/games/"
        tournament_url += first_page_id
        tournament_url += "/contests/"
        tournament_url += full_page_id
        tournament_url += "/enter/"

        browser.get(tournament_url)  # navigate to the page
        time.sleep(2)

        return_projected = {}

        search_element = browser.find_element_by_xpath("//input[@title='Find a player']")
        for player in players:
            search_element.clear()
            search_element.send_keys(player['FULL_NAME'])
            time.sleep(0.5)
            player_element = browser.find_element_by_class_name("player-indicators")

            projected = player_element.text
            projected = projected.replace('\n', '').replace('\t', '')
            return_projected[player['PLAYER_ID']] = projected

        return return_projected

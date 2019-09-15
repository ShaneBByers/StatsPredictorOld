from WebDataCollector import WebDataCollector
from DatabaseDataManager import DatabaseDataManager
from FileManager import FileManager
from LinearProgrammingSolver import IntegerProgrammingSolver

from datetime import datetime, timedelta
import logging
import time


class DataManager:

    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)

        self.web_data = WebDataCollector(logger_name)

        # self.db_collect = DatabaseDataCollector(logger_name)

        self.db_manager = DatabaseDataManager(logger_name)

        self.ip_solver = IntegerProgrammingSolver(logger_name)

        self.file_manager = FileManager(logger_name)

    def transfer_teams(self):
        teams = self.web_data.get_teams()

        insert_team_values = []

        for team in teams:
            insert_team_values.append((
                team['id'],
                team['name'],
                team['teamName'],
                team['locationName'],
                team['abbreviation'],
                team['shortName'],
                team['division']['id'],
                team['conference']['id'],
                team['venue']['timeZone']['offset']
            ))
        self.db_manager.insert('WEB_TEAM', insert_team_values)

    def transfer_players(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))
        for season in active_seasons:
            players = self.web_data.get_players(season['SEASON_ID'])

            insert_player_values = []

            db_players = self.db_manager.select('WEB_NHL_PLAYER',
                                                cols=('PLAYER_ID',))

            for player in players:
                already_in_db = False
                for db_player in db_players:
                    if player['id'] == db_player['PLAYER_ID']:
                        already_in_db = True
                        break
                if not already_in_db:
                    height_int = None
                    if 'height' in player:
                        height_string = player['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_player_values.append((
                        player['id'],
                        player['fullName'],
                        player['firstName'],
                        player['lastName'],
                        player['primaryNumber'] if 'primaryNumber' in player else None,
                        player['currentAge'] if 'currentAge' in player else None,
                        height_int,
                        player['weight'],
                        player['alternateCaptain'] if 'alternateCaptain' in player else None,
                        player['captain'] if 'captain' in player else None,
                        player['rookie'],
                        player['shootsCatches'],
                        player['currentTeam']['id'] if 'currentTeam' in player else None,
                        player['primaryPosition']['code'],
                        player['primaryPosition']['type']
                    ))

            self.db_manager.insert('WEB_NHL_PLAYER', insert_player_values)

    def transfer_goalies(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:

            goalies = self.web_data.get_goalies(season['SEASON_ID'])

            insert_goalie_values = []

            db_goalies = self.db_manager.select('WEB_NHL_GOALIE',
                                                cols=('GOALIE_ID',))

            for goalie in goalies:
                already_in_db = False
                for db_goalie in db_goalies:
                    if goalie['id'] == db_goalie['GOALIE_ID']:
                        already_in_db = True
                        break
                if not already_in_db:
                    height_int = None
                    if 'height' in goalie:
                        height_string = goalie['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_goalie_values.append((
                        goalie['id'],
                        goalie['fullName'],
                        goalie['firstName'],
                        goalie['lastName'],
                        goalie['primaryNumber'] if 'primaryNumber' in goalie else None,
                        goalie['currentAge'] if 'currentAge' in goalie else None,
                        height_int,
                        goalie['weight'],
                        goalie['alternateCaptain'] if 'alternateCaptain' in goalie else None,
                        goalie['captain'] if 'captain' in goalie else None,
                        goalie['rookie'],
                        goalie['shootsCatches'] if 'shootsCatches' in goalie else 'L',
                        goalie['currentTeam']['id'] if 'currentTeam' in goalie else None,
                        goalie['primaryPosition']['code'],
                        goalie['primaryPosition']['type']
                    ))

            self.db_manager.insert('WEB_NHL_GOALIE', insert_goalie_values)

    def transfer_games(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:

            games = self.web_data.get_games(season['START_DATE'], season['END_DATE'])

            insert_game_values = []

            for game in games:
                insert_game_values.append((
                    game['gamePk'],
                    season['SEASON_ID'],
                    game['teams']['home']['team']['id'],
                    game['teams']['away']['team']['id'],
                    game['gameDate'].replace('T', ' ').replace('Z', '')
                ))

            self.db_manager.insert('WEB_NHL_GAME', insert_game_values)

    def transfer_team_stats(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            games = self.db_manager.select('WEB_NHL_GAME',
                                           where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_team_stats_values = []
            for game in games:
                if game['DATE_TIME'] < datetime.now():
                    self.get_single_game_team_stats(game, insert_team_stats_values)
            self.db_manager.insert('WEB_NHL_TEAM_STATS', insert_team_stats_values)

    def transfer_player_stats(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        db_players = self.db_manager.select('WEB_NHL_PLAYER')

        for season in active_seasons:
            games = self.db_manager.select('WEB_NHL_GAME',
                                           where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_player_values = []
            insert_player_stats_values = []
            for game in games:
                if game['DATE_TIME'] < datetime.now():
                    self.get_single_game_player_stats(game,
                                                      insert_player_stats_values,
                                                      db_players,
                                                      insert_player_values)
            self.db_manager.insert('WEB_NHL_PLAYER', insert_player_values)
            self.db_manager.insert('WEB_NHL_PLAYER_STATS', insert_player_stats_values)

    def transfer_goalie_stats(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        db_goalies = self.db_manager.select('WEB_NHL_GOALIE')

        for season in active_seasons:
            games = self.db_manager.select('WEB_NHL_GAME',
                                           where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_goalie_stats_values = []
            insert_goalie_values = []
            for game in games:
                if game['DATE_TIME'] < datetime.now():
                    self.get_single_game_goalie_stats(game,
                                                      insert_goalie_stats_values,
                                                      db_goalies,
                                                      insert_goalie_values)

            self.db_manager.insert('WEB_NHL_GOALIE', insert_goalie_values)
            self.db_manager.insert('WEB_NHL_GOALIE_STATS', insert_goalie_stats_values)

    def transfer_salaries(self):
        self.web_data.get_salaries()
        time.sleep(3)
        self.file_manager.transfer_all_files()
        salary_info = self.file_manager.parse_all_files()
        insert_salary_info = []
        fd_players = self.db_manager.select('WEB_FD_PLAYER')
        nhl_players = self.db_manager.select('WEB_NHL_PLAYER')
        nhl_goalies = self.db_manager.select('WEB_NHL_GOALIE')
        connect_players = self.db_manager.select('WEB_CONNECT_PLAYER')
        for tournament_key in salary_info:
            tournament = salary_info[tournament_key]
            insert_players = []
            insert_connect_players = []
            file_players = tournament['PLAYERS']
            for player_id in file_players:
                player = file_players[player_id]
                found = False
                for db_player in fd_players:
                    if str(db_player['PLAYER_ID']) == player_id:
                        found = True
                if not found:
                    insert_players.append((
                        player_id,
                        player['First Name'] + ' ' + player['Last Name'],
                        player['First Name'],
                        player['Last Name']
                    ))
                    already_connected = False
                    for connect_player in connect_players:
                        if str(connect_player['FD_PLAYER_ID']) == player_id:
                            already_connected = True
                            break
                    if not already_connected:
                        if player['Position'] is not 'G':
                            connect_found = False
                            for nhl_player in nhl_players:
                                if nhl_player['FIRST_NAME'] == player['First Name'] and \
                                        nhl_player['LAST_NAME'] == player['Last Name']:
                                    insert_connect_players.append((
                                        player_id,
                                        nhl_player['PLAYER_ID'],
                                        None
                                    ))
                                    connect_found = True
                                    break
                            if not connect_found:
                                insert_connect_players.append((
                                    player_id,
                                    None,
                                    None
                                ))
                        else:
                            connect_found = False
                            for nhl_goalie in nhl_goalies:
                                if nhl_goalie['FIRST_NAME'] == player['First Name'] and \
                                        nhl_goalie['LAST_NAME'] == player['Last Name']:
                                    insert_connect_players.append((
                                        player_id,
                                        None,
                                        nhl_goalie['GOALIE_ID']
                                    ))
                                    connect_found = True
                                    break
                            if not connect_found:
                                insert_connect_players.append((
                                    player_id,
                                    None,
                                    None
                                ))
                insert_salary_info.append((
                    tournament_key,
                    player_id,
                    player['Position'],
                    player['FPPG'],
                    player['Played'],
                    player['Salary'],
                    player['Game'],
                    player['Team'],
                    player['Opponent'],
                    player['Injury Indicator'],
                    player['Injury Details'],
                    player['Tier']
                ))
            insert_tournament = [(
                tournament_key,
                tournament['DATE_TIME'],
                tournament['FILE_NAME']
            )]
            self.db_manager.insert('WEB_FD_TOURNAMENT', insert_tournament, False)
            self.db_manager.insert('WEB_FD_PLAYER', insert_players, False)
            self.db_manager.insert('WEB_CONNECT_PLAYER', insert_connect_players, False)
            self.db_manager.insert('WEB_FD_TOURNAMENT_DATA', insert_salary_info, False)
            self.db_manager.commit()

        self.file_manager.archive_all_files()

    def calculate_average_player_stats(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            insert_avg_player_stats_values = []
            player_stats = self.db_manager.select('WEB_NHL_PLAYER_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))

            for player_stat in player_stats:
                self.calculate_single_game_average_player_stats(player_stat,
                                                                insert_avg_player_stats_values)
            self.db_manager.insert('AVG_PLAYER_STATS', insert_avg_player_stats_values)

    def calculate_average_goalie_stats(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            insert_avg_goalie_stats_values = []
            goalie_stats = self.db_manager.select('WEB_NHL_GOALIE_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))

            for goalie_stat in goalie_stats:
                self.calculate_single_game_average_goalie_stats(goalie_stat,
                                                                insert_avg_goalie_stats_values)
            self.db_manager.insert('AVG_GOALIE_STATS', insert_avg_goalie_stats_values)

    def transfer_pred_player_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            player_stats = self.db_manager.select('AVG_PLAYER_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_pred_player_values = []

            for player_stat in player_stats:
                insert_pred_player_values.append((
                    player_stat['PLAYER_ID'],
                    player_stat['GAME_ID'],
                    season['SEASON_ID'],
                    player_stat['GOALS'],
                    player_stat['ASSISTS'],
                    player_stat['SHOTS'],
                    player_stat['PP_GOALS'] + player_stat['PP_ASSISTS'],
                    player_stat['SH_GOALS'] + player_stat['SH_ASSISTS'],
                    player_stat['BLOCKED_SHOTS'],
                    None
                ))

            self.db_manager.insert('PRED_PLAYER_VALUES',
                                   insert_pred_player_values)

    def transfer_pred_goalie_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            goalie_stats = self.db_manager.select('AVG_GOALIE_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_pred_goalie_values = []

            for goalie_stat in goalie_stats:
                insert_pred_goalie_values.append((
                    goalie_stat['GOALIE_ID'],
                    goalie_stat['GAME_ID'],
                    season['SEASON_ID'],
                    goalie_stat['WIN'],
                    goalie_stat['GOALS_AGAINST'],
                    goalie_stat['SAVES'],
                    goalie_stat['SHUTOUT'],
                    None
                ))

            self.db_manager.insert('PRED_GOALIE_VALUES',
                                   insert_pred_goalie_values)

    def transfer_act_player_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            player_stats = self.db_manager.select('WEB_NHL_PLAYER_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_act_player_values = []

            for player_stat in player_stats:
                insert_act_player_values.append((
                    player_stat['PLAYER_ID'],
                    player_stat['GAME_ID'],
                    player_stat['SEASON_ID'],
                    player_stat['GOALS'],
                    player_stat['ASSISTS'],
                    player_stat['SHOTS'],
                    player_stat['PP_GOALS'] + player_stat['PP_ASSISTS'],
                    player_stat['SH_GOALS'] + player_stat['SH_ASSISTS'],
                    player_stat['BLOCKED_SHOTS'],
                    None
                ))

            self.db_manager.insert('ACT_PLAYER_VALUES',
                                   insert_act_player_values)

    def transfer_act_goalie_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            goalie_stats = self.db_manager.select('WEB_NHL_GOALIE_STATS',
                                                  where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_act_goalie_values = []

            for goalie_stat in goalie_stats:
                is_win = False
                if goalie_stat['DECISION'] == 'W':
                    is_win = True
                is_shutout = goalie_stat['SHOTS_AGAINST'] == goalie_stat['SAVES']
                insert_act_goalie_values.append((
                    goalie_stat['GOALIE_ID'],
                    goalie_stat['GAME_ID'],
                    season['SEASON_ID'],
                    is_win,
                    goalie_stat['SHOTS_AGAINST'] - goalie_stat['SAVES'],
                    goalie_stat['SAVES'],
                    is_shutout,
                    None
                ))

            self.db_manager.insert('ACT_GOALIE_VALUES', insert_act_goalie_values)

    def calculate_player_cost_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause=('IS_ACTIVE = 1',))

        for season in active_seasons:
            pred_values = self.db_manager.select('PRED_PLAYER_VALUES',
                                                 where_clause=('SEASON_ID = ' + str(season['SEASON_ID']),))
            insert_cost_values = []

            for pred_value in pred_values:
                act_value = self.db_manager.select('ACT_PLAYER_VALUES',
                                                   where_clause=('PLAYER_ID = ' +
                                                                 str(pred_value['PLAYER_ID']) +
                                                                 ' && GAME_ID = ' +
                                                                 str(pred_value['GAME_ID']),),
                                                   single=True)

                pred = 12 * float(pred_value['GOALS']) +\
                    8 * float(pred_value['ASSISTS']) +\
                    1.6 * float(pred_value['SHOTS']) +\
                    2 * float(pred_value['SH_POINTS']) +\
                    0.5 * float(pred_value['PP_POINTS']) +\
                    1.6 * float(pred_value['BLOCKED_SHOTS'])
                act = 12 * float(act_value['GOALS']) +\
                    8 * float(act_value['ASSISTS']) +\
                    1.6 * float(act_value['SHOTS']) +\
                    2 * float(act_value['SH_POINTS']) +\
                    0.5 * float(act_value['PP_POINTS']) +\
                    1.6 * float(act_value['BLOCKED_SHOTS'])
                if act == 0.0:
                    cost = None
                else:
                    cost = abs(pred - act)/act
                insert_cost_values.append((
                    pred_value['PLAYER_ID'],
                    pred_value['GAME_ID'],
                    season['SEASON_ID'],
                    cost
                ))

            self.db_manager.insert('SIM_PLAYER_COST_VALUE',
                                   insert_cost_values)

    def calculate_goalie_cost_values(self):
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause='IS_ACTIVE = 1')

        for season in active_seasons:
            pred_values = self.db_manager.select('PRED_GOALIE_VALUES',
                                                 where_clause='SEASON_ID = ' + str(season['SEASON_ID']))
            insert_cost_values = []

            for pred_value in pred_values:
                act_value = self.db_manager.select('ACT_GOALIE_VALUES',
                                                   where_clause=('GOALIE_ID = ' + str(pred_value['GOALIE_ID']),
                                                                 'GAME_ID = ' + str(pred_value['GAME_ID'])),
                                                   single=True)

                pred = 12 * float(pred_value['WIN']) +\
                    0.8 * float(pred_value['SAVES']) +\
                    8 * float(pred_value['SHUTOUT']) -\
                    4 * float(pred_value['GOALS_AGAINST'])
                act = 12 * float(act_value['WIN']) +\
                    0.8 * float(act_value['SAVES']) +\
                    8 * float(act_value['SHUTOUT']) -\
                    4 * float(act_value['GOALS_AGAINST'])
                if act == 0.0:
                    cost = None
                else:
                    cost = abs(pred - act)/act
                insert_cost_values.append((
                    pred_value['GOALIE_ID'],
                    pred_value['GAME_ID'],
                    season['SEASON_ID'],
                    cost
                ))

            self.db_manager.insert('SIM_GOALIE_COST_VALUE',
                                   insert_cost_values)

    def transfer_season_data(self):
        seasons = self.web_data.get_seasons(2016, 2019)
        insert_season_values = []
        for season in seasons:
            insert_season_values.append((
                season['SEASON_ID'],
                season['START_DATE'],
                season['END_DATE'],
                False
            ))

        self.db_manager.insert('WEB_NHL_SEASON',
                               insert_season_values)

    def calculate_timezone_offsets(self):
        games = self.db_manager.select('WEB_NHL_GAME')
        update_timezone_clauses = []
        for game in games:
            team = self.db_manager.select('WEB_NHL_TEAM',
                                          cols=('TIMEZONE_OFFSET',),
                                          where_clause='TEAM_ID = ' + str(game['HOME_ID']),
                                          single=True)
            current_date_time = game['DATE_TIME']
            new_date_time = current_date_time + timedelta(hours=team['TIMEZONE_OFFSET'])
            update_timezone_clause = {
                'COL_VALS': {
                    'DATE_TIME': new_date_time.strftime('%Y-%m-%d %H:%M:%S')
                },
                'WHERE_CLAUSE': 'GAME_ID = ' + str(game['GAME_ID'])
            }
            update_timezone_clauses.append(update_timezone_clause)

        self.db_manager.update('WEB_NHL_GAME',
                               clauses=update_timezone_clauses)

    def current_day_functions(self):
        current_day = datetime.now()
        insert_team_stats_values = []
        insert_player_stats_values = []
        insert_player_values = []
        insert_goalie_stats_values = []
        insert_goalie_values = []
        insert_avg_player_stats_values = []
        insert_avg_goalie_stats_values = []
        insert_act_player_values = []
        insert_act_goalie_values = []
        insert_pred_player_values = []
        insert_pred_goalie_values = []
        active_seasons = self.db_manager.select('WEB_NHL_SEASON',
                                                where_clause='IS_ACTIVE = 1')

        db_players = self.db_manager.select('WEB_NHL_PLAYER')
        db_goalies = self.db_manager.select('WEB_NHL_GOALIE')

        for season in active_seasons:
            games = self.db_manager.select('WEB_NHL_GAME',
                                           where_clause='SEASON_ID = ' + str(season['SEASON_ID']))

            for game in games:
                if game['DATE_TIME'].date() < current_day.date():
                    team_stats = self.db_manager.select('WEB_NHL_TEAM_STATS',
                                                        where_clause='GAME_ID = ' + str(game['GAME_ID']))
                    player_stats = self.db_manager.select('WEB_NHL_PLAYER_STATS',
                                                          where_clause='GAME_ID = ' + str(game['GAME_ID']))
                    goalie_stats = self.db_manager.select('WEB_NHL_GOALIE_STATS',
                                                          where_clause='GAME_ID = ' + str(game['GAME_ID']))
                    if len(team_stats) == 0:
                        self.get_single_game_team_stats(game, insert_team_stats_values)

                    if len(player_stats) == 0:
                        self.get_single_game_player_stats(game,
                                                          insert_player_stats_values,
                                                          db_players,
                                                          insert_player_values)
                    if len(goalie_stats) == 0:
                        self.get_single_game_goalie_stats(game,
                                                          insert_goalie_stats_values,
                                                          db_goalies,
                                                          insert_goalie_values)

            self.db_manager.insert('WEB_NHL_TEAM_STATS',
                                   insert_team_stats_values)
            self.db_manager.insert('WEB_NHL_PLAYER',
                                   insert_player_values)
            self.db_manager.insert('WEB_NHL_PLAYER_STATS',
                                   insert_player_stats_values)
            self.db_manager.insert('WEB_NHL_GOALIE',
                                   insert_goalie_values)
            self.db_manager.insert('WEB_NHL_GOALIE_STATS',
                                   insert_goalie_stats_values)

            for insert_player_stat in insert_player_stats_values:
                player_stat = self.db_manager.select('WEB_NHL_PLAYER_STATS',
                                                     where_clause=('PLAYER_ID = ' + str(insert_player_stat[0]),
                                                                   'GAME_ID = ' + str(insert_player_stat[1])),
                                                     single=True)
                self.calculate_single_game_average_player_stats(player_stat,
                                                                insert_avg_player_stats_values)
                insert_act_player_values.append((
                    player_stat['PLAYER_ID'],
                    player_stat['GAME_ID'],
                    player_stat['SEASON_ID'],
                    player_stat['GOALS'],
                    player_stat['ASSISTS'],
                    player_stat['SHOTS'],
                    player_stat['PP_GOALS'] + player_stat['PP_ASSISTS'],
                    player_stat['SH_GOALS'] + player_stat['SH_ASSISTS'],
                    player_stat['BLOCKED_SHOTS'],
                    None
                ))

            for insert_goalie_stat in insert_goalie_stats_values:
                goalie_stat = self.db_manager.select('WEB_NHL_GOALIE_STATS',
                                                     where_clause=('GOALIE_ID = ' + str(insert_goalie_stat[0]),
                                                                   'GAME_ID = ' + str(insert_goalie_stat[1])),
                                                     single=True)

                self.calculate_single_game_average_goalie_stats(goalie_stat,
                                                                insert_avg_goalie_stats_values)
                is_win = False
                if goalie_stat['DECISION'] == 'W':
                    is_win = True
                is_shutout = goalie_stat['SHOTS_AGAINST'] == goalie_stat['SAVES']
                insert_act_goalie_values.append((
                    goalie_stat['GOALIE_ID'],
                    goalie_stat['GAME_ID'],
                    season['SEASON_ID'],
                    is_win,
                    goalie_stat['SHOTS_AGAINST'] - goalie_stat['SAVES'],
                    goalie_stat['SAVES'],
                    is_shutout,
                    None
                ))

            self.db_manager.insert('AVG_PLAYER_STATS',
                                   insert_avg_player_stats_values)
            self.db_manager.insert('AVG_GOALIE_STATS',
                                   insert_avg_goalie_stats_values)
            self.db_manager.insert('ACT_PLAYER_VALUES',
                                   insert_act_player_values)
            self.db_manager.insert('ACT_GOALIE_VALUES',
                                   insert_act_goalie_values)

            for insert_avg_player_stat in insert_avg_player_stats_values:
                player_stat = self.db_manager.select('AVG_PLAYER_STATS',
                                                     where_clause=('PLAYER_ID = ' + str(insert_avg_player_stat[0]),
                                                                   'GAME_ID = ' + str(insert_avg_player_stat[1])),
                                                     single=True)
                insert_pred_player_values.append((
                    player_stat['PLAYER_ID'],
                    player_stat['GAME_ID'],
                    season['SEASON_ID'],
                    player_stat['GOALS'],
                    player_stat['ASSISTS'],
                    player_stat['SHOTS'],
                    player_stat['PP_GOALS'] + player_stat['PP_ASSISTS'],
                    player_stat['SH_GOALS'] + player_stat['SH_ASSISTS'],
                    player_stat['BLOCKED_SHOTS'],
                    None
                ))

            for insert_avg_goalie_stat in insert_avg_goalie_stats_values:
                goalie_stat = self.db_manager.select('AVG_GOALIE_STATS',
                                                     where_clause=('GOALIE_ID = ' + str(insert_avg_goalie_stat[0]),
                                                                   'GAME_ID = ' + str(insert_avg_goalie_stat[1])),
                                                     single=True)
                insert_pred_goalie_values.append((
                    goalie_stat['GOALIE_ID'],
                    goalie_stat['GAME_ID'],
                    season['SEASON_ID'],
                    goalie_stat['WIN'],
                    goalie_stat['GOALS_AGAINST'],
                    goalie_stat['SAVES'],
                    goalie_stat['SHUTOUT'],
                    None
                ))

            self.db_manager.insert('PRED_PLAYER_VALUES',
                                   insert_pred_player_values)
            self.db_manager.insert('PRED_GOALIE_VALUES',
                                   insert_pred_goalie_values)

        self.transfer_salaries()

    def get_single_game_team_stats(self, game, insert_team_stats_values):
        game_id = game['GAME_ID']
        game_data = self.web_data.get_game(game_id)
        home = game_data['home']
        stats = home['teamStats']['teamSkaterStats']
        insert_team_stats_values.append((
            game_id,
            home['team']['id'],
            game['SEASON_ID'],
            True,
            stats['goals'],
            stats['pim'],
            stats['shots'],
            float(stats['powerPlayPercentage']),
            stats['powerPlayGoals'],
            stats['powerPlayOpportunities'],
            float(stats['faceOffWinPercentage']),
            stats['blocked'],
            stats['takeaways'],
            stats['giveaways'],
            stats['hits']
        ))
        away = game_data['away']
        stats = away['teamStats']['teamSkaterStats']
        insert_team_stats_values.append((
            game_id,
            away['team']['id'],
            game['SEASON_ID'],
            False,
            stats['goals'],
            stats['pim'],
            stats['shots'],
            float(stats['powerPlayPercentage']),
            stats['powerPlayGoals'],
            stats['powerPlayOpportunities'],
            float(stats['faceOffWinPercentage']),
            stats['blocked'],
            stats['takeaways'],
            stats['giveaways'],
            stats['hits']
        ))

    def get_single_game_player_stats(self, game, insert_player_stats_values, db_players, insert_player_values):
        game_id = game['GAME_ID']
        game_data = self.web_data.get_game(game_id)
        away = game_data['away']
        players = away['players']
        for player_id in players:
            if 'skaterStats' in players[player_id]['stats']:
                already_in_db = False
                actual_id = players[player_id]['person']['id']
                for db_player in db_players:
                    if db_player['PLAYER_ID'] == actual_id:
                        already_in_db = True
                        break
                if not already_in_db:
                    for insert_player in insert_player_values:
                        if insert_player[0] == actual_id:
                            already_in_db = True
                            break
                if not already_in_db:
                    height_int = None
                    player = players[player_id]['person']
                    if 'height' in player:
                        height_string = player['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_player_values.append((
                        player['id'],
                        player['fullName'],
                        player['firstName'],
                        player['lastName'],
                        player['primaryNumber'] if 'primaryNumber' in player else None,
                        player['currentAge'] if 'currentAge' in player else None,
                        height_int,
                        player['weight'] if 'weight' in player else None,
                        player['alternateCaptain'] if 'alternateCaptain' in player else None,
                        player['captain'] if 'captain' in player else None,
                        player['rookie'],
                        player['shootsCatches'],
                        player['currentTeam']['id'] if 'currentTeam' in player else None,
                        player['primaryPosition']['code'],
                        player['primaryPosition']['type']
                    ))
                stats = players[player_id]['stats']['skaterStats']
                toi_string = stats['timeOnIce']
                toi = int(toi_string.split(':')[0]) * 60 + int(toi_string.split(':')[1])
                even_toi_string = stats['evenTimeOnIce']
                even_toi = int(even_toi_string.split(':')[0]) * 60 + int(even_toi_string.split(':')[1])
                pp_toi_string = stats['powerPlayTimeOnIce']
                pp_toi = int(pp_toi_string.split(':')[0]) * 60 + int(pp_toi_string.split(':')[1])
                sh_toi_string = stats['shortHandedTimeOnIce']
                sh_toi = int(sh_toi_string.split(':')[0]) * 60 + int(sh_toi_string.split(':')[1])
                insert_player_stats_values.append((
                    players[player_id]['person']['id'],
                    game_id,
                    game['SEASON_ID'],
                    away['team']['id'],
                    toi,
                    stats['assists'],
                    stats['goals'],
                    stats['shots'],
                    stats['hits'],
                    stats['powerPlayGoals'],
                    stats['powerPlayAssists'],
                    stats['penaltyMinutes'],
                    stats['faceOffPct'] if 'faceOffPct' in stats else None,
                    stats['faceOffWins'],
                    stats['faceoffTaken'],
                    stats['takeaways'],
                    stats['giveaways'],
                    stats['shortHandedGoals'],
                    stats['shortHandedAssists'],
                    stats['blocked'],
                    stats['plusMinus'],
                    even_toi,
                    pp_toi,
                    sh_toi
                ))
        home = game_data['home']
        players = home['players']
        for player_id in players:
            if 'skaterStats' in players[player_id]['stats']:
                already_in_db = False
                actual_id = players[player_id]['person']['id']
                for db_player in db_players:
                    if db_player['PLAYER_ID'] == actual_id:
                        already_in_db = True
                        break
                if not already_in_db:
                    for insert_player in insert_player_values:
                        if insert_player[0] == actual_id:
                            already_in_db = True
                            break
                if not already_in_db:
                    height_int = None
                    player = players[player_id]['person']
                    if 'height' in player:
                        height_string = player['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_player_values.append((
                        player['id'],
                        player['fullName'],
                        player['firstName'],
                        player['lastName'],
                        player['primaryNumber'] if 'primaryNumber' in player else None,
                        player['currentAge'] if 'currentAge' in player else None,
                        height_int,
                        player['weight'] if 'weight' in player else None,
                        player['alternateCaptain'] if 'alternateCaptain' in player else None,
                        player['captain'] if 'captain' in player else None,
                        player['rookie'],
                        player['shootsCatches'],
                        player['currentTeam']['id'] if 'currentTeam' in player else None,
                        player['primaryPosition']['code'],
                        player['primaryPosition']['type']
                    ))
                stats = players[player_id]['stats']['skaterStats']
                toi_string = stats['timeOnIce']
                toi = int(toi_string.split(':')[0]) * 60 + int(toi_string.split(':')[1])
                even_toi_string = stats['evenTimeOnIce']
                even_toi = int(even_toi_string.split(':')[0]) * 60 + int(even_toi_string.split(':')[1])
                pp_toi_string = stats['powerPlayTimeOnIce']
                pp_toi = int(pp_toi_string.split(':')[0]) * 60 + int(pp_toi_string.split(':')[1])
                sh_toi_string = stats['shortHandedTimeOnIce']
                sh_toi = int(sh_toi_string.split(':')[0]) * 60 + int(sh_toi_string.split(':')[1])
                insert_player_stats_values.append((
                    players[player_id]['person']['id'],
                    game_id,
                    game['SEASON_ID'],
                    home['team']['id'],
                    toi,
                    stats['assists'],
                    stats['goals'],
                    stats['shots'],
                    stats['hits'],
                    stats['powerPlayGoals'],
                    stats['powerPlayAssists'],
                    stats['penaltyMinutes'],
                    stats['faceOffPct'] if 'faceOffPct' in stats else None,
                    stats['faceOffWins'],
                    stats['faceoffTaken'],
                    stats['takeaways'],
                    stats['giveaways'],
                    stats['shortHandedGoals'],
                    stats['shortHandedAssists'],
                    stats['blocked'],
                    stats['plusMinus'],
                    even_toi,
                    pp_toi,
                    sh_toi
                ))

    def get_single_game_goalie_stats(self, game, insert_goalie_stats_values, db_goalies, insert_goalie_values):
        game_id = game['GAME_ID']
        game_data = self.web_data.get_game(game_id)
        away = game_data['away']
        goalies = away['players']
        for goalie_id in goalies:
            if 'goalieStats' in goalies[goalie_id]['stats']:
                already_in_db = False
                actual_id = goalies[goalie_id]['person']['id']
                for db_goalie in db_goalies:
                    if db_goalie['GOALIE_ID'] == actual_id:
                        already_in_db = True
                        break
                if not already_in_db:
                    for insert_goalie in insert_goalie_values:
                        if insert_goalie[0] == actual_id:
                            already_in_db = True
                            break
                if not already_in_db:
                    height_int = None
                    goalie = goalies[goalie_id]['person']
                    if 'height' in goalie:
                        height_string = goalie['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_goalie_values.append((
                        goalie['id'],
                        goalie['fullName'],
                        goalie['firstName'],
                        goalie['lastName'],
                        goalie['primaryNumber'] if 'primaryNumber' in goalie else None,
                        goalie['currentAge'] if 'currentAge' in goalie else None,
                        height_int,
                        goalie['weight'],
                        goalie['alternateCaptain'] if 'alternateCaptain' in goalie else None,
                        goalie['captain'] if 'captain' in goalie else None,
                        goalie['rookie'],
                        goalie['shootsCatches'] if 'shootsCatches' in goalie else 'L',
                        goalie['currentTeam']['id'] if 'currentTeam' in goalie else None,
                        goalie['primaryPosition']['code'],
                        goalie['primaryPosition']['type']
                    ))
                stats = goalies[goalie_id]['stats']['goalieStats']
                toi_string = stats['timeOnIce']
                toi = int(toi_string.split(':')[0]) * 60 + int(toi_string.split(':')[1])
                insert_goalie_stats_values.append((
                    goalies[goalie_id]['person']['id'],
                    game_id,
                    game['SEASON_ID'],
                    away['team']['id'],
                    toi,
                    stats['shots'],
                    stats['saves'],
                    stats['powerPlaySaves'],
                    stats['shortHandedSaves'],
                    stats['evenSaves'],
                    stats['powerPlayShotsAgainst'],
                    stats['shortHandedShotsAgainst'],
                    stats['evenShotsAgainst'],
                    stats['decision'],
                    stats['savePercentage'] if 'savePercentage' in stats else None,
                    stats['powerPlaySavePercentage'] if 'powerPlaySavePercentage' in stats else None,
                    stats['shortHandedSavePercentage'] if 'shortHandedSavePercentage' in stats else None,
                    stats['evenStrengthSavePercentage'] if 'evenStrengthSavePercentage' in stats else None
                ))
        home = game_data['home']
        goalies = home['players']
        for goalie_id in goalies:
            if 'goalieStats' in goalies[goalie_id]['stats']:
                already_in_db = False
                actual_id = goalies[goalie_id]['person']['id']
                for db_goalie in db_goalies:
                    if db_goalie['GOALIE_ID'] == actual_id:
                        already_in_db = True
                        break
                if not already_in_db:
                    for insert_goalie in insert_goalie_values:
                        if insert_goalie[0] == actual_id:
                            already_in_db = True
                            break
                if not already_in_db:
                    height_int = None
                    goalie = goalies[goalie_id]['person']
                    if 'height' in goalie:
                        height_string = goalie['height']
                        feet, inches_string = height_string.split("' ")
                        height_int = int(feet) * 12 + int(inches_string.split("\"")[0])
                    insert_goalie_values.append((
                        goalie['id'],
                        goalie['fullName'],
                        goalie['firstName'],
                        goalie['lastName'],
                        goalie['primaryNumber'] if 'primaryNumber' in goalie else None,
                        goalie['currentAge'] if 'currentAge' in goalie else None,
                        height_int,
                        goalie['weight'],
                        goalie['alternateCaptain'] if 'alternateCaptain' in goalie else None,
                        goalie['captain'] if 'captain' in goalie else None,
                        goalie['rookie'],
                        goalie['shootsCatches'] if 'shootsCatches' in goalie else 'L',
                        goalie['currentTeam']['id'] if 'currentTeam' in goalie else None,
                        goalie['primaryPosition']['code'],
                        goalie['primaryPosition']['type']
                    ))
                stats = goalies[goalie_id]['stats']['goalieStats']
                toi_string = stats['timeOnIce']
                toi = int(toi_string.split(':')[0]) * 60 + int(toi_string.split(':')[1])
                insert_goalie_stats_values.append((
                    goalies[goalie_id]['person']['id'],
                    game_id,
                    game['SEASON_ID'],
                    home['team']['id'],
                    toi,
                    stats['shots'],
                    stats['saves'],
                    stats['powerPlaySaves'],
                    stats['shortHandedSaves'],
                    stats['evenSaves'],
                    stats['powerPlayShotsAgainst'],
                    stats['shortHandedShotsAgainst'],
                    stats['evenShotsAgainst'],
                    stats['decision'],
                    stats['savePercentage'] if 'savePercentage' in stats else None,
                    stats['powerPlaySavePercentage'] if 'powerPlaySavePercentage' in stats else None,
                    stats['shortHandedSavePercentage'] if 'shortHandedSavePercentage' in stats else None,
                    stats['evenStrengthSavePercentage'] if 'evenStrengthSavePercentage' in stats else None
                ))

    def calculate_single_game_average_player_stats(self, player_stat, insert_avg_player_stats_values):
        current_game = self.db_manager.select('WEB_NHL_GAME',
                                              where_clause=('GAME_ID = ' + str(player_stat['GAME_ID']),),
                                              single=True)
        previous_games_for_player = self.db_manager.select('WEB_NHL_GAME',
                                                           cols=('GAME_ID',),
                                                           where_clause=('(HOME_ID = ' +
                                                                         str(player_stat['TEAM_ID']) +
                                                                         ' OR AWAY_ID = ' +
                                                                         str(player_stat['TEAM_ID']) +
                                                                         ') AND DATE_TIME < \'' +
                                                                         current_game['DATE_TIME'].strftime(
                                                                             '%Y-%m-%d %H:%M:%S') +
                                                                         '\' AND SEASON_ID = ' +
                                                                         str(current_game['SEASON_ID']),))
        length = len(previous_games_for_player)
        if length > 0:
            avg_toi = 0.0
            avg_assists = 0.0
            avg_goals = 0.0
            avg_shots = 0.0
            avg_hits = 0.0
            avg_pp_goals = 0.0
            avg_pp_assists = 0.0
            avg_pim = 0.0
            avg_faceoff_wins = 0.0
            avg_faceoff_taken = 0.0
            avg_takeaways = 0.0
            avg_giveaways = 0.0
            avg_sh_goals = 0.0
            avg_sh_assists = 0.0
            avg_blocked = 0.0
            avg_plus_minus = 0.0
            avg_even_toi = 0.0
            avg_pp_toi = 0.0
            avg_sh_toi = 0.0
            for previous_game in previous_games_for_player:
                prev = self.db_manager.select('WEB_NHL_PLAYER_STATS',
                                              where_clause=('PLAYER_ID = ' +
                                                            str(player_stat['PLAYER_ID']) +
                                                            ' && GAME_ID = ' +
                                                            str(previous_game['GAME_ID']),),
                                              single=True)
                if prev is not None:
                    avg_toi += prev['TOI']
                    avg_assists += prev['ASSISTS']
                    avg_goals += prev['GOALS']
                    avg_shots += prev['SHOTS']
                    avg_hits += prev['HITS']
                    avg_pp_goals += prev['PP_GOALS']
                    avg_pp_assists += prev['PP_ASSISTS']
                    avg_pim += prev['PIM']
                    avg_faceoff_wins += prev['FACEOFF_WINS']
                    avg_faceoff_taken += prev['FACEOFF_TAKEN']
                    avg_takeaways += prev['TAKEAWAYS']
                    avg_giveaways += prev['GIVEAWAYS']
                    avg_sh_goals += prev['SH_GOALS']
                    avg_sh_assists += prev['SH_ASSISTS']
                    avg_blocked += prev['BLOCKED_SHOTS']
                    avg_plus_minus += prev['PLUS_MINUS']
                    avg_even_toi += prev['EVEN_TOI']
                    avg_pp_toi += prev['PP_TOI']
                    avg_sh_toi += prev['SH_TOI']

            if avg_faceoff_taken > 0:
                avg_faceoff_pct = float(avg_faceoff_wins) / float(avg_faceoff_taken)
            else:
                avg_faceoff_pct = None

            avg_toi /= float(length)
            avg_assists /= float(length)
            avg_goals /= float(length)
            avg_shots /= float(length)
            avg_hits /= float(length)
            avg_pp_goals /= float(length)
            avg_pp_assists /= float(length)
            avg_pim /= float(length)
            avg_faceoff_wins /= float(length)
            avg_faceoff_taken /= float(length)
            avg_takeaways /= float(length)
            avg_giveaways /= float(length)
            avg_sh_goals /= float(length)
            avg_sh_assists /= float(length)
            avg_blocked /= float(length)
            avg_plus_minus /= float(length)
            avg_even_toi /= float(length)
            avg_pp_toi /= float(length)
            avg_sh_toi /= float(length)

            insert_avg_player_stats_values.append((
                player_stat['PLAYER_ID'],
                player_stat['GAME_ID'],
                current_game['SEASON_ID'],
                player_stat['TEAM_ID'],
                avg_toi,
                avg_assists,
                avg_goals,
                avg_shots,
                avg_hits,
                avg_pp_goals,
                avg_pp_assists,
                avg_pim,
                avg_faceoff_pct,
                avg_faceoff_wins,
                avg_faceoff_taken,
                avg_takeaways,
                avg_giveaways,
                avg_sh_goals,
                avg_sh_assists,
                avg_blocked,
                avg_plus_minus,
                avg_even_toi,
                avg_pp_toi,
                avg_sh_toi
            ))

    def calculate_single_game_average_goalie_stats(self, goalie_stat, insert_avg_goalie_stats_values):
        current_game = self.db_manager.select('WEB_NHL_GAME',
                                              where_clause=('GAME_ID = ' + str(goalie_stat['GAME_ID']),),
                                              single=True)
        previous_games_for_goalie = self.db_manager.select('WEB_NHL_GAME',
                                                           cols=('GAME_ID',),
                                                           where_clause=('(HOME_ID = ' +
                                                                         str(goalie_stat['TEAM_ID']) +
                                                                         ' OR AWAY_ID = ' +
                                                                         str(goalie_stat['TEAM_ID']) +
                                                                         ') AND DATE_TIME < \'' +
                                                                         current_game['DATE_TIME'].strftime(
                                                                             '%Y-%m-%d %H:%M:%S') +
                                                                         '\' AND SEASON_ID = ' +
                                                                         str(current_game['SEASON_ID']),))
        length = len(previous_games_for_goalie)
        if length > 0:
            avg_toi = 0.0
            avg_shots_against = 0.0
            avg_saves = 0.0
            avg_pp_saves = 0.0
            avg_sh_saves = 0.0
            avg_even_saves = 0.0
            avg_pp_shots_against = 0.0
            avg_sh_shots_against = 0.0
            avg_even_shots_against = 0.0
            avg_win = 0.0
            avg_shutout = 0.0
            avg_goals_against = 0.0
            for previous_game in previous_games_for_goalie:
                prev = self.db_manager.select('WEB_NHL_GOALIE_STATS',
                                              where_clause=('GOALIE_ID = ' +
                                                            str(goalie_stat['GOALIE_ID']) +
                                                            ' && GAME_ID = ' +
                                                            str(previous_game['GAME_ID']),),
                                              single=True)
                if prev is not None:
                    avg_toi += prev['TOI']
                    avg_shots_against += prev['SHOTS_AGAINST']
                    avg_saves += prev['SAVES']
                    avg_pp_saves += prev['PP_SAVES']
                    avg_sh_saves += prev['SH_SAVES']
                    avg_even_saves += prev['EVEN_SAVES']
                    avg_pp_shots_against += prev['PP_SHOTS_AGAINST']
                    avg_sh_shots_against += prev['SH_SHOTS_AGAINST']
                    avg_even_shots_against += prev['EVEN_SHOTS_AGAINST']
                    if prev['DECISION'] == 'W':
                        avg_win += 1
                    if prev['SAVES'] == prev['SHOTS_AGAINST'] and prev['SAVES'] > 0:
                        avg_shutout += 1
                    avg_goals_against += prev['SHOTS_AGAINST'] - prev['SAVES']

            if avg_shots_against > 0:
                avg_save_percent = float(avg_saves) / float(avg_shots_against)
            else:
                avg_save_percent = None

            if avg_pp_shots_against > 0:
                avg_pp_save_percent = float(avg_pp_saves) / float(avg_pp_shots_against)
            else:
                avg_pp_save_percent = None

            if avg_sh_shots_against > 0:
                avg_sh_save_percent = float(avg_sh_saves) / float(avg_sh_shots_against)
            else:
                avg_sh_save_percent = None

            if avg_even_shots_against > 0:
                avg_even_save_percent = float(avg_even_saves) / float(avg_even_shots_against)
            else:
                avg_even_save_percent = None

            avg_toi /= float(length)
            avg_shots_against /= float(length)
            avg_saves /= float(length)
            avg_pp_saves /= float(length)
            avg_sh_saves /= float(length)
            avg_even_saves /= float(length)
            avg_pp_shots_against /= float(length)
            avg_sh_shots_against /= float(length)
            avg_even_shots_against /= float(length)
            avg_win /= float(length)
            avg_shutout /= float(length)
            avg_goals_against /= float(length)

            insert_avg_goalie_stats_values.append((
                goalie_stat['GOALIE_ID'],
                goalie_stat['GAME_ID'],
                current_game['SEASON_ID'],
                goalie_stat['TEAM_ID'],
                avg_toi,
                avg_shots_against,
                avg_saves,
                avg_pp_saves,
                avg_sh_saves,
                avg_even_saves,
                avg_pp_shots_against,
                avg_sh_shots_against,
                avg_even_shots_against,
                avg_win,
                avg_save_percent,
                avg_pp_save_percent,
                avg_sh_save_percent,
                avg_even_save_percent,
                avg_shutout,
                avg_goals_against
            ))

    def transfer_active_status(self):
        tournament_id = self.db_manager.select('WEB_FD_TOURNAMENT',
                                               cols=('TOURNAMENT_ID',),
                                               where_clause='DATE = \'' +
                                                            datetime.now().strftime('%Y-%m-%d') +
                                                            '\'',
                                               single=True)

        player_ids = self.db_manager.select('WEB_FD_TOURNAMENT_DATA',
                                            cols=('PLAYER_ID',),
                                            where_clause='TOURNAMENT_ID = ' + str(tournament_id['TOURNAMENT_ID']))

        player_names = []

        for player_id in player_ids:
            db_player = self.db_manager.select('WEB_FD_PLAYER',
                                                 cols=('PLAYER_ID', 'FULL_NAME'),
                                                 where_clause='PLAYER_ID = ' + str(player_id['PLAYER_ID']),
                                                 single=True)

            player = {'PLAYER_ID': db_player['PLAYER_ID'],
                      'FULL_NAME': db_player['FULL_NAME']}

            player_names.append(player)

        active_data_values = self.web_data.get_active_statuses(player_names)

        projected_data = self.db_manager.select('WEB_FD_PROJECTED_DATA',
                                                where_clause='TOURNAMENT_ID = ' +
                                                             str(tournament_id['TOURNAMENT_ID']))

        projected_data_exists = len(projected_data) > 0

        insert_projected_values = []
        if projected_data_exists:
            for player_id in active_data_values:
                print('UPDATE')
        else:
            for player_id in active_data_values:
                if 'Projected goalie' in active_data_values[player_id]:
                    insert_projected_values.append((
                        tournament_id['TOURNAMENT_ID'],
                        player_id,
                        True,
                        None
                    ))
                elif 'Projected line' in active_data_values[player_id]:
                    insert_projected_values.append((
                        tournament_id['TOURNAMENT_ID'],
                        player_id,
                        None,
                        active_data_values[player_id][14]
                    ))
                else:
                    insert_projected_values.append((
                        tournament_id['TOURNAMENT_ID'],
                        player_id,
                        None,
                        None
                    ))

        if len(insert_projected_values) > 0:
            self.db_manager.insert('WEB_FD_PROJECTED_DATA',
                                   insert_projected_values)

    def prepare_lp_values(self):
        test_date_string = '2019-01-14'
        test_date = datetime(2019, 1, 14)
        current_tournament = self.db_manager.select('WEB_FD_TOURNAMENT',
                                                    where_clause='DATE = \'' +
                                                                 # datetime.now().strftime('%Y-%m-%d') +
                                                                 test_date_string +
                                                                 '\'',
                                                    single=True)

        projected_players = self.db_manager.select('WEB_FD_PROJECTED_DATA',
                                                   where_clause='TOURNAMENT_ID = ' +
                                                                 str(current_tournament['TOURNAMENT_ID']))

        active_season = self.db_manager.select('WEB_NHL_SEASON',
                                               where_clause='IS_ACTIVE = 1',
                                               single=True)

        lp_variables = []

        insert_sol_lp_values = []

        for projected_player in projected_players:

            if projected_player['PROJECTED_LINE'] is not None:
                current_tournament_data = self.db_manager.select('WEB_FD_TOURNAMENT_DATA',
                                                                 where_clause=('TOURNAMENT_ID = ' +
                                                                               str(current_tournament['TOURNAMENT_ID']),
                                                                               'PLAYER_ID = ' +
                                                                               str(projected_player['PLAYER_ID'])),
                                                                 single=True)

                player_salary = current_tournament_data['SALARY']

                player_team = current_tournament_data['TEAM']

                nhl_team_id = self.db_manager.select('WEB_CONNECT_TEAM',
                                                     where_clause='FD_TEAM = \'' + player_team + '\'',
                                                     single=True)

                nhl_player_id = self.db_manager.select('WEB_CONNECT_PLAYER',
                                                       where_clause='FD_PLAYER_ID = ' +
                                                                    str(projected_player['PLAYER_ID']),
                                                       single=True)

                nhl_season_games = self.db_manager.select('WEB_NHL_GAME',
                                                          where_clause='SEASON_ID = ' + str(active_season['SEASON_ID']))

                nhl_game_id = None

                for nhl_game in nhl_season_games:
                    # if nhl_game['DATE_TIME'].date() == datetime.now().date() and \
                    if nhl_game['DATE_TIME'].date() == test_date.date() and \
                       (nhl_game['HOME_ID'] == nhl_team_id['NHL_TEAM_ID'] or
                            nhl_game['AWAY_ID'] == nhl_team_id['NHL_TEAM_ID']):
                        nhl_game_id = nhl_game['GAME_ID']
                        break

                if nhl_game_id is not None:
                    nhl_pred_values = self.db_manager.select('PRED_PLAYER_VALUES',
                                                             where_clause=('PLAYER_ID = ' +
                                                                           str(nhl_player_id['NHL_PLAYER_ID']),
                                                                           'GAME_ID = ' + str(nhl_game_id)),
                                                             single=True)

                    if nhl_pred_values is None:
                        player_stat = {
                            'PLAYER_ID': nhl_player_id['NHL_PLAYER_ID'],
                            'GAME_ID': nhl_game_id,
                            'TEAM_ID': nhl_team_id['NHL_TEAM_ID']
                        }

                        insert_avg_player_stats_values = []
                        insert_pred_player_values = []

                        self.calculate_single_game_average_player_stats(player_stat,
                                                                        insert_avg_player_stats_values)

                        self.db_manager.insert('AVG_PLAYER_STATS',
                                               insert_avg_player_stats_values)

                        for insert_avg_player_stat in insert_avg_player_stats_values:
                            player_stat = self.db_manager.select('AVG_PLAYER_STATS',
                                                                 where_clause=(
                                                                 'PLAYER_ID = ' + str(insert_avg_player_stat[0]),
                                                                 'GAME_ID = ' + str(insert_avg_player_stat[1])),
                                                                 single=True)
                            insert_pred_player_values.append((
                                player_stat['PLAYER_ID'],
                                player_stat['GAME_ID'],
                                active_season['SEASON_ID'],
                                player_stat['GOALS'],
                                player_stat['ASSISTS'],
                                player_stat['SHOTS'],
                                player_stat['PP_GOALS'] + player_stat['PP_ASSISTS'],
                                player_stat['SH_GOALS'] + player_stat['SH_ASSISTS'],
                                player_stat['BLOCKED_SHOTS'],
                                None
                            ))

                        self.db_manager.insert('PRED_PLAYER_VALUES',
                                               insert_pred_player_values)

                        nhl_pred_values = self.db_manager.select('PRED_PLAYER_VALUES',
                                                                 where_clause=('PLAYER_ID = ' +
                                                                               str(nhl_player_id['NHL_PLAYER_ID']),
                                                                               'GAME_ID = ' + str(nhl_game_id)),
                                                                 single=True)

                    nhl_pred_score = 12 * float(nhl_pred_values['GOALS']) +\
                                     8 * float(nhl_pred_values['ASSISTS']) +\
                                     1.6 * float(nhl_pred_values['SHOTS']) +\
                                     2 * float(nhl_pred_values['SH_POINTS']) +\
                                     0.5 * float(nhl_pred_values['PP_POINTS']) +\
                                     1.6 * float(nhl_pred_values['BLOCKED_SHOTS'])

                    lp_variables.append({
                        'FD_PLAYER_ID': projected_player['PLAYER_ID'],
                        'NHL_PLAYER_ID': nhl_player_id['NHL_PLAYER_ID'],
                        'PREDICTION': nhl_pred_score,
                        'POSITION': current_tournament_data['POSITION'],
                        'SALARY': player_salary
                    })
            elif projected_player['PROJECTED_GOALIE'] is not None:
                current_tournament_data = self.db_manager.select('WEB_FD_TOURNAMENT_DATA',
                                                                 where_clause=('TOURNAMENT_ID = ' +
                                                                               str(current_tournament['TOURNAMENT_ID']),
                                                                               'PLAYER_ID = ' +
                                                                               str(projected_player['PLAYER_ID'])),
                                                                 single=True)

                player_salary = current_tournament_data['SALARY']

                player_team = current_tournament_data['TEAM']

                nhl_team_id = self.db_manager.select('WEB_CONNECT_TEAM',
                                                     where_clause='FD_TEAM = \'' + player_team + '\'',
                                                     single=True)

                nhl_goalie_id = self.db_manager.select('WEB_CONNECT_PLAYER',
                                                       where_clause='FD_PLAYER_ID = ' +
                                                                    str(projected_player['PLAYER_ID']),
                                                       single=True)

                nhl_season_games = self.db_manager.select('WEB_NHL_GAME',
                                                          where_clause='SEASON_ID = ' + str(active_season['SEASON_ID']))

                nhl_game_id = None

                if nhl_goalie_id['NHL_GOALIE_ID'] is None:
                    print("IN")

                for nhl_game in nhl_season_games:
                    # if nhl_game['DATE_TIME'].date() == datetime.now().date() and \
                    if nhl_game['DATE_TIME'].date() == test_date.date() and \
                       (nhl_game['HOME_ID'] == nhl_team_id['NHL_TEAM_ID'] or
                            nhl_game['AWAY_ID'] == nhl_team_id['NHL_TEAM_ID']):
                        nhl_game_id = nhl_game['GAME_ID']
                        break

                if nhl_game_id is not None:
                    nhl_pred_values = self.db_manager.select('PRED_GOALIE_VALUES',
                                                             where_clause=('GOALIE_ID = ' +
                                                                           str(nhl_goalie_id['NHL_GOALIE_ID']),
                                                                           'GAME_ID = ' + str(nhl_game_id)),
                                                             single=True)

                    if nhl_pred_values is None:
                        goalie_stat = {
                            'GOALIE_ID': nhl_goalie_id['NHL_GOALIE_ID'],
                            'GAME_ID': nhl_game_id,
                            'TEAM_ID': nhl_team_id['NHL_TEAM_ID']
                        }

                        insert_avg_goalie_stats_values = []
                        insert_pred_goalie_values = []

                        self.calculate_single_game_average_goalie_stats(goalie_stat,
                                                                        insert_avg_goalie_stats_values)

                        self.db_manager.insert('AVG_GOALIE_STATS',
                                               insert_avg_goalie_stats_values)

                        for insert_avg_goalie_stat in insert_avg_goalie_stats_values:
                            goalie_stat = self.db_manager.select('AVG_GOALIE_STATS',
                                                                 where_clause=(
                                                                 'GOALIE_ID = ' + str(insert_avg_goalie_stat[0]),
                                                                 'GAME_ID = ' + str(insert_avg_goalie_stat[1])),
                                                                 single=True)
                            insert_pred_goalie_values.append((
                                goalie_stat['GOALIE_ID'],
                                goalie_stat['GAME_ID'],
                                active_season['SEASON_ID'],
                                goalie_stat['WIN'],
                                goalie_stat['GOALS_AGAINST'],
                                goalie_stat['SAVES'],
                                goalie_stat['SHUTOUT'],
                                None
                            ))

                        self.db_manager.insert('PRED_GOALIE_VALUES',
                                               insert_pred_goalie_values)

                        nhl_pred_values = self.db_manager.select('PRED_GOALIE_VALUES',
                                                                 where_clause=('GOALIE_ID = ' +
                                                                               str(nhl_goalie_id['NHL_GOALIE_ID']),
                                                                               'GAME_ID = ' + str(nhl_game_id)),
                                                                 single=True)

                    nhl_pred_score = 12 * float(nhl_pred_values['WIN']) + \
                                     0.8 * float(nhl_pred_values['SAVES']) + \
                                     8 * float(nhl_pred_values['SHUTOUT']) - \
                                     4 * float(nhl_pred_values['GOALS_AGAINST'])

                    lp_variables.append({
                        'FD_PLAYER_ID': projected_player['PLAYER_ID'],
                        'NHL_PLAYER_ID': nhl_goalie_id['NHL_GOALIE_ID'],
                        'PREDICTION': nhl_pred_score,
                        'POSITION': current_tournament_data['POSITION'],
                        'SALARY': player_salary
                    })

        for lp_variable in lp_variables:
            insert_sol_lp_values.append((
                lp_variable['FD_PLAYER_ID'],
                lp_variable['POSITION'],
                lp_variable['PREDICTION'],
                lp_variable['SALARY'] / 100,
                lp_variable['PREDICTION'] / (lp_variable['SALARY'] / 100)
            ))

        self.db_manager.delete('SOL_LP_VALUES', ())

        self.db_manager.insert('SOL_LP_VALUES', insert_sol_lp_values)

    def execute_lp_solver(self):

        db_centers = self.db_manager.select('SOL_LP_VALUES',
                                            where_clause='POSITION = \'C\'')

        # db_centers = sorted(db_centers, key=lambda k: k['SALARY_100S'], reverse=True)
        # db_centers = sorted(db_centers, key=lambda k: k['POINTS_PER_DOLLAR'], reverse=True)
        db_centers = sorted(db_centers, key=lambda k: k['SALARY_100S'])

        db_wingers = self.db_manager.select('SOL_LP_VALUES',
                                            where_clause='POSITION = \'W\'')

        db_wingers = sorted(db_wingers, key=lambda k: k['SALARY_100S'])

        db_defense = self.db_manager.select('SOL_LP_VALUES',
                                            where_clause='POSITION = \'D\'')

        db_defense = sorted(db_defense, key=lambda k: k['SALARY_100S'])

        db_goalies = self.db_manager.select('SOL_LP_VALUES',
                                            where_clause='POSITION = \'G\'')

        db_goalies = sorted(db_goalies, key=lambda k: k['SALARY_100S'])

        CENTERS = 2
        WINGERS = 4
        DEFENSE = 2
        GOALIES = 1

        objective_values = []
        restrictions = {}
        salary_values = []
        center_values = []
        winger_values = []
        defense_values = []
        goalie_values = []

        restrictions['C'] = (2, [])
        restrictions['W'] = (4, [])
        restrictions['D'] = (2, [])
        restrictions['G'] = (1, [])

        for copies in range(3):
            for center_count in range(CENTERS):
                center = db_centers[center_count * copies]
                restrictions['C'][1].append(len(objective_values))
                objective_values.append(center['PREDICTED_VALUE'])
                salary_values.append(center['SALARY_100S'])
                center_values.append(1)
                winger_values.append(0)
                defense_values.append(0)
                goalie_values.append(0)
            for winger_count in range(WINGERS):
                winger = db_wingers[winger_count * copies]
                restrictions['W'][1].append(len(objective_values))
                objective_values.append(winger['PREDICTED_VALUE'])
                salary_values.append(winger['SALARY_100S'])
                center_values.append(0)
                winger_values.append(1)
                defense_values.append(0)
                goalie_values.append(0)
            for defense_count in range(DEFENSE):
                defense_player = db_defense[defense_count * copies]
                restrictions['D'][1].append(len(objective_values))
                objective_values.append(defense_player['PREDICTED_VALUE'])
                salary_values.append(defense_player['SALARY_100S'])
                center_values.append(0)
                winger_values.append(0)
                defense_values.append(1)
                goalie_values.append(0)
            for goalie_count in range(GOALIES):
                goalie = db_goalies[goalie_count * copies]
                restrictions['G'][1].append(len(objective_values))
                objective_values.append(goalie['PREDICTED_VALUE'])
                salary_values.append(goalie['SALARY_100S'])
                center_values.append(0)
                winger_values.append(0)
                defense_values.append(0)
                goalie_values.append(1)

        # for lp_variable in lp_variables:
        #     objective_values.append(lp_variable['PREDICTION'])
        #     salary_values.append(lp_variable['SALARY'])
        #     if lp_variable['POSITION'] == 'C':
        #         center_values.append(1)
        #         winger_values.append(0)
        #         defense_values.append(0)
        #         goalie_values.append(0)
        #     elif lp_variable['POSITION'] == 'W':
        #         center_values.append(0)
        #         winger_values.append(1)
        #         defense_values.append(0)
        #         goalie_values.append(0)
        #     elif lp_variable['POSITION'] == 'D':
        #         center_values.append(0)
        #         winger_values.append(0)
        #         defense_values.append(1)
        #         goalie_values.append(0)
        #     elif lp_variable['POSITION'] == 'G':
        #         center_values.append(0)
        #         winger_values.append(0)
        #         defense_values.append(0)
        #         goalie_values.append(1)

        self.ip_solver.set_objective(objective_values, restrictions)
        self.ip_solver.add_constraint(salary_values, 550)
        self.ip_solver.add_constraint(center_values, 2, True)
        self.ip_solver.add_constraint(winger_values, 4, True)
        self.ip_solver.add_constraint(defense_values, 2, True)
        self.ip_solver.add_constraint(goalie_values, 1, True)
        # salary_values.sort()
        solution = self.ip_solver.solve()

        # for i in range(len(lp_variables)):
        #     if solution[i] == 1:
        #         print(lp_variables[i]['FD_PLAYER_ID'])

        self.logger.info("DONE")

    def predict_lineup(self):

        self.prepare_lp_values()

        self.execute_lp_solver()

    def get_lp_solution_test(self):
        self.ip_solver.set_objective((-8, -2, -4, -7, -5))
        self.ip_solver.add_constraint((-3, -3, 1, 2, 3), -2, True)
        self.ip_solver.add_constraint((-5, -3, -2, -1, 1), -4)
        solution = self.ip_solver.solve()

        for solution_index, solution_value in solution.items():
            if solution_index != "P":
                self.logger.info("x[" + solution_index + "]: " + str(round(solution_value, 2)))
            else:
                self.logger.info(solution_index + ": " + str(round(solution_value, 2)))

import shutil
import os
import logging
import csv


class FileManager:

    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self.salary_info = {}
        self.download_source = "REDACTED DOWNLOAD SOURCE"
        self.download_destination = "REDACTED DOWNLOAD DESTINATION"
        self.parsed_destination = "REDACTED PARSED DESTINATION"

    def transfer_file(self, file_name):
        source = self.download_source
        source += file_name
        destination = self.download_destination
        destination += file_name
        self.logger.info("Source: " + source)
        self.logger.info("Destination: " + destination)
        shutil.move(source, destination)
        self.logger.info("File transferred")

    def transfer_all_files(self):
        files_list = os.listdir(self.download_source)
        for file in files_list:
            if "FanDuel-NHL-" in file and "-players-list.csv" in file:
                self.transfer_file(file)

    def parse_file(self, folder, file_name):
        file_path = folder + file_name
        self.logger.info("Parse: " + file_path)
        file = open(file_path)
        reader = list(csv.reader(file))
        row_count = 0
        headers = reader[0]
        player_info = {}
        for row in reader:
            if row_count != 0:
                player = {}
                col_count = 0
                for col in row:
                    player[headers[col_count]] = col
                    col_count += 1
                self.logger.info("Player Salary Parsed")
                player_info[player[headers[0]].split("-")[1]] = player
            row_count += 1
        tournament_id = reader[1][0].split("-")[0]
        self.salary_info[tournament_id] = {}
        self.salary_info[tournament_id]['DATE_TIME'] = file_name[12:22]
        self.salary_info[tournament_id]['FILE_NAME'] = file_name
        self.salary_info[tournament_id]['PLAYERS'] = player_info

    def parse_all_files(self):
        self.salary_info = {}
        files_list = os.listdir(self.download_destination)
        for file in files_list:
            if "FanDuel-NHL-" in file and "-players-list.csv" in file:
                self.parse_file(self.download_destination, file)
        return self.salary_info

    def archive_file(self, file_name):
        source = self.download_destination
        source += file_name
        destination = self.parsed_destination
        destination += file_name
        self.logger.info("Source: " + source)
        self.logger.info("Destination: " + destination)
        shutil.move(source, destination)
        self.logger.info("File transferred")

    def archive_all_files(self):
        files_list = os.listdir(self.download_destination)
        for file in files_list:
            if "FanDuel-NHL-" in file and "-players-list.csv" in file:
                self.archive_file(file)

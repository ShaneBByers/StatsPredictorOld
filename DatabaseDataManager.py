from DatabaseConnector import DatabaseConnector
import logging


class DatabaseDataManager:

    def __init__(self, logger_name):
        self.connector = DatabaseConnector(logger_name)
        self.insert_statement = ""
        self.update_statement = ""
        self.logger = logging.getLogger(logger_name)
        # logger.info("Database Data Executor created")

    def insert(self, table, values, commit=True):
        if isinstance(values, (list, tuple)):
            for single_value_set in values:
                self.connector.create_insert(table, single_value_set)
                self.connector.execute_insert(False)
            if commit:
                self.connector.commit_execute()
        else:
            self.connector.create_insert(table, values)
            self.connector.execute_insert()

    def select(self, table, cols=(), where_clause=(), single=False):
        self.connector.create_select(table, cols, where_clause)
        return self.connector.execute_select(single)

    def update(self, table, clauses=(), commit=True):
        for clause in clauses:
            self.connector.create_update(table, clause['COL_VALS'], clause['WHERE_CLAUSE'])
            self.connector.execute_update(False)
        if commit:
            self.connector.commit_execute()

    def delete(self, table, where_clause, commit=True):
        self.connector.create_delete(table, where_clause)
        return self.connector.execute_delete()

    def commit(self):
        self.connector.commit_execute()

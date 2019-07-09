import mysql.connector
import logging


class DatabaseConnector:

    def __init__(self, logger_name):
        self.database = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="password",
            database="PREDICTOR"
        )
        self.logger = logging.getLogger(logger_name)
        self.cursor = self.database.cursor(dictionary=True)
        # self.statement = ""
        self.select_statement = ""
        self.insert_statement = ""
        self.update_statement = ""
        self.delete_statement = ""
        # logging.info("DatabaseConnector created")

    def create_select(self, table, cols=(), where_clause=()):
        self.select_statement = "SELECT "
        if len(cols) == 0:
            self.select_statement += "* "
        else:
            for col in cols[:-1]:
                self.select_statement += col + ", "
            else:
                self.select_statement += cols[-1]
            self.select_statement += " "
        self.select_statement += "FROM "
        self.select_statement += table
        if isinstance(where_clause, str):
            self.select_statement += " WHERE "
            self.select_statement += where_clause
        elif len(where_clause) != 0:
            self.select_statement += " WHERE ("
            for where in where_clause[:-1]:
                self.select_statement += where + " AND "
            else:
                self.select_statement += where_clause[-1]
            self.select_statement += ")"
        self.select_statement += ";"
        self.logger.info("Created statement: '" + self.select_statement + "'")

    def execute_select(self, single=False):
        self.cursor.execute(self.select_statement)
        if single:
            select_dict = self.cursor.fetchone()
        else:
            select_dict = self.cursor.fetchall()
        if select_dict is None:
            self.logger.info("Executed statement: '" + self.select_statement + "' NO ROWS SELECTED")
        else:
            self.logger.info("Executed statement: '" + self.select_statement + "' Rows: " + str(len(select_dict)))
        return select_dict

    def create_insert(self, table, values):
        self.insert_statement = "INSERT INTO "
        self.insert_statement += table
        self.insert_statement += " VALUES ("
        for value in values[:-1]:
            if value is None:
                self.insert_statement += "NULL, "
            else:
                if isinstance(value, str):
                    self.insert_statement += "\"" + value + "\", "
                else:
                    self.insert_statement += str(value) + ", "
        else:
            if values[-1] is None:
                self.insert_statement += "NULL"
            else:
                if isinstance(values[-1], str):
                    self.insert_statement += "\"" + values[-1] + "\""
                else:
                    self.insert_statement += str(values[-1])
        self.insert_statement += ");"
        self.logger.info("Created statement: '" + self.insert_statement + "'")

    def execute_insert(self, commit=True):
        row_count = self.cursor.execute(self.insert_statement)
        self.logger.info("Executed statement: '" + self.insert_statement + "' Row Count: " + str(row_count))
        if commit:
            self.commit_execute()
        return row_count

    def create_update(self, table, col_vals, where_clause):
        self.update_statement = "UPDATE "
        self.update_statement += table
        self.update_statement += " SET "
        last = None
        for col in col_vals:
            if last is None:
                last = col
            else:
                value = col_vals[col]
                self.update_statement += col
                self.update_statement += " = "
                if value is None:
                    self.update_statement += "NULL, "
                else:
                    if isinstance(value, str):
                        self.update_statement += "\"" + value + "\", "
                    else:
                        self.update_statement += str(value) + ", "
        if last is not None:
            value = col_vals[last]
            self.update_statement += last
            self.update_statement += " = "
            if value is None:
                self.update_statement += "NULL"
            else:
                if isinstance(value, str):
                    self.update_statement += "\"" + value + "\""
                else:
                    self.update_statement += str(value)

        if isinstance(where_clause, str):
            self.update_statement += " WHERE "
            self.update_statement += where_clause
        elif len(where_clause) != 0:
            self.update_statement += " WHERE ("
            for where in where_clause[:-1]:
                self.update_statement += where + " AND "
            else:
                self.update_statement += where_clause[-1]
            self.update_statement += ")"
        self.update_statement += ";"
        self.logger.info("Created statement: '" + self.update_statement + "'")

    def execute_update(self, commit=True):
        row_count = self.cursor.execute(self.update_statement)
        self.logger.info("Executed statement: '" + self.update_statement + "' Row Count: " + str(row_count))
        if commit:
            self.commit_execute()
        return row_count

    def create_delete(self, table, where_clause):
        self.delete_statement = "DELETE FROM "
        self.delete_statement += table
        if isinstance(where_clause, str):
            self.delete_statement += " WHERE "
            self.delete_statement += where_clause
        elif len(where_clause) != 0:
            self.delete_statement += "WHERE ("
            for where in where_clause[:-1]:
                self.delete_statement += where + " AND "
            else:
                self.delete_statement += where_clause[-1]
            self.delete_statement += ")"
        self.delete_statement += ";"
        self.logger.info("Created statement: '" + self.delete_statement + "'")

    def execute_delete(self, commit=True):
        row_count = self.cursor.execute(self.delete_statement)
        self.logger.info("Executed statement: '" + self.delete_statement + "' Row Count: " + str(row_count))
        if commit:
            self.commit_execute()
        return row_count

    def commit_execute(self):
        self.database.commit()
        self.logger.info("Committed execution.")

import mysql.connector
import os


class DatabaseConnection:
    def __init__(self):
        self.cnx = None
        self.wcnx = None

    def connect(self):
        # Read connection (read replica / normal read)
        self.cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Alina123@",
            database="voxship",
            autocommit=True,
        )

        # Write connection
        self.wcnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Alina123@",
            database="voxship",
            autocommit=True,
        )

        return self.cnx, self.wcnx

    def close(self):
        if self.cnx and self.cnx.is_connected():
            self.cnx.close()

        if self.wcnx and self.wcnx.is_connected():
            self.wcnx.close()

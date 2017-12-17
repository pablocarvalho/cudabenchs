import sqlite3

class DBConnection:
    def __init__(self):
        self.connection = sqlite3.connect('kernels.db')
        self.connection.row_factory = sqlite3.Row
        self.cursor = None

    def __del__(self):
        self.connection.close()
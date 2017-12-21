import sqlite3

class DBConnection:
    def __init__(self,db=None):

        if db is None:
            db = 'kernels.db'

        try:
            self.connection = sqlite3.connect(db)
        except sqlite3.Error:
            print "Error opening db: " + db
            raise

        self.connection.row_factory = sqlite3.Row
        self.cursor = None

    def __del__(self):
        self.connection.close()
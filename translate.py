#!/usr/bin/env python

from db import DBConnection
import cxxfilt

class NameTranslator(DBConnection):
    def run(self):
        self.cursor = self.connection.cursor()
        self.cursor.execute("SELECT mangledName from Kernels;")

        for row in self.cursor.fetchall():
            self.cursor = self.connection.cursor()
            self.cursor.execute("UPDATE Kernels SET signature=? WHERE mangledName=?;",
                                (cxxfilt.demangle(row['mangledName']), row['mangledName']))
            self.connection.commit()

translate = NameTranslator()
translate.run()
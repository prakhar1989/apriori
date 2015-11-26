import sqlite3
import csv
import os

def readFile(filename):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    with open(filename) as f:
        reader = csv.reader(f, delimiter=',')
        for (index, row) in enumerate(reader):
            if index:
                addInTable(row, c)
    conn.commit()
    conn.close()

def addInTable(row, c):
    truncated_row = filter(len, row)
    if len(truncated_row) == 6:
        c.execute('INSERT INTO school VALUES (?, ?, ?, ?, ?, ?)', truncated_row)

def setup():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE school
                (dbn TEXT, peer_index REAL, overall_grade TEXT,
                 env_grade TEXT, perf_grade TEXT, progress_grade TEXT)''')
    conn.commit()
    conn.close()

def createDatabase(filename):
    try:
        setup()
    except sqlite3.OperationalError:
        os.remove('data.db')
        setup()
        readFile(filename)

if __name__ == '__main__':
    createDatabase('INTEGRATED-DATASET.csv')

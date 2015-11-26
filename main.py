import sqlite3
categories = {
    'env_grade': set([x + '2' for x in "ABCDE"]),
    'perf_grade': set([x + '3' for x in "ABCDE"]),
    'progress_grade': set([x + '4' for x in "ABCDE"])
}
print categories

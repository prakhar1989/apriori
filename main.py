"""
from collections import Counter
import csv

data = open('data.csv').read()
c = Counter([w for x in data.split() for w in x.split(',')])

"""

def getChar(n):
    if n < 1 or n > 26:
        return None
    return chr(n - 65)

#print getChar(12)#

"""

- Use the amazon sqs service to create a processing queue for the tweets that are delviered by the twtiter streaming API

- Use the amazon sns service to update the status processing on each tweet so the UI can refresh

- integrate a third party cloud service api into twitter processing workflow

"""
class Numbers(object):
    def __init__(self, start=0, end=10):
        self.start = start
        self.end = end

    def __iter__(self):
        return self

    def next(self):
        tmp = self.start
        if self.start == self.end:
            raise StopIteration
        self.start += 1
        return tmp


#n = Numbers(start=1000, end=1035)
def myRange(start, end):
    if start >= end:
        return
    i = start
    while i < end:
        yield i
        i += 1


for i in myRange(10, 14):
    print i

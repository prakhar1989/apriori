#!/usr/bin/python

import sqlite3
from itertools import combinations
import operator
from tabulate import tabulate
import sys
from contextlib import contextmanager
from dataloader import createDatabase

### Helpful decorator to redirect output to file
@contextmanager
def stdout_redirected(new_stdout):
    save_stdout = sys.stdout
    sys.stdout = new_stdout
    try:
        yield None
    finally:
        sys.stdout = save_stdout

class Apriori(object):
    """ The Apriori class that implements the apiori algorithm for
    mining association rules from a database
    """
    def __init__(self, dbfile, dbname, categories, threshold, confidence):
        self.conn = sqlite3.connect(dbfile)
        self.dbname = dbname
        self.columns = categories
        self.categoryMap, self.valueMap = self.initMapping()
        self.threshold = threshold
        self.frequentSets = {}
        self.totalSize = self.runFetchOne("select count(*) from %s" % self.dbname)[0]
        self.support = int(threshold * self.totalSize)
        self.confidence = confidence
        self.assocrules = []

    def initMapping(self):
        # builds the mapping of fields to columns and vice-versa to help with faster
        # lookups in the rest of the algorithm
        categoryMap, valueMap = {}, {}
        for col in self.columns:
            values = self.runFetchAll("select distinct %s from %s" % (col, self.dbname))
            categoryMap[col] = set([x[0] for x in values])
        for k, values in categoryMap.iteritems():
            for v in values:
                valueMap[v] = k
        return categoryMap, valueMap

    def runFetchAll(self, query):
        # utility method for running a query against the sqlite database
        c = self.conn.cursor()
        return c.execute(query).fetchall()

    def runFetchOne(self, query):
        # utility method for running a query against the sqlite database
        c = self.conn.cursor()
        return c.execute(query).fetchone()

    def getFrequentItemSets(self, candidateSets):
        # generates the frequent item set for the current candidate set
        # From the paper, this returns Ln for Cn
        frequentSets = []
        for s in candidateSets:
            count = self.getCount(tuple(s))[0]
            if count >= self.support:
                frequentSets.append(s)
        return frequentSets

    def convertToSet(self, iterable):
        # utility function for reducing over an iterable of sets
        return reduce(lambda x, y: x.union(y), iterable)

    def hasUniqueCategories(self, values):
        # utility method to check if a list of values all belong to the same column
        return len(values) == len(set([self.valueMap[v] for v in values]))

    def getNextCandidates(self, candidates, size=2):
        """
        Algorithm: As explained in section 2.2.1
        JOIN STEP:
            1. Generate all 2-combinations of Ln-1
            2. Merge these sets together
            3. Validate these if these are of the correct size
               and do not have multiple entries of the same column
            4. Return all the valid ones
        PRUNE STEP:
            1. For each the valid subsets of size n
            2. Generate all subsets of size n - 1
            3. If ANY of these subsets are NOT in Ln-1, reject this set
        """
        possibleCombs = map(self.convertToSet, combinations(candidates, 2))
        validSets = [c for c in possibleCombs if self.hasUniqueCategories(c) and len(c) == size]
        newCandidates = []
        for s in validSets: # s is a set
            subsets = combinations(s, len(s)-1)
            isValid = True
            for x in subsets:
                if set(x) not in candidates:
                    isValid = False
                    continue
            if isValid and s not in newCandidates:
                newCandidates.append(s)
        return newCandidates

    def getCount(self, values):
        # returns the count of a n-ary tuple e.g. getCount(("A2", "B3"))
        m = {} # build the mapping 
        for v in values:
            for cat, items in self.categoryMap.iteritems():
                if v in items:
                    m[cat] = v
        query = self.generateQuery(**m)
        return self.runFetchOne(query)

    def generateQuery(self, **kwargs):
        # utility method that generates a SQL query based on the keyword arguments provided
        clause = " and ".join(["%s = '%s'" % (k, v) for (k, v) in kwargs.iteritems()])
        return "select count(*) from %s where %s" % (self.dbname, clause)

    def generateFrequentItemSets(self):
        # generates all sets of frequent itemsets from the dataset
        candidateSet = map(lambda x: set([x]), self.valueMap.keys())
        currentSize = 2
        while len(candidateSet):
            frequentSet = self.getFrequentItemSets(candidateSet)
            for s in frequentSet:
                t = tuple(s)
                self.frequentSets[t] = self.getCount(t)[0]
            candidateSet = self.getNextCandidates(frequentSet, currentSize)
            currentSize += 1

    def getSupportForRule(self, lhs, rhs):
        """ utility method that computes the support and confidence
        for a association rule lhs => rhs """
        s1 = self.getCount(tuple(lhs))[0]
        s2 = self.getCount(tuple(list(lhs) + [rhs]))[0]
        return float(s2)/s1, float(s2)/self.totalSize

    def getReadableContent(self, value):
        # utility missed to get a readable value of a value
        return self.valueMap[value] + " = " + value[0]

    def buildAssociationRules(self):
        # the primary workhorse method that generates the association rules
        candidates = filter(lambda x: len(x) > 1, self.frequentSets.keys())
        for candidate in candidates:
            for x in candidate:
                c = set(candidate)
                lhs, rhs = c.difference(set([x])), x
                conf, supp = self.getSupportForRule(lhs, rhs)
                if conf > self.confidence:
                    self.assocrules.append((lhs, rhs, conf, supp))

    def generateOutput(self, output_file):
        """ pretty prints the frequent itemsets and association mining rules
            in the output file """
        with open(output_file, "w") as f:
            with stdout_redirected(f):
                print "==Frequent itemsets (min_sup=%.2f%%)" % (100 * self.threshold)
                sortedSets = sorted(self.frequentSets.items(), key=operator.itemgetter(1), reverse=True)
                table = ((",".join(map(self.getReadableContent, s)), count, '%.2f%%' % (count*100/float(self.totalSize))) for s, count in sortedSets)
                print tabulate(table, headers=["ItemSets", "Count", "Support"], tablefmt="grid")

                print "\n\n==High-confidence association rules (min_conf=%.2f%%)" % (100 * self.confidence)
                sortedRules = sorted(self.assocrules, key=operator.itemgetter(2), reverse=True)
                table = []
                for lhs, rhs, conf, supp in sortedRules:
                    l = map(self.getReadableContent, lhs)
                    table.append((",".join(l), "=>", self.getReadableContent(rhs),
                                  "%.2f%%" % (100 * conf), "%.2f%%" % (100 * supp)))
                print tabulate(table, headers=["LHS", "", "RHS", "Confidence", "Support"], tablefmt="grid")

### Main driver
if __name__ == "__main__":
    filename = raw_input("File (leave blank to use INTEGRATED_DATASET.csv): ").strip()
    threshold = float(raw_input("Enter support(0.07): "))
    confidence = float(raw_input("Enter confidence(0.5): "))

    if filename and filename != "INTEGRATED_DATASET.csv":
        print "Please use the integrated dataset to run this program"
        sys.exit()

    createDatabase("INTEGRATED-DATASET.csv")
    apriori = Apriori(dbfile="data.db", dbname="school", confidence=confidence, threshold=threshold,
                      categories=["overall_grade", "env_grade", "perf_grade", "progress_grade"])
    apriori.generateFrequentItemSets()
    apriori.buildAssociationRules()
    apriori.generateOutput("output.txt")
    print "Rules and frequent itemsets generated in output.txt"

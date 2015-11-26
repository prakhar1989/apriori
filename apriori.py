import sqlite3

class Apriori(object):
    def __init__(self, dbfile, dbname, categories):
        self.conn = sqlite3.connect(dbfile)
        self.dbname = dbname
        self.columns = categories
        self.categoryMap = self.getCategories()
        self.support = 200

    def getCategories(self):
        categoryMap = {}
        for col in self.columns:
            values = self.runSelect("select distinct %s from %s" % (col, self.dbname))
            categoryMap[col] = set([x[0] for x in values])
        return categoryMap

    def runSelect(self, query):
        c = self.conn.cursor()
        return c.execute(query).fetchall()

    def getFrequentItemSets(size=1):
        pass

    def getCount(self, values):
        m = {} # build the mapping 
        for v in values:
            for cat, items in self.categoryMap.iteritems():
                if v in items:
                    m[cat] = v
        query = self.generateQuery(**m)
        return self.runSelect(query)

    def generateQuery(self, **kwargs):
        clause = " and ".join(["%s = '%s'" % (k, v) for (k, v) in kwargs.iteritems()])
        return "select count(*) from %s where %s" % (self.dbname, clause)


if __name__ == "__main__":
    apriori = Apriori(dbfile="data.db", dbname="school",
                      categories=["overall_grade", "env_grade", "perf_grade"])

    #print apriori.generateQuery(name="prakhar", age=10)
    #print apriori.runSelect(apriori.generateQuery(overall_grade="C1", env_grade="B2"))
    print apriori.getCount(("A1", "B2"))
    print apriori.getCount(("F1",))
    print apriori.getCount(("B2", "A1"))

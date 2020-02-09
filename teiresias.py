#!/usr/bin/env python3

import pymonetdb
import pprint
import re

class Storage:
    def __init__(self):
        self.colsizes = {}

    def __str__(self):
       return str(self.colsizes)

    def set_colsize(self, schema, table, column, size):
        if size < 0:
            raise ValueError("size musn't be negative")
        self.colsizes[schema +"." + table + "." + column] = size;

    def get_colsize(self, schema, table, column):
        return self.colsizes[schema +"." + table + "." + column]

    def get_colsize_by_key(self, key):
        return self.colsizes[key]

    def get_keys(self):
        return self.colsizes.keys()

    def count(self):
        return len(self.colsizes)

def get_storage(conn):
   strg = Storage()

   c = conn.cursor()
   c.execute('SELECT schema, table, column, columnsize + heapsize + hashes + imprints + orderidx AS colsize FROM sys.storage()')
   for row in c.fetchall():
       strg.set_colsize(row[0], row[1], row[2], row[3])
   return strg

class Adviser:
    def __init__(self, conn, storage):
        self.conn = conn
        self.storage = storage

    def _get_name(self, _str):
        m = re.search(":", _str)
        return m.string[1:m.end()-2]

    def estimate(self, query):
        c = self.conn.cursor()
        c.execute('explain ' + query)
        totalsize = 0
        columns = set()
        for row in c.fetchall():
            if re.search("sql.bind\(.*", str(row)) != None:
                substrs = re.split('\s+', str(row))
                schema = self._get_name(substrs[4])
                table  = self._get_name(substrs[5])
                column = self._get_name(substrs[6])
                columns.add((schema,table,column))
        for schema, table, column in columns:
                size = self.storage.get_colsize(schema, table, column)
                totalsize += size
        return totalsize


    # machine_specs is expected to be a dictionary containing:
    # {
    #  "<machine_name1>": <mem_size_in_bytes>,
    #  "<machine_name2>": <mem_size_in_bytes>,
    # ...
    # }
    def advise(self, query, machine_specs):
        totalsize = self.estimate(query)
        # print("Totalsize: " + str(totalsize))

        # sort machine_specs by values
        for machine, mem in sorted(machine_specs.items(), key = lambda
                machine_specs:(machine_specs[1], machine_specs[0])):
            # extra mem is for intermediates
            if totalsize * 2 < mem:
                return machine
        return machine;

# Examples to test the two main functions here, i.e. get_storage() and advise()
def test_storage(conn):
    storage = get_storage(conn)
    print("No. columns in storage: " + str(storage.count()))
    for k in storage.get_keys():
        print(k + ": " + str(storage.get_colsize_by_key(k)))

def test_advise(conn, query):
    machine_specs = {}
    machine_specs.update(small = 1024)
    machine_specs.update(large = 10*1024*1024)
    machine_specs.update(medium = 1024*1024)

    print("Query: " + query)

    storage = get_storage(conn)

    a = Adviser(conn, storage)
    adv = a.advise(query, machine_specs) 
    print("Advice: " + adv + " (" + str(machine_specs[adv]/1024.0/1024) + " MB RAM)")

def test_query(conn, query):
    c = conn.cursor()
    c.execute(query)
    for row in c.fetchall():
        print(row)

if __name__ == "__main__":
    database = "demo"
    hostname = "localhost"
    port = 50000
    conn = pymonetdb.connect(database, hostname, port)
    query = 'SELECT * FROM _tables limit 3'

    #test_storage(conn)
    test_advise(conn, query)
    #test_query(conn, query)

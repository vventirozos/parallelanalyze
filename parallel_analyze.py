#!/usr/local/bin/python
import multiprocessing
import argparse
import time
import sys
import psycopg2
import pprint

parser = argparse.ArgumentParser(description="Description goes here")
parser.add_argument('-c','--connection', default="dbname=postgres", help="""Connection string for use by psycopg. Defaults to "dbname=postgres" (local socket connecting to postgres database). dbname parameter in connection string is required.""")
parser.add_argument('-j', '--jobs', default=1, help="Total Threads to spawn")
parser.add_argument('--debug', action="store_true", help="Show additional debugging output")
args = parser.parse_args()

global my_jobs
my_jobs=int(args.jobs)

def conn_init():
    global dbname
    dbname_found = False
    for c in args.connection.split(" "):
        if args.debug:
            print("connection paramter: " + str(c))
        if c.find("dbname=") != -1:
            dbname = c.split("=")[1]
            dbname_found = True
            break
    if not dbname_found:
        print("Missing dbname parameter in database connection string")
        sys.exit(2)

    if args.debug:
        print "Connecting to database   -> %s" % (dbname)

    global conn
    conn = psycopg2.connect(args.connection)

def get_list_of_objects():
    obj_list="""select 'analyze ' || c.oid::regclass ||' /* manual analyze */ ;' FROM pg_class c LEFT JOIN pg_class t ON c.reltoastrelid = t.oid \
	WHERE c.relkind = 'r' order by greatest(age(c.relfrozenxid),age(t.relfrozenxid)) desc ;"""
    cursor = conn.cursor()
    cursor.execute(obj_list)
    records = cursor.fetchall()
    pprint.pprint(records)


def worker():
    """worker function"""
    print 'Worker'
    return

if __name__ == '__main__':
#    conn_init()
#    get_list_of_objects()
    jobs = []
    for i in range(my_jobs):
        p = multiprocessing.Process(target=worker)
        jobs.append(p)
        p.start()

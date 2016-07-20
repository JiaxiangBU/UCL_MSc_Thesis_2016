from __future__ import print_function
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import sys


def progress(table, percent):
	'''Just a progress bar to be printed to the screen
	'''
	width = 10
	perc = np.floor(percent*width)
	prog = "="*perc + ">"
	if percent != 1:
	    print('\r' + '{} Progress: [{}] {}%'.format(table, prog.ljust(width+1), str(percent*100)),end="")
	else:
	    print('\r' + '{} Progress: [{}] {}%'.format(table, prog.ljust(width+1), str(percent*100)),end="")
	sys.stdout.flush()

print("building appln_id lookup tree")
# get ppln_ids of interest
appln_ids = pd.read_csv('../Data/Y02E.csv')["appln_id"].values

# create lookup tree
ids = {}
for i in range(len(appln_ids)):
	key = str(appln_ids[i])[:2]
	if key not in ids.keys():
		ids[key] = []
	ids[key].append(int(appln_ids[i]))


# connect to db
print("connecting to psql db")
# query for all items from table
conn = psycopg2.connect("dbname = 'patstat' user = 'christophermartin' host = 'localhost' password = 'Sehnsucht7'")
cur = conn.cursor()

#print("finding total number of rows")
#cur.execute("SELECT COUNT(*) FROM tls203_appln_abstr;")
#N = cur.fetchall()

print("executing select query")
cur.execute("SELECT * FROM tls203_appln_abstr;")

# iterate over rows
print("iterating over rows")
count = 0
C = 0
row = cur.fetchone()
while row:
	# print progress
	#progress("abstracts", float(count)/float(N))
	print('\r' + str(count),end="")
	sys.stdout.flush()
	key = str(row.appln_id)[:2]
	if str(row.appln_id) in tree[key]:
		# write it to csv.
		C += 1
	count += 1
	row = cur.fetchone()

conn.close()

print(len(appln_ids))
print(C)




#engine = create_engine('postgresql://christophermartin:Sehnsucht7@localhost:5432/patstat')
#df = pd.read_sql_query("SELECT * FROM tls203_appln_abstr;",con=engine)








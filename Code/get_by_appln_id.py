from __future__ import print_function
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time
import sys
import numpy as np

'''Purpose: To pull entries from a psql table matching a set of appln_id's from a master csv file.
This is done incrementally in chucks of 10,000 as queries are slower on larger collections of keys.
Once a key has been queried at least once on the table before, the db will be faster the next time around
when checking against that key.
'''


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

def query(t1, table_name):
	t1 = "'), ('".join(map(str,t1['appln_id'].values))
	t1 = "('"+t1+"')"
	#t0 = time.time()
	df = pd.read_sql_query("SELECT * FROM {} WHERE cited_appln_id = ANY (VALUES {});".format(table_name, t1),con=engine)
	#print(time.time()-t0)
	return df


if __name__ == '__main__':

	temp = pd.read_csv('y02e_cpc.csv')
	engine = create_engine('postgresql://christophermartin:Sehnsucht7@localhost:5432/patstat')

	i = 0
	N = len(temp)
	while i < N:
		t1 = temp[i:i+10000]
		df = query(t1, "tls212_citation")
		if i == 0:
			mode = 'replace'
		else:
			mode = 'append'
		df.to_sql("y02e_citation", engine, if_exists='append')
		df.to_csv('y02e_citation.csv', mode='a',encoding='utf-8')
		progress("abstr", float(i)/float(N))
		i += 10000


















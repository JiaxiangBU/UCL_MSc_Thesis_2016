#!/usr/bin/env python
'''
Author: Chris Martin
Date: 2016-06-06
Purpose: to translate the series of CSV files
given by PATSTATs trial to PSQL.
'''
from __future__ import print_function
import csv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import time
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

def load_table(t_name):
	'''Writes the corresponding csv file of t_name to the patstat psql database one row at a time
	prints progress to screen.
	'''
	filesource = "../../" + t_name + ".csv"
	with open(filesource, 'rb') as f:
		# create reader to iterate through lines of csv
		reader = csv.reader(f, delimiter=';', quotechar='"')

		# create sql engine
		engine = create_engine('postgresql://christophermartin:Sehnsucht7@localhost:5432/patstat')

		# get the total number of rows in the most memory efficient way possible...
		totalrows = 0
		for row in reader:
		 	totalrows += 1
		f.seek(0)

		# begin reading csv line by line into corresponding psql table.
		count = 0
		rows = []
		for row in reader:
			pp = np.round(float(count)/float(totalrows),4)
			if count == 0:
				cols = [i.strip('\xef\xbb\xbf') for i in row]
			elif count ==1:
				# format row
				temp = pd.DataFrame(row).T
				temp.columns = cols

				# erase table if present!
				temp.to_sql(t_name, engine, if_exists='replace')

				# print progress of table loading to screen...
				progress(t_name, pp)
			elif count%400 == 0:
				# format row
				temp = pd.DataFrame(rows, columns = cols)

				# append to table
				temp.to_sql(t_name, engine, if_exists='append')

				# print progress of table loading to screen...
				progress(t_name, pp)
				rows = []
			else:
				# add to rows
				rows.append(row)

			count += 1

		# write remaining chunk.
		temp = pd.DataFrame(rows, columns = cols)
		temp.to_sql(t_name, engine, if_exists='append')
		progress(t_name, pp)

		#statement = "INSERT INTO ImportCSVTable " + \
		#"(name, address, phone) " + \
		#"VALUES ('%s', '%s', '%s')" % (tuple(row[0:3]))
		#execute statement


if __name__ == '__main__':
	
	table_list=["tls203_appln_abstr"]

	# for table in table list 
	for t_name in table_list:
		load_table(t_name)

	'''
	table_list=	[
	"tls201_appln",
	"tls202_appln_title",
	"tls203_appln_abstr",
	"tls204_appln_prior",
	"tls205_tech_rel",
	"tls206_person",
	"tls207_pers_appln",
	"tls209_appln_ipc",

	"tls210_appln_n_cls",
	"tls211_pat_publn",
	"tls212_citation",
	"tls214_npl_publn",
	"tls215_citn_categ",

	"tls216_appln_contn",
	"tls218_docdb_fam",
	"tls219_inpadoc_fam",
	"tls222_appln_jp_class",
	"tls223_appln_docus",
	"tls224_appln_cpc",
	"tls227_pers_publn",
	"tls228_docdb_fam_citn",
	"tls229_appln_nace2",
	"tls801_country",
	"tls901_techn_field_ipc",
	"tls902_ipc_nace2"]
	'''

	'''Done
	tls201_appln
	tls202_appln_title



	"tls205_tech_rel"
	'''

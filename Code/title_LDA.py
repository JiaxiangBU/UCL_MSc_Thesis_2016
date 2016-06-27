#!/usr/bin/env python
'''
Author: Chris Martin
Date: 2016-06-07
Purpose: to connect to patstat sql db, and prepare titles for LDA analysis
'''
import sqlite3


db_path = "../Data/PATSTAT_Trial_2015Autumn_DOCDB_MsAccess/"
db_name = "PATSTAT_Trial.sqlite"
conn = sqlite3.connect(db_path+db_name)
cur = conn.cursor()

# pull all patent dates and titles together
cur.execute('''SELECT tls201_appln.appln_id, tls202_appln_title.appln_title_lg, tls201_appln.appln_filing_year, tls202_appln_title.appln_title  
FROM tls201_appln, tls202_appln_title
WHERE tls201_appln.appln_filing_year < 2017 
AND tls202_appln_title.appln_title_lg='en'
''')

rows = cur.fetchall()
conn.close()

# format dates as dates and titles as strings

# sort by date and group by decade (to start)

# run LDA

import sqlite3


db_path = "../Data/PATSTAT_Trial_2015Autumn_DOCDB_MsAccess/"
db_name = "PATSTAT_Trial.sqlite"
conn = sqlite3.connect(db_path+db_name)
cur = conn.cursor()

cur.execute('''SELECT tls201_appln.appln_id, tls201_appln.appln_filing_year, tls202_appln_title.appln_title  
FROM tls201_appln, tls202_appln_title
INNER JOIN tls201_appln ON tls201_appln.appln_id = tls202_appln_title.appln_id
''')





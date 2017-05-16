# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 12:08:02 2017

@author: tpauley
"""
#set number of lineups to generate
num_lineups = 5

user_id = 'talktotfp'
session_id = 'abc12345'
#set point difference in lineups
point_gap = .025

import pulp
import sqlalchemy
from sqlalchemy.pool import NullPool
import cx_Oracle 
import pandas as pd
from tabulate import tabulate
import timeit
import matplotlib.pyplot as plt
import numpy as np
import datetime
#function for cleaning name strings in optimized results
def clean_result_strings(string):
    for ch in ['c_','pf_','sf_','sg_','pg_']:
        if ch in string:
            string=string.replace(ch,'')
    return string

#import cx_Oracle as oracle

 #import cx_Oracle as oracle
ip = 'localhost'
port = 1522
SID = 'xe'
dsn_tns = cx_Oracle.makedsn(ip, port, SID) 
alchemy_db = sqlalchemy.create_engine('oracle://nba:rp4490@localhost:1522/xe', poolclass=NullPool)
alchemy_con = alchemy_db.connect()    
    
    
#open oracle connection
ip = 'localhost'
port = 1522
SID = 'xe'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)

oracle_con = cx_Oracle.connect('nba', 'rp4490', dsn_tns)
#oracle_con = cx_Oracle.connect('nba/rp4490@localhost/xe')
oracle_cur = oracle_con.cursor()

#sql to select daily lineups and projections
oracle_query = """select GURU_ID as PLAYER_ID, GURU_ID as NUM_ID, GURU_ID as NAME, TO_NUMBER(FDP) as DFS_STD_DEV, FDS as SAL, ROUND(TO_NUMBER(FDP),3) as POINTS,
CASE WHEN POS = 1 then 1 else 0 end as PG,
CASE WHEN POS = 2 then 1 else 0 end as SG,
CASE WHEN POS = 3 then 1 else 0 end as SF,
CASE WHEN POS = 4 then 1 else 0 end as PF,
CASE WHEN POS = 5 then 1 else 0 end as C,
TO_NUMBER(FDP) as ADJ_MOD1_PCT_DIFF,TO_NUMBER(FDP) as ADJ_MOD1_REAL_DIFF,
1 as LOCK_EX
from GLOG_SALARY
where gdate = TO_DATE('11-APR-17','dd-mon-yy')
and FDP is NOT NULL
and FDS is NOT NULL
and GURU_ID IN (select PLAYER_ID from WEB_FD_LOCK_EX_LIST where USER_ID = '%s' and LOCK_EX = 1)
union all
select GURU_ID as PLAYER_ID, GURU_ID as NUM_ID, GURU_ID as NAME, TO_NUMBER(FDP) as DFS_STD_DEV, FDS as SAL, ROUND(TO_NUMBER(FDP),3) as POINTS,
CASE WHEN POS = 1 then 1 else 0 end as PG,
CASE WHEN POS = 2 then 1 else 0 end as SG,
CASE WHEN POS = 3 then 1 else 0 end as SF,
CASE WHEN POS = 4 then 1 else 0 end as PF,
CASE WHEN POS = 5 then 1 else 0 end as C,
TO_NUMBER(FDP) as ADJ_MOD1_PCT_DIFF,TO_NUMBER(FDP) as ADJ_MOD1_REAL_DIFF,
0 as LOCK_EX
from GLOG_SALARY
where gdate = TO_DATE('11-APR-17','dd-mon-yy')
and FDP is NOT NULL
and FDS is NOT NULL
and GURU_ID not IN (select PLAYER_ID from WEB_FD_LOCK_EX_LIST where USER_ID = '%s')""" % (user_id,user_id)


#load in query as dataframe
df = pd.read_sql_query(oracle_query,oracle_con)

#index the dataframe by player name 
idf = df.set_index('PLAYER_ID')



#convert positions to individual lists
pg = idf[idf['PG'] == 1]['NAME'].tolist()
sg = idf[idf['SG'] == 1]['NAME'].tolist()
sf = idf[idf['SF'] == 1]['NAME'].tolist()
pf = idf[idf['PF'] == 1]['NAME'].tolist()
c = idf[idf['C'] == 1]['NAME'].tolist()

#create dictionary of point projections for each position
pg_pts = dict( zip( pg, idf[idf['PG'] == 1]['POINTS'].tolist()) )
sg_pts = dict( zip( sg, idf[idf['SG'] == 1]['POINTS'].tolist()) )
sf_pts = dict( zip( sf, idf[idf['SF'] == 1]['POINTS'].tolist()) )
pf_pts = dict( zip( pf, idf[idf['PF'] == 1]['POINTS'].tolist()) )
c_pts = dict( zip( c, idf[idf['C'] == 1]['POINTS'].tolist()) )

#repeat above with salary dictionary
pg_sal = dict( zip( pg, idf[idf['PG'] == 1]['SAL'].tolist()) )
sg_sal = dict( zip( sg, idf[idf['SG'] == 1]['SAL'].tolist()) )
sf_sal = dict( zip( sf, idf[idf['SF'] == 1]['SAL'].tolist()) )
pf_sal = dict( zip( pf, idf[idf['PF'] == 1]['SAL'].tolist()) )
c_sal = dict( zip( c, idf[idf['C'] == 1]['SAL'].tolist()) )

pg_lock = dict( zip( pg, idf[idf['PG'] == 1]['LOCK_EX'].tolist()) )
sg_lock = dict( zip( sg, idf[idf['SG'] == 1]['LOCK_EX'].tolist()) )
sf_lock = dict( zip( sf, idf[idf['SF'] == 1]['LOCK_EX'].tolist()) )
pf_lock = dict( zip( pf, idf[idf['PF'] == 1]['LOCK_EX'].tolist()) )
c_lock = dict( zip( c, idf[idf['C'] == 1]['LOCK_EX'].tolist()) )
locked_player_total = idf["LOCK_EX"].sum()

#set position variables for solver
lpg     = pulp.LpVariable.dicts( "pg", indexs = pg, lowBound=0, upBound=1, cat='Integer', indexStart=[] )
lsg     = pulp.LpVariable.dicts( "sg", indexs = sg, lowBound=0, upBound=1, cat='Integer', indexStart=[] )
lsf     = pulp.LpVariable.dicts( "sf", indexs = sf, lowBound=0, upBound=1, cat='Integer', indexStart=[] )
lpf     = pulp.LpVariable.dicts( "pf", indexs = pf, lowBound=0, upBound=1, cat='Integer', indexStart=[] )
lc     = pulp.LpVariable.dicts( "c", indexs = c, lowBound=0, upBound=1, cat='Integer', indexStart=[] )


#setup to spit out multiple lineups
max_val = 1000
start = timeit.default_timer()
lineup_res = pd.DataFrame([])
export_res = pd.DataFrame([])
overall_res = pd.DataFrame([])
for i in range(0,num_lineups):
    #setup solver objective (sum of all points per player)
    prob  = pulp.LpProblem( "Minimalist example", pulp.LpMaximize )
    prob += pulp.lpSum( [ lpg[i]*pg_pts[i] for i in pg ] + 
                       [ lsg[i]*sg_pts[i] for i in sg ] +
                       [ lsf[i]*sf_pts[i] for i in sf ] +
                       [ lpf[i]*pf_pts[i] for i in pf ] +
                       [ lc[i]*c_pts[i] for i in c ]), " Objective of total points "
    
    #setup constraints for solver
    prob += pulp.lpSum( [ lpg[i]*pg_sal[i] for i in pg ] + 
                       [ lsg[i]*sg_sal[i] for i in sg ] +
                       [ lsf[i]*sf_sal[i] for i in sf ] +
                       [ lpf[i]*pf_sal[i] for i in pf ] +
                       [ lc[i]*c_sal[i] for i in c ]) <= 60000, " Constraint of total salary "
    prob += pulp.lpSum( [ lpg[i] for i in pg ] )==2, " Constraints for number of players"
    prob += pulp.lpSum( [ lsg[i] for i in sg ] )==2
    prob += pulp.lpSum( [ lsf[i] for i in sf ] )==2
    prob += pulp.lpSum( [ lpf[i] for i in pf ] )==2
    prob += pulp.lpSum( [ lc[i] for i in c ] )==1, " Constraint is that we choose two items "
    
    prob += pulp.lpSum( [ lpg[i]*pg_pts[i] for i in pg ] + 
                       [ lsg[i]*sg_pts[i] for i in sg ] +
                       [ lsf[i]*sf_pts[i] for i in sf ] +
                       [ lpf[i]*pf_pts[i] for i in pf ] +
                       [ lc[i]*c_pts[i] for i in c ]) <= max_val - point_gap
       
    prob += pulp.lpSum( [ lpg[i]*pg_lock[i] for i in pg ] + 
                       [ lsg[i]*sg_lock[i] for i in sg ] +
                       [ lsf[i]*sf_lock[i] for i in sf ] +
                       [ lpf[i]*pf_lock[i] for i in pf ] +
                       [ lc[i]*c_lock[i] for i in c ]) == locked_player_total
    
    #run solver
    prob.solve()

      
    #generate list of results from solver and clean names to match dataframe index (idf)        
    results = []
    for v in prob.variables():
        if v.varValue ==1:
            results.append(int(clean_result_strings(v.name)))
      
    #build dataframe of results
    results_df = idf.ix[results][['SAL','POINTS','NUM_ID']].sort_values(['POINTS'], ascending=False)
    max_val = pulp.value(prob.objective)
    res_list = results_df.index
    lineup_res = pd.DataFrame(res_list).reset_index()
    lineup_res = lineup_res.transpose().ix[['PLAYER_ID']]
    lineup_res['lineup_id'] = i
    lineup_res['lineup_pts'] = pulp.value(prob.objective)
    lineup_res['lineup_cost'] = int(idf.ix[results][['SAL']].sum().values)
    lineup_res['lineup_dev'] = int(idf.ix[results][['DFS_STD_DEV']].sum().values)
    lineup_res['lineup_acc'] = int(idf.ix[results][['ADJ_MOD1_REAL_DIFF']].sum().values)*-1
    overall_res = pd.concat([overall_res,lineup_res])
    
overall_res = overall_res.reset_index()
overall_res['lineup_pts'] =  round(overall_res['lineup_pts'],2)
overall_res.columns = ['index', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9', 'lineup_id', 'lineup_pts', 'lineup_cost', 'lineup_dev', 'lineup_acc']

print(overall_res)
overall_res['USER_ID'] = user_id
overall_res['SESSION_ID'] = session_id

delete_session_lineup_str = """delete from OPT_RESULTS where UPPER(USER_ID) like UPPER('%s') and UPPER(SESSION_ID) like UPPER('%s')""" % (user_id,session_id)

oracle_cur.execute(delete_session_lineup_str)
oracle_con.commit()
overall_res.to_sql('opt_results', alchemy_con, flavor=None,if_exists='append', 
                 index=True, index_label='GURU_ID', chunksize=num_lineups+1, dtype=None)


oracle_cur.close()
oracle_con.close()

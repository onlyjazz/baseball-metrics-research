# this file calculates wOBA and k percent fpr amgen study

import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd

ctx = snowflake.connector.connect(
          user="hannah",
          password="Hannah-312",
          account="dsidune-gob16463",
          warehouse="COMPUTE_WH",
          database="RISKGRAPHTRAINING",
          schema="DBT_WORKING")

cur = ctx.cursor()
sql = "select * from ADRSP"
cur.execute(sql)
df = cur.fetch_pandas_all()
cur.close()

resp_dict = {}

#wHR=4 (complete), wBB=2 (partial), wAB=1 (stable), progressive
#wOBA  = HR(complete)*wHR + BB(partial)*wBB + AB(stable)*wAB

#id# : [#complete, #partial, #stable, #progressive, #total visists]
# K% = (Total PD / Total Visits) * 100

def fill_resp_dict(response_code):
        for i, row in df.iterrows(): 
                patient_id = row['SUBJID']
                response = row[response_code]

                if patient_id not in resp_dict:
                        resp_dict[patient_id] = [0] * 5
                if response == "Complete response":
                        resp_dict[patient_id][0] += 1
                elif response == "Partial response":
                        resp_dict[patient_id][1] += 1
                elif response == "Stable disease":
                        resp_dict[patient_id][2] += 1
                elif response == "Progressive disease":
                        resp_dict[patient_id][3] += 1
                if response != "":
                        resp_dict[patient_id][4] += 1

resp_metric_dict = {} # patient: [wOBA, K%]
#id# : [#complete, #partial, #stable, #progressive, #total visists]

#wOBA = (1B * w1B  + 2B * w2B  +HR * wHR) / AB

def calc_wOBA():
        for key in resp_dict:
                if key not in resp_metric_dict:
                        resp_metric_dict[key] = [0] * 2
                resp_metric_dict[key][0]  =  round(100*(resp_dict[key][2] * 0.1 + resp_dict[key][1] * 0.3 + resp_dict[key][0])/resp_dict[key][4], 3)
                resp_metric_dict[key][1] =  round(100*resp_dict[key][3]/resp_dict[key][4], 3)

fill_resp_dict("RSRESP") 
calc_wOBA()

print(resp_metric_dict)
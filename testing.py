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

sql = "select * from AMGLABS"
cur.execute(sql)
df = cur.fetch_pandas_all()

sql2 = "select * from ADRSP"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

cur.close()
ctx.close()

tox_dict = {}

for i, row in df.iterrows():
        biomarker = row['MEASUREMENT']
        val = row['VALUE']
        patient_id = row['SUBJECT']

        if patient_id not in tox_dict:
                tox_dict[patient_id] = 0
        
        if biomarker ==  "Albumin" or biomarker == "Alkaline Phosphatase":
                tox_dict[patient_id] += 1

count = 0
for i in tox_dict:
        if tox_dict[i] != 0:
                count += 1

resp_dict = {}
for i, row in df2.iterrows():
        response = row["RSRESP"]
        id = row["SUBJID"]

        if id not in resp_dict:
                resp_dict[id] = 0
        if response != "":
                resp_dict[id] += 1

for i in resp_dict:
        if resp_dict[i] != 0:
                if i in tox_dict:
                        if tox_dict[i] == 0:
                                print("Patient", i, "has treatment but no liver labs")
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
sql = "select * from ENDPOINTS"
cur.execute(sql)
df = cur.fetch_pandas_all()
cur.close()

patient_dict = {}

for i, row in df.iterrows(): 
        patient_id = row['SUBJECT']
        bit = row['BIT']

        if patient_id not in patient_dict:
                patient_dict[patient_id] = True
        if bit == 1:
                patient_dict[patient_id] = False

print(patient_dict)
print("\n")

reached_end = []
not_reached = []

for key in patient_dict:
        if patient_dict[key] == True:
                reached_end.append(key)
        else:
                not_reached.append(key)

print("REACHED ENDPOINT:", reached_end)
print("\n")
print("DID NOT REACH ENDPOINT:", not_reached)
print("\n")
print(100*(len(reached_end)/len(patient_dict)), "percent of patients reached the primary endpoint")
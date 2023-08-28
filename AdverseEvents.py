import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector as snw
import pandas as pd
#from snowflake.snowpark.functions import col

ctx = snw.connect(
          #host=host,
          user="hannah",
          password="Hannah-312",
          account="dsidune-gob16463",
          warehouse="COMPUTE_WH",
          database="RISKGRAPHTRAINING",
          schema="DBT_WORKING")
          #protocol='https',
          #port=port

cur = ctx.cursor()
sql = "select * from AdverseEvents"
cur.execute(sql)
df = cur.fetch_pandas_all()
cur.close()
ctx.close()

AE_dict = {}
pat_list = []
pat_count = 0

# subject : [related, serious, non-serious]

for i, row in df.iterrows():
        pat_id = row['SUBJECT']
        AErelated = row['AEREL']
        AEserious = row['AESER']

        if pat_id not in AE_dict:
                AE_dict[pat_id] = [0, 0, 0]
                pat_list.append(pat_count)
                pat_count += 1
        if AErelated >= 2:
                AE_dict[pat_id][0] += 1
        if AEserious == 1:
                AE_dict[pat_id][1] += 1
        if AEserious > 1:
                AE_dict[pat_id][2] += 1

data = list(AE_dict.values())

rel_list = []
ser_list = []
non_ser_list = []

for sublist in data:
        rel_list.append(float(sublist[0]))
        ser_list.append(float(sublist[1]))
        non_ser_list.append(float(sublist[2]))

plt.plot(pat_list, rel_list, label='Related AE')
plt.plot(pat_list, ser_list, label='Serious AE')
plt.plot(pat_list, non_ser_list, label='Non-Serious AE')
plt.xlabel("Patient number")
plt.ylabel("number of instances")
plt.legend(loc='right')
plt.xticks(np.arange(min(pat_list), max(pat_list)+1, 1.0))
plt.show()
        

        

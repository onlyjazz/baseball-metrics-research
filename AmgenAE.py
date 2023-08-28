#from asyncio.windows_events import NULL
#from tkinter.tix import MAX
import statistics as stats
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

cur = ctx.cursor()

sql = "select * from AMGae"
cur.execute(sql)
df = cur.fetch_pandas_all()

sql2 = "select * from AMGsubjects"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

cur.close()
ctx.close()

AE_dict = {}

for i, row in df2.iterrows():
     pat_id = row['SUBJID']
     AE_dict[pat_id] = 0   


def normal_AE():
        min = 1000000000000
        max = 0
        # pat_count = 0
        # total = 0
        for i, row in df.iterrows():
                pat_id = row['SUBJID']
                severity = row['AESEVCD']
                
                if severity in [1, 2, 3, 4, 5]:
                        AE_dict[pat_id] += severity
        #                 if AE_dict[pat_id] < min:
        #                         min = AE_dict[pat_id]
        #                 if AE_dict[pat_id] > max:
        #                         max = AE_dict[pat_id]
        # print("MIN:", min, "MAX:", max)

        data = list(AE_dict.values())
        #avg = np.array(data.mean())
        # Calculate Q1 (25th percentile) and Q3 (75th percentile)
        Q1 = np.percentile(data, 25)
        Q3 = np.percentile(data, 75)
        #median = stats.median(data)
        # Calculate the IQR
        IQR = Q3 - Q1
        upper_fence = Q3 + 1.5*IQR
        print("FENCE:", upper_fence)

        red_flag = {}

        for key in AE_dict:
                if AE_dict[key] < upper_fence:
                        if AE_dict[key] < min:
                                min = AE_dict[key]
                        if AE_dict[key] > max:
                                max = AE_dict[key]
        
                        # AE_dict[key] = round((AE_dict[key] - min)/(max-min), 4)
                        # print(key, AE_dict[key]) 
                else:
                        red_flag[key] = AE_dict[key]

        print("MIN:", min, "MAX:", max)
        for key in red_flag:
                del AE_dict[key]

        for key in AE_dict:
                if AE_dict[key] < upper_fence:
                        AE_dict[key] = round((AE_dict[key] - min)/(max-min), 4)
                        print(key, AE_dict[key])

        # for key in AE_dict:
        #                   print(key, AE_dict[key])
        
        # print("MIN:", min, "MAX:", max)
        # print("Og mean:", mean)
        print("NUM OUTLIERS", len(red_flag))
        print(red_flag)

normal_AE()

AEnorm_score = sorted(list(AE_dict.values()))
pat_list = list(AE_dict.keys())
plt.scatter(pat_list, AEnorm_score, label='Non-Serious AE')
plt.title("min-max Adverse Event metric by patient (excluding outliers)")
plt.xlabel("Patient number")
plt.ylabel("AE score")
#plt.legend(loc='right')
#plt.xticks(np.arange(min(pat_list), max(pat_list)+1, 1.0))
plt.show()
        
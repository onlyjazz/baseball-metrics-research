import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd
import yaml

with open("amginfo.yaml", "r") as f:
        yamldata = yaml.safe_load(f)

num_biomarkers = yamldata['dataset_info'][0]
percent = yamldata['dataset_info'][1]
filter = yamldata['dataset_info'][2]

#print(yamldata)

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
cur.close()

patient_dict = {}
metric_dict = {}

def fill_patient_dict(bm_idx, bm_LB, bm_UB, percent, patient, value):
        #print("index:", bm_idx, "high:", bm_UB, "low:", bm_LB)
        patient_dict[patient][bm_idx] += 1
        
        bm_high = bm_UB*(1+percent/100)
        bm_low = bm_LB*(1-percent/100)
        
        if value <= bm_low or value >= bm_high:
                patient_dict[patient][bm_idx+1] += 1

def batting_avg(num_tests, num_abnormal):
        if num_tests == 0:
                return -0.1
        else:
                return round(num_abnormal / num_tests, 3)

def gen_metrics():
        for key in patient_dict:
                metric_dict[key] = [0] * num_biomarkers
                #print(yamldata["biomarkers"])
                for biomarker in yamldata["biomarkers"]:
                        # print("HERE:", yamldata['biomarkers'][biomarker][0])
                        curr_dict = yamldata['biomarkers']
                        # print(curr_dict[biomarker][0])
                        # print("HERE:", patient_dict[key][(curr_dict[biomarker][0])+1])
                        # print("HERE 2:", int((curr_dict[biomarker][0])/2))
                        metric_dict[key][int((curr_dict[biomarker][0])/2)] = batting_avg(patient_dict[key][curr_dict[biomarker][0]], patient_dict[key][(curr_dict[biomarker][0])+1])


def read_table():
       for i, row in df.iterrows(): 
                biomarker = row['MEASUREMENT']
                val = row['VALUE']
                patient_id = row['SUBJECT']

                if biomarker in yamldata["biomarkers"]:
                        if patient_id not in patient_dict:
                                patient_dict[patient_id] = [0] * 2*num_biomarkers 

                        index = yamldata["biomarkers"][biomarker][0]
                        low = yamldata["biomarkers"][biomarker][1]
                        high = yamldata["biomarkers"][biomarker][2]
                        fill_patient_dict(index, low, high, percent, patient_id, val) 

read_table()
#print(patient_dict)
gen_metrics()
print(metric_dict)

###########################################################################################################################################

#print("metric_dict length =", len(metric_dict))

# ALB_pat_list = []
# ALP_pat_list = []
# ALB_list = []
# ALP_list = []

# ALB_quartiles = [0] * 4
# ALP_quartiles = [0] * 4

# # total_alp = 0
# # alp_thrown = 0

# for key in metric_dict:
#         if metric_dict[key][0] >= filter:
#                 #print("patient", key, "has abnormal ALB levels 100 percent of the time!")
#                 ALB_pat_list.append(key)
#                 ALB_list.append(metric_dict[key][0])
#                 if metric_dict[key][0] < (1-filter)*0.25:
#                         ALB_quartiles[0] += 1   
#                 elif metric_dict[key][0] >= (1-filter)*0.25 and metric_dict[key][0] < (1-filter)*0.5:
#                         ALB_quartiles[1] += 1
#                 elif metric_dict[key][0] >= (1-filter)*0.5 and metric_dict[key][0] < (1-filter)*0.75:
#                         ALB_quartiles[2] += 1
#                 else:
#                         ALB_quartiles[3] += 1

#         if metric_dict[key][1] >= filter:
#                 #total_alp += 1
#                 #print("patient", key, "has abnormal ALP levels 100 percent of the time!")
#                 ALP_pat_list.append(key)
#                 ALP_list.append(metric_dict[key][1])
#                 if metric_dict[key][1] < (1-filter)*0.25:
#                         ALP_quartiles[0] += 1   
#                 elif metric_dict[key][1] >= (1-filter)*0.25 and metric_dict[key][1] < (1-filter)*0.5:
#                         ALP_quartiles[1] += 1
#                 elif metric_dict[key][1] >= (1-filter)*0.5 and metric_dict[key][1] < (1-filter)*0.75:
#                         ALP_quartiles[2] += 1
#                 else:
#                         ALP_quartiles[3] += 1
        
#         # elif metric_dict[key][1] < filter:
#         #         print(metric_dict[key][1])
#         #         alp_thrown += 1

# ALB_sorted = sorted(ALB_list)
# ALP_sorted = sorted(ALP_list)

# # print("ALP thrown out = ", alp_thrown)
# # print("TOTAL ALP COUNT =", total_alp)

# print("ALB quartile #1 =", ALB_quartiles[0], "quartile #2 =", ALB_quartiles[1], "quartile #3 =", ALB_quartiles[2], "quartile #4 =", ALB_quartiles[3])
# print("ALP quartile #1 =", ALP_quartiles[0], "quartile #2 =", ALP_quartiles[1], "quartile #3 =", ALP_quartiles[2], "quartile #4 =", ALP_quartiles[3])


# #gen_metrics()
# #print(metric_dict)

# # patients = list(metric_dict.keys())
# # ALB_list = []
# # ALP_list = []
# # for patient in metric_dict:
# #         ALB_list.append(metric_dict[patient][0])
# #         ALP_list.append(metric_dict[patient][1])
# plt.figure(1)
# plt.scatter(ALB_pat_list, ALB_sorted, label='ALB', color='pink')
# plt.legend(loc='right')
# plt.xlabel("Patient id")
# plt.ylabel("metric")
# plt.figure(2)
# plt.scatter(ALP_pat_list, ALP_sorted, label='ALP', color='green')
# plt.legend(loc='right')
# plt.xlabel("Patient id")
# plt.ylabel("metric")
# #plt.ylim(0.2, 1)
# #plt.xticks(np.arange(min(patients), max(patients)+1, 1.0))
# plt.show()
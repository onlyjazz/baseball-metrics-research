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

sql2 = "select * from AMGae"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

sql3 = "select * from ADRSP"
cur.execute(sql3)
df3 = cur.fetch_pandas_all()

cur.close()
ctx.close()

# tox_test_pats = []
# AE_pats = []

# only_tox = []
# only_AE = []

# for i,  row in df.iterrows(): 
#         id = row['SUBJECT']
#         if id not in tox_test_pats:
#                 tox_test_pats.append(id)

# for i,  row in df2.iterrows():
#         id = row['SUBJID']
#         if id not in AE_pats:
#                 AE_pats.append(id)
 
# for i in tox_test_pats:
#         if i not in AE_pats:
#                 only_tox.append(i)

# for i in AE_pats:
#         if i not in tox_test_pats:
#                 only_AE.append(i)

# print("Patients w tox tests and no AE:", only_tox)
# print("Patients w AE and no tox tests:", only_AE)


patient_dict = {}
tox_dict = {}

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
                tox_dict[key] = [0] * num_biomarkers
                #print(yamldata["biomarkers"])
                for biomarker in yamldata["biomarkers"]:
                        # print("HERE:", yamldata['biomarkers'][biomarker][0])
                        curr_dict = yamldata['biomarkers']
                        # print(curr_dict[biomarker][0])
                        # print("HERE:", patient_dict[key][(curr_dict[biomarker][0])+1])
                        # print("HERE 2:", int((curr_dict[biomarker][0])/2))
                        tox_dict[key][int((curr_dict[biomarker][0])/2)] = batting_avg(patient_dict[key][curr_dict[biomarker][0]], patient_dict[key][(curr_dict[biomarker][0])+1])
                toxicity = round((tox_dict[key][0] + tox_dict[key][1])/2, 3)
                if toxicity < 1:
                        tox_dict[key] = 1
                else:
                        tox_dict[key] = toxicity


def read_table():
       for i,  row in df.iterrows(): 
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

# combined_tox_dict = {}
# def combine_metric():
#         for key in tox_dict:
#                 combined_tox_dict[key] = round((tox_dict[key][0]+tox_dict[key][1])/2, 3)


AE_dict = {} # pat_id : [severe AE, total AE]
#AEtox_dict = {}

# severe/total
def count_AE():
        for i, row in df2.iterrows():
                pat_id = row['SUBJID']
                severity = row['AESEVCD']

                if pat_id not in AE_dict:
                        AE_dict[pat_id] = [0] * 2
                
                AE_dict[pat_id][1] += 1
                if severity >= 3:
                        AE_dict[pat_id][0] += 1
        #print(AE_dict)
        for key in AE_dict:
                if AE_dict[key][0] == 0:
                        AE_dict[key] = 0
                else:
                        AE_dict[key] = round(AE_dict[key][0]/AE_dict[key][1], 3)

#(AE metric + Toxicity metric)/2
wAST_dict = {}
def calc_wAST():
        for key in tox_dict:
                if key in AE_dict:
                        #print(tox_dict)
                        wAST_dict[key] = round((AE_dict[key] + tox_dict[key])/2, 3)
                else:
                        wAST_dict[key] = round(tox_dict[key]/2, 3)

read_table()
gen_metrics()
count_AE()
calc_wAST()
#print(wAST_dict)

##################################################################################################################

resp_dict = {}

def fill_resp_dict(response_code):
        for i, row in df3.iterrows(): 
                patient_id = row['SUBJID']
                response = row[response_code]

                if patient_id not in resp_dict:
                        resp_dict[patient_id] = [0] * 5
                if response == "Complete response":
                        resp_dict[patient_id][0] += 1
                        resp_dict[patient_id][4] += 1
                elif response == "Partial response":
                        resp_dict[patient_id][1] += 1
                        resp_dict[patient_id][4] += 1
                elif response == "Stable disease":
                        resp_dict[patient_id][2] += 1
                        resp_dict[patient_id][4] += 1
                elif response == "Progressive disease":
                        resp_dict[patient_id][3] += 1
                        resp_dict[patient_id][4] += 1

resp_metric_dict = {} # patient: [wOBA, K%]

def calc_wOBA():
        for key in resp_dict:
                if key not in resp_metric_dict:
                        resp_metric_dict[key] = [0] * 2
                if resp_dict[key][4] == 0:
                        resp_metric_dict[key][0] = 0
                        resp_metric_dict[key][1] = 0
                else:
                        resp_metric_dict[key][0]  =  round((resp_dict[key][2] * 0.1 + resp_dict[key][1] * 0.3 + resp_dict[key][0])/resp_dict[key][4], 0) #wOBA
                        resp_metric_dict[key][1] =  round(resp_dict[key][3]/resp_dict[key][4], 0) #k percent
                
fill_resp_dict("RSRESP") 
calc_wOBA()

composite_dict = {}

def calc_composite():
        for key in wAST_dict:
                if key in resp_metric_dict:
                        composite_dict[key] = round((resp_metric_dict[key][0] - resp_metric_dict[key][1])/wAST_dict[key], 3)# efficacy vs. risk-safety
                                                                                                                            # (wOBA - K%) / wAST


calc_composite()
#print("RESP_DICT", resp_dict)
print("RESP_METRIC_DICT:", resp_metric_dict)

for key in composite_dict:

        print("Patient", key)
        if key not in AE_dict:
                print("AE metric: NONE", "Toxicity:", tox_dict[key], "the wAST:", wAST_dict[key])
            
        print("AE metric:", AE_dict[key], "Toxicity:", tox_dict[key], "the wAST:", wAST_dict[key])
        print("Patient", key, ": wOBA and k%:", resp_metric_dict[key], "final metric:", composite_dict[key])
        print("\n")


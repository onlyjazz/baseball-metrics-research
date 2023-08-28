import statistics as stats
import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector as snw
import pandas as pd
import yaml

with open("amginfo.yaml", "r") as f:
        yamldata = yaml.safe_load(f)

num_biomarkers = yamldata['dataset_info'][0]
percent = yamldata['dataset_info'][1]
filter = yamldata['dataset_info'][2]

ctx = snw.connect(
          #host=host,
          user="hannah",
          password="Hannah-312",
          account="dsidune-gob16463",
          warehouse="COMPUTE_WH",
          database="RISKGRAPHTRAINING",
          schema="DBT_WORKING")

cur = ctx.cursor()

#Adverse Events
sql = "select * from AMGae"
cur.execute(sql)
df = cur.fetch_pandas_all()

#All patients
sql2 = "select * from AMGsubjects"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

#Response/treatment
sql3 = "SELECT DISTINCT subjid, visit, rsresp FROM ADRSP;"
#sql3 = "select * from ADRSP"
cur.execute(sql3)
df3 = cur.fetch_pandas_all()

#toxicity
sql4 = "select * from AMGLABS"
cur.execute(sql4)
df4 = cur.fetch_pandas_all()

# cur.close()
# ctx.close()

#**********************************************************************************************************************************

# master_dict = {}
# AE_dict = master_dict # sujectid: [weighted AE count, num treatments]

# def create_master_dict():
#         #fill patient master dictionary
#         for i, row in df2.iterrows():
#                 pat_id = row['SUBJID']
#                 master_dict[pat_id] = 0

resp_dict = {} #id# : [#complete, #partial, #stable, #progressive, #total visists]
#tally number of treatments 
def count_treatments():
        for i, row in df3.iterrows(): 
                patient_id = row['SUBJID']
                response = row['RSRESP']

                #resp_dict[patient_id] = [0] * 5

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

efficacy_dict = {} # patient: [wOBA, K%]
def calc_efficacy():
        for key in resp_dict:
                if key not in efficacy_dict:
                        efficacy_dict[key] = [0] * 2
                if resp_dict[key][4] == 0:
                        efficacy_dict[key][0] = 0
                        efficacy_dict[key][1] = 0
                else:
                        efficacy_dict[key][0]  =  round((resp_dict[key][2] * 0.1 + resp_dict[key][1] * 0.3 + resp_dict[key][0])/resp_dict[key][4], 2) #wOBA
                        efficacy_dict[key][1] =  round(resp_dict[key][3]/resp_dict[key][4], 2) #k percent

AE_dict = {} #patient#: wighted AE sum
# weighted sum of adverse events 
def sum_AE():
        for i, row in df.iterrows():
                pat_id = row['SUBJID']
                severity = row['AESEVCD']
                
                if pat_id not in AE_dict:
                        AE_dict[pat_id] = 0
                
                if severity in [1, 2, 3, 4, 5]:
                        AE_dict[pat_id] += severity

# number of adverse events/ total treatments
# will leave us with only patioents who have treatments
invalid_pats = []
def AEoverAB():
        #invalid_pats = []
        for key in AE_dict:
                if key not in resp_dict or resp_dict[key][4] == 0:
                        AE_dict[key] = 'NO RECORDED TREATMENTS'
                        invalid_pats.append(key)
                else:
                        AE_dict[key] = round(AE_dict[key]/resp_dict[key][4], 3)
        #print(invalid_pats)
        for i in invalid_pats:
                del AE_dict[i]
                if i in resp_dict:
                        del resp_dict[i]
                        del efficacy_dict[i]
        for key in resp_dict:
                if key not in AE_dict:
                        AE_dict[key] = 0
 
red_flag = {}
def find_outliers(dict):
        data = list(dict.values())
        Q1 = np.percentile(data, 25)
        Q3 = np.percentile(data, 75)

        # Calculate the IQR
        IQR = Q3 - Q1
        upper_fence = Q3 + 1.5*IQR
        print("UPPER FENCE:", upper_fence)

        #remove outliers and put in separate dict
        for key in dict:
                if dict[key] > upper_fence:
                        red_flag[key] = dict[key]
        
        for key in red_flag:
                del dict[key]


def scale_data(read_dict, write_dict, rg_val): #0 = green, 1 = red
        maxi = read_dict[max(read_dict, key = read_dict.get)]
        mini = read_dict[min(read_dict, key = read_dict.get)]

        #print("MINI:", mini, "MAXI:", maxi)

        for key in read_dict:
                if rg_val == 1: # red flag patients
                        #print("RED FLAG PAT =", key)
                        write_dict[key] = round((read_dict[key] - mini)/(maxi-mini) + 1, 4)
                        # if key in AE_dict:
                        #         print("successfully added to red flag to AE_dict")
                else: #green patients
                        write_dict[key] = round((read_dict[key] - mini)/(maxi-mini), 4)
                #print(key, dict[key])
        # if rg_val == 1:
        #         print("pat 141", AE_dict[141])
        #         print("FINAL AE DICT:", AE_dict)


#**********************************************************************************************************************************

patient_dict = {}
tox_dict = {}

def fill_tox_dict(bm_idx, bm_LB, bm_UB, percent, patient, value):
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

def gen_tox_metrics():
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

def read_toxicity():
       for i,  row in df4.iterrows(): 
                biomarker = row['MEASUREMENT']
                val = row['VALUE']
                patient_id = row['SUBJECT']

                if biomarker in yamldata["biomarkers"]:
                        if (patient_id not in patient_dict) and (patient_id not in invalid_pats):
                                #print("PAT ID ADDED TO TOX DICT:", patient_id)
                                patient_dict[patient_id] = [0] * 2*num_biomarkers
                        if patient_id in patient_dict:      
                                index = yamldata["biomarkers"][biomarker][0]
                                low = yamldata["biomarkers"][biomarker][1]
                                high = yamldata["biomarkers"][biomarker][2]
                                fill_tox_dict(index, low, high, percent, patient_id, val) 

# BaseballMetric = m1*efficacy - m2*safety - m3*toxicity
comp_dict = {} #Seq #, efficacy, AE, tox, composite, red_flag
def composite_metric(m1, m2, m3): # m1 = efficacy weight, m2 = safety weight, m3 = toxicity weight
        seq_count = 1
        for key in efficacy_dict:
                comp_dict[key] = [0] * 6
                comp_dict[key][0] = seq_count #sequence number
                comp_dict[key][1] = efficacy_dict[key] #efficacy metric
                comp_dict[key][2] = AE_dict[key] #Adverse events metric
                comp_dict[key][3] = tox_dict[key] #toxicity metric
                comp_dict[key][4] = m1*(efficacy_dict[key][0]-efficacy_dict[key][1]) - m2*AE_dict[key] - m3*tox_dict[key] #composite metric
                if comp_dict[key][4] <= 0: 
                        comp_dict[key][4] = 0
                if key in red_flag:
                        comp_dict[key][5] = True
                else:
                        comp_dict[key][5] = False
                seq_count += 1
#**********************************************************************************************************************************

#create_master_dict()
count_treatments()
calc_efficacy()
#have finished efficacy metric at this point stored in efficacy_dict

sum_AE()
AEoverAB()
find_outliers(AE_dict)
scale_data(AE_dict, AE_dict, 0)
#print("RED FLAG DICT:", red_flag, "\n")
scale_data(red_flag, AE_dict, 1) #adds red flag patients to all AE data
#print("INVALID PATS", invalid_pats)
myKeys = list(AE_dict.keys())
myKeys.sort()
AE_dict = {i: AE_dict[i] for i in myKeys}

#print("FINAL AE METRIC DICT:", AE_dict)
#have finished AE metric at this point stored in AE_dict

read_toxicity()
gen_tox_metrics()
#have finished toxicity metric at this point stored in tox_dict

composite_metric(60, 30, 10)
for key in efficacy_dict:
        print(key, efficacy_dict[key][0]-efficacy_dict[key][1])


#Seq #, efficacy, AE, tox, composite, red_flag
cur.execute("CREATE TABLE AMG RESULTS (seq_num int, pat_id int, efficacy VARCHAR(50), AE float, toxicity float, composite float, red_flag VARCHAR(10))")

cur.close()
ctx.close()
# for key in comp_dict:
#         if comp_dict[key][4] != 0:
#                 print(key, comp_dict[key])

# print("length efficacy dict =", len(efficacy_dict))
# print("length AE dict =", len(AE_dict))
# print("length tox dict =", len(tox_dict))

# for key in tox_dict:
#         if key not in efficacy_dict and key not in AE_dict:
#                 print("patient", key, "in tox but not in efficacy or AE")

#print(AE_dict)
# print(red_flag)
# print("NUM OUTLIERS", len(red_flag))
#scale_data(AE_dict)
#scale_data(red_flag)


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

sql2 = "select * from AMGae"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()
cur.close()

ctx.close()

ALB_upper = 54 # g/dL
ALB_lower = 34 # g/dL

ALP_upper = 140 # IU/L
ALP_lower = 20  # IU/L

percent = 20

# patient_id:[#ALBtests, #ALB abnormal, #ALPtests, #ALP abnormal]
patient_dict = {}

def fill_patient_dict(chem_index, chem_UB, chem_LB, patient, value): 
        patient_dict[patient][chem_index] += 1

        if value <= chem_LB or value >= chem_UB:
                patient_dict[patient][chem_index+1] += 1


def find_outliers(ALB_UB, ALB_LB, ALP_UB, ALP_LB, threshold):
      
        upper_thresh = 1+threshold/100        
        lower_thresh = 1-threshold/100
        
        ALB_high = ALB_UB*upper_thresh
        ALB_low = ALB_LB*lower_thresh
        #print("ALB lower = ", ALB_low, ", ALB upper = ", ALB_high)

        ALP_high = ALP_UB*upper_thresh
        ALP_low = ALP_LB*lower_thresh
        #print("ALP lower = ", ALP_low, ", ALP upper = ", ALP_high)


        for i, row in df.iterrows():
                chemical = row['MEASUREMENT']
                val = row['VALUE']
                patient_id = row['SUBJECT']

                if patient_id not in patient_dict:
                        patient_dict[patient_id] = [0, 0, 0, 0] 
                elif chemical == 'Albumin':
                        fill_patient_dict(0, ALB_high, ALB_low, patient_id, val)
                elif chemical == 'Alkaline Phosphatase':
                        fill_patient_dict(2, ALP_high, ALP_low, patient_id, val)


def batting_avg(num_tests, num_abnormal):
        if num_tests == 0:
                return -0.1
        else:
                return round(num_abnormal / num_tests, 3)


metric_dict = {}

def calc_tox_metrics():
        for key in patient_dict:
                metric_dict[key] = [0, 0]
                #print("Patient:", key)
                
                ALB_metric = batting_avg(patient_dict[key][0], patient_dict[key][1])
                #print("# ALB tests:", patient_dict[key][0], ", # ALB abnormal:", patient_dict[key][1], ", ALB metric:", ALB_metric)
                metric_dict[key][0] = ALB_metric
                
                ALP_metric = batting_avg(patient_dict[key][2], patient_dict[key][3])
                #print("# ALP tests:", patient_dict[key][2], ", # ALP abnormal:", patient_dict[key][3], ", ALP metric:", ALP_metric)
                metric_dict[key][1] = ALP_metric

find_outliers(ALB_upper, ALB_lower, ALP_upper, ALP_lower, percent)
print(patient_dict)
calc_tox_metrics()

#print(metric_dict)
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
#import snowflake.snowpark as snowpark
import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd
#from snowflake.snowpark.functions import col

ctx = snowflake.connector.connect(
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

# Execute a statement that will generate a result set.

# All patient data
sql = "select * from AllLabs"
cur.execute(sql)
df = cur.fetch_pandas_all()
# cur.close()
# ctx.close()

# biomarker limits 
sql2 = "select * from BioBounds"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

print("*************** printing bio-marker bounds **************")
for i, row in df2.iterrows():
        biomarker = row['BIOMARKER']
        low_val = row['LOW']
        high_val = row['HIGH']
        print(biomarker, low_val, high_val)

cur.close()
ctx.close()


ALB_upper = 5.4 # g/dL
ALB_lower = 3.4 # g/dL

ALP_upper = 140 # IU/L
ALP_lower = 20  # IU/L

ALT_upper = 40  # IU/L
ALT_lower = 7   # IU/L

AST_upper = 48 # IU/L
AST_lower = 8  # IU/L

BILI_upper = 1.2 # mg/dL
BILI_lower = 0.1 # mg/dL

PROT_upper = 8.3 # g/dL
PROT_lower = 6   # g/dL

percent = 20

# patient_id:[#ALBtests, #ALBoor, #ALPtests, #ALPoor, #ALTtests, #ALToor, #ASTtests, #BILItests, #PROTtests]
patient_dict = {}
# total_rows = 0
# overall_outliers = 0

def fill_patient_dict(chem_index, chem_UB, chem_LB, threshold, patient, value): 
        patient_dict[patient][chem_index] += 1

        if value <= chem_LB or value >= chem_UB:
        #if value  <= chem_LB:
                #overall_outliers += 1
                patient_dict[patient][chem_index+1] += 1


def find_outliers(ALB_UB, ALB_LB, ALP_UB, ALP_LB, ALT_UB, ALT_LB, AST_UB, AST_LB, BILI_UB, BILI_LB, PROT_UB, PROT_LB, threshold):
      
        upper_thresh = 1+threshold/100        
        lower_thresh = 1-threshold/100
        
        ALB_high = ALB_UB*upper_thresh
        ALB_low = ALB_LB*lower_thresh
        #print("ALB lower = ", ALB_low, ", ALB upper = ", ALB_high)

        ALP_high = ALP_UB*upper_thresh
        ALP_low = ALP_LB*lower_thresh
        #print("ALP lower = ", ALP_low, ", ALP upper = ", ALP_high)

        ALT_high = ALT_UB*upper_thresh
        ALT_low = ALT_LB*lower_thresh
        #print("ALT lower = ", ALT_low, ", ALT upper = ", ALT_high)

        AST_high = AST_UB*upper_thresh
        AST_low = AST_LB*lower_thresh
        #print("AST lower = ", AST_low, ", AST upper = ", AST_high)

        BILI_high = BILI_UB*upper_thresh
        BILI_low = BILI_LB*lower_thresh
        #print("BILI lower = ", BILI_low, ", BILI upper = ", BILI_high)

        PROT_high = PROT_UB*upper_thresh
        PROT_low = PROT_LB*lower_thresh
        #print("PROT lower = ", PROT_low, ", PROT upper = ", PROT_high)


        for i, row in df.iterrows():
                chemical = row['MEASUREMENT']
                val = row['VALUE']
                patient_id = row['SUBJECT']

                if patient_id not in patient_dict:
                        patient_dict[patient_id] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]  # does not accomodate any number of elements
                elif chemical == 'ALB' or chemical == 'Albumin':
                        fill_patient_dict(0, ALB_high, ALB_low, threshold, patient_id, val)
                elif chemical == 'ALP' or chemical == 'Alkaline Phosphatase':
                        fill_patient_dict(2, ALP_high, ALP_low, threshold, patient_id, val)
                elif chemical == 'ALT':
                        fill_patient_dict(4, ALT_high, ALT_low, threshold, patient_id, val)
                elif chemical == 'AST':
                        fill_patient_dict(6, AST_high, AST_low, threshold, patient_id, val)
                elif chemical == 'BILI' or chemical == 'Bilirubin':
                        fill_patient_dict(8, BILI_high, BILI_low, threshold, patient_id, val)
                elif chemical == 'PROT':
                        fill_patient_dict(10, PROT_high, PROT_low, threshold, patient_id, val)

                #total_rows += 1
        # percentage_outliers = num_outliers/num_rows
        # print("num_outliers = ", num_outliers, "\ntotal rows = ", num_rows, "\npercent outliers = ", percentage_outliers)

find_outliers(ALB_upper, ALB_lower, ALP_upper, ALP_lower, ALT_upper, ALT_lower, AST_upper, AST_lower, BILI_upper, BILI_lower, PROT_upper, PROT_lower, percent)

def batting_avg(num_tests, num_abnormal):
        if num_tests == 0:
                return -0.1
        else:
                return round(num_abnormal / num_tests, 2)


metric_dict = {}

for key in patient_dict:
        #patient_ALT_metric = round(patient_dict[key][1] / patient_dict[key][0], 2)
        #print("Patient", key, ", # visits:", patient_dict[key][0], ", # past threshold:", patient_dict[key][1], ", ALT metric =", patient_ALT_metric)
        metric_dict[key] = [0, 0, 0, 0, 0, 0]
        #print("Patient:", key)
        
        ALB_metric = batting_avg(patient_dict[key][0], patient_dict[key][1])
        #print("# ALB tests:", patient_dict[key][0], ", # ALB abnormal:", patient_dict[key][1], ", ALB metric:", ALB_metric)
        metric_dict[key][0] = ALB_metric
        
        ALP_metric = batting_avg(patient_dict[key][2], patient_dict[key][3])
        #print("# ALP tests:", patient_dict[key][2], ", # ALP abnormal:", patient_dict[key][3], ", ALP metric:", ALP_metric)
        metric_dict[key][1] = ALP_metric
        
        ALT_metric = batting_avg(patient_dict[key][4], patient_dict[key][5])
       # print("# ALT tests:", patient_dict[key][4], ", # ALT abnormal:", patient_dict[key][5], ", ALT metric:", batting_avg(patient_dict[key][4], patient_dict[key][5]))
        metric_dict[key][2] = ALT_metric
        
        AST_metric = batting_avg(patient_dict[key][6], patient_dict[key][7])
       # print("# AST tests:", patient_dict[key][6], ", # AST abnormal:", patient_dict[key][7], ", AST metric:", AST_metric)
        metric_dict[key][3] = AST_metric
        
        BILI_metric = batting_avg(patient_dict[key][8], patient_dict[key][9])
       # print("# BILI tests:", patient_dict[key][8], ", # BILI abnormal:", patient_dict[key][9], ", BILI metric:", BILI_metric)
        metric_dict[key][4] = BILI_metric
        
        PROT_metric = batting_avg(patient_dict[key][10], patient_dict[key][11])
       # print("# PROT tests:", patient_dict[key][10], ", # PROT abnormal:", patient_dict[key][11], ", PROT metric:", PROT_metric)
        metric_dict[key][5] = PROT_metric

        #print("\n")

# print("********** PRINTING METRIC DICT ***************")
# for key in metric_dict:
#         print("Patient:", key)
#         print("ALB:", metric_dict[key][0], "ALP:", metric_dict[key][1], "ALT:", metric_dict[key][2], "AST:", metric_dict[key][3], "BILI:", metric_dict[key][4], "PROT:", metric_dict[key][5])
#         #print("\n")


# composite metric
# weighted metric: (ALT + AST +  0.5(BILI) +  0.3 (ALP) +  0.1 (ALB) + 0.1 (PROT))/3
#composite_dict = {}

weighted_list = []
unweighted_list = []
valid_patient_list = []

#def composite_metric()
for key in metric_dict:
        valid_patient = True
        for i in metric_dict[key]:
                if i < 0:
                        valid_patient = False
        if valid_patient:
                weighted_list.append(round((metric_dict[key][2] + metric_dict[key][3] + 0.5*metric_dict[key][4] + 0.3*metric_dict[key][1] + 0.1*metric_dict[key][0] + 0.1*metric_dict[key][5])/3, 2))
                unweighted_list.append(round((metric_dict[key][0] + metric_dict[key][1] + metric_dict[key][2] + metric_dict[key][3] + metric_dict[key][4] + metric_dict[key][5])/6, 2))
                valid_patient_list.append(key)
        #composite_dict[key] = [unweighted, weighted]

print("WEIGHTED:", weighted_list, "\n")
print("UNWEIGHTED:", unweighted_list, "\n")



# graphing

#data = pd.DataFrame(metric_dict)
#data.head()
patients = list(metric_dict.keys())
metrics = list(metric_dict.values())

ALB_list = []
ALP_list = []
ALT_list = []
AST_list = []
BILI_list = []
PROT_list = []

for sublist in metrics:
        ALB_list.append(float(sublist[0]))
        ALP_list.append(float(sublist[1]))
        ALT_list.append(float(sublist[2]))
        AST_list.append(float(sublist[3]))
        BILI_list.append(float(sublist[4]))
        PROT_list.append(float(sublist[5]))

# print(patients)
# print(metrics)
#AST_list, BILI_list, PROT_list
#plt.plot(patients, ALB_list, ALP_list, ALT_list)
plt.figure(1)
plt.plot(patients, ALB_list, label='ALB')
plt.plot(patients, ALP_list, label='ALP')
plt.plot(patients, ALT_list, label='ALT')
plt.plot(patients, AST_list, label='AST')
plt.plot(patients, BILI_list, label='BILI')
plt.plot(patients, PROT_list, label='PROT')

plt.legend(loc='right')

plt.figure(2)
plt.plot(valid_patient_list, weighted_list, label='weighted')
plt.plot(valid_patient_list, unweighted_list, label='unweighted')
plt.xlabel("Patient id")
plt.ylabel("Composite metric score")
plt.legend(loc='right')
#plt.xticks(0, 100, 1)
plt.show()



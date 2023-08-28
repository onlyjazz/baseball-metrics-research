from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

st.title("Variable Inputs")
with st.form(key = "form1"):
        study_selection = st.selectbox('Select a Study', options=[15218191, 15224811, 15224428, 15224679])
        patient_selection = st.text_input('Input a Patient ID', '1')
        day_selection = st.text_input("Input a day to see a patient's progress upto", '-1')
        m1_selection = st.text_input('Input an efficacy weight', '60')
        m2_selection = st.text_input('Input a risk weight', '30')
        m3_selection = st.text_input('Input a toxicity weight', '10')

        submit_button = st.form_submit_button(label = "Enter")

target_id = int(patient_selection)
date_until = int(day_selection)

m1 = int(m1_selection)
m2 = int(m2_selection)
m3 = int(m3_selection)

study_id = str(study_selection)

ctx = snowflake.connector.connect(
          user="hannah",
          password="Hannah-312",
          account="dsidune-gob16463",
          warehouse="COMPUTE_WH",
          database="RISKGRAPHTRAINING",
          schema="DBT_WORKING")

cur = ctx.cursor()

#toxicity
sql =  "select * from mamglb where study_id =" + study_id + "and (LBTEST = 'Albumin' or LBTEST = 'Alkaline Phosphatase') order by VISITDY ASC;"
cur.execute(sql)
df = cur.fetch_pandas_all()

#response
sql2 = "select DISTINCT subjid, visitdy, rsresp, study_id from mamgrsp where (RSRESP = 'Progressive disease' or RSRESP = 'Stable disease' or RSRESP = 'Complete response' or RSRESP = 'Partial response') and study_id =" + study_id + "order by VISITDY ASC;"
cur.execute(sql2)
df2 = cur.fetch_pandas_all()

#adverse events
sql3 = "select * from mamgae where study_id =" + study_id + ";"
cur.execute(sql3)
df3 = cur.fetch_pandas_all()

cur.close()
ctx.close()
 
#************************************************************************************************

tox_selection = df.loc[df['SUBJID'] == target_id]
resp_selection = df2.loc[df2['SUBJID'] == target_id]
AE_selection = df3.loc[df3['SUBJID'] == target_id]

resp_data = [0] * 5 # [#complete, #partial, #stable, #progressive, #total visits]
AE_data = [0] * 2 # [#AEs, weighed AE sum]
tox_data = [0] * 4 # [#ALB tests, #abnormal ALB tests, #ALP tests, #abnormal ALP tests]

#print(tox_selection)

#iterate through all the visit days to find a patients latest day 
latest_day = 0

for i, row in tox_selection.iterrows(): #toxicity
        day = row['VISITDY']
        if day > latest_day:
                latest_day = int(day)

for i, row in resp_selection.iterrows(): #treatment
        day = row['VISITDY']
        if day > latest_day:
                latest_day = int(day)  

if date_until < latest_day and date_until >= 0:
        latest_day = date_until

#print("LASTEST DAY =", latest_day) 

#toxicity helper function
def fill_tox_dict(bm_idx, bm_LB, bm_UB, percent, value):
        tox_data[bm_idx] += 1
        
        bm_high = bm_UB*(1+percent/100)
        bm_low = bm_LB*(1-percent/100)
        
        if value <= bm_low or value >= bm_high:
                tox_data[bm_idx+1] += 1  

#*********************************************************************************************


def calc_efficacy(): # [#complete, #partial, #stable, #progressive, #total visits]
        if resp_data[4] == 0:
                return 0 
        else:
                obp = (resp_data[0] + resp_data[1] + resp_data[2])/resp_data[4] #SD + PR + CR)/AB
                slg = (2*resp_data[1] + 4*resp_data[0])/resp_data[4] #(2PR + 4CR)/AB
                return round(obp+slg, 3)

def calc_safety():
        if resp_data[4] == 0:
                return 0
        else:
                return round(AE_data[1]/resp_data[4], 3)

def calc_toxicity(): # [#ALB tests, #abnormal ALB tests, #ALP tests, #abnormal ALP tests]
        if resp_data[4] == 0:
                return 0
        else:
                return round((tox_data[1] + tox_data[3])/resp_data[4], 3)


def calc_composite(efficacy, safety, toxicity):
        return round((m1*efficacy)-(m2*safety)-(m3*toxicity), 3)

day_list = []
daily_composite_list = []

def tally_data(last_tox, last_AE, last_resp):
        for i in range(1, latest_day+1):
                day_list.append(i)
                for j, row in tox_selection.iterrows():
                        biomarker = row['LBTEST']
                        val = row['LBSTRESN']
                        day = row['VISITDY']
                        
                        #print("j =", j, "last_tox =", last_tox, "day = ", day, "day iterator = ", i)
                        if j > last_tox and day <= i:
                                #print("j = ", j)
                                if biomarker == 'Albumin':
                                        fill_tox_dict(0, 34, 54, 20, val) 
                                elif biomarker == 'Alkaline Phosphatase':
                                        fill_tox_dict(2, 20, 140, 20, val)
                                #print("looked at row", row.name)
                                
                                last_tox = j
                #print("Toxicity stats after day", i, tox_data)
                                

                for j, row in AE_selection.iterrows():
                        start_day = row['AESTDYI']
                        end_day = row['AEENDYI']
                        severity = row['AESEVCD']

                        if j > last_AE and (i >= start_day and i <= end_day):
                                AE_data[0] += 1
                                AE_data[1] += severity

                                #print("looked at row", row.name) 
                        
                                last_AE = j
                #print("AE stats after day", i, AE_data)


                for j, row in resp_selection.iterrows():
                        day = row['VISITDY']
                        response = row['RSRESP']

                        if j > last_resp and day <= i:
                                resp_data[4] += 1
                                if response == "Complete response":
                                     resp_data[0] += 1   
                                elif response == "Partial response":
                                     resp_data[1] += 1 
                                elif response == "Stable disease":
                                     resp_data[2] += 1
                                elif response == "Progressive disease":
                                     resp_data[3] += 1 
                        
                                last_resp = j

                efficacy = calc_efficacy()
                safety = calc_safety()
                toxicity = calc_toxicity()
                composite = calc_composite(efficacy, safety, toxicity)
                print("DAY", i, ": Efficacy =", efficacy, "Safety =", safety, "Toxicity", toxicity, "COMPOSITE =", composite)
                daily_composite_list.append(composite)


tally_data(0, 0, 0)

st.subheader("Patient "+ patient_selection +" Performance")
plt.plot(day_list, daily_composite_list)
plt.xlabel("Day")
plt.ylabel("Performance")
st.pyplot(plt.gcf())
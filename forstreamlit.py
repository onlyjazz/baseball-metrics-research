import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd
import streamlit as st

ctx = snowflake.connector.connect(
          user="hannah",
          password="Hannah-312",
          account="dsidune-gob16463",
          warehouse="COMPUTE_WH",
          database="RISKGRAPHTRAINING",
          schema="DBT_WORKING")
cur = ctx.cursor()

#sql = "select * from SUBJECTS"
sql = "select * from subjects where ab is not null or toxicity is not null"
cur.execute(sql)
df = cur.fetch_pandas_all()
cur.close()
ctx.close()

patient_master_list = [] # list of lists, sublists = [efficacy, safety, toxicity]

for i, row in df.iterrows():
        efficacy = row['EFFICACY']
        safety = row['SAFETY']
        toxicity = row['TOXICITY']

        sublist = [efficacy, safety, toxicity]
        patient_master_list.append(sublist)

pat_list = []
comp_list = []
def calc_composite(m1, m2, m3):
        count = 0
        for i in patient_master_list:
                pat_list.append(count)
                comp_list.append(m1*patient_master_list[count][0] - m2*patient_master_list[count][1] - m3*patient_master_list[count][2])
                count += 1

st.title("Composite Weights")
st.subheader("Efficacy Weight")
M1 = st.slider("a number 0-100", value=60)
st.write('slider number', M1)

st.subheader("Safety Weight")
M2 = st.slider("a number 0-100", value=30)
st.write('slider number', M2)

st.subheader("Toxicity Weight")
M3 = st.slider("a number 0-100", value=10)
st.write('slider number', M3)

calc_composite(M1, M2, M3)
comp_list = sorted(comp_list)

plt.plot(pat_list, comp_list)
plt.xlabel("Patients")
plt.ylabel("Composite")
st.pyplot(plt.gcf())
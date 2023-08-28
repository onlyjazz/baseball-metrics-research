#Python code developed during summer 2023 bootcamp to investigate and develop baseball metrics for clinical trial data

- makepandas.py - very first file written to deal with toxicity data from RLF100 - not dynamic, everything hardcoded

- biomarkermetrics.py - dynamic toxicity metric program, uses amginfo.yaml to change program parameters

- wAST.py - a program written to get composite metrics when we had the wAST and wOBA model

- forstreamlit.py - written for the first time we deployed to streamlit, the slider metric program

- timestamp.py - program that gets a single patient's composite trajectory over the entire course of the study, takes parameters from streamlit user input and can work with any of the 4 AMG oncology studies

- AEatbats.py - version of a program where we tried to define outliers or red flags, a design choice we ended up moving past

- adrsp.py, AmgenAE.py, findendpoints.py - all small programs written to simply tally/isolate one datapoint, the namesake of the file
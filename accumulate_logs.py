from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import argparse
import re
def get_xls(path):
    excel_list = []
    excel_name_lst=[]
    for dirname, dirnames, files in os.walk(path):
        for file in files:
            if file.endswith((".xlsx" ,".xls")):
                excel_list.append(os.path.join(dirname,file))
    for excel_name in excel_list:
        if excel_name.endswith('.xlsx'):
            excel_name_lst.append(os.path.split(excel_name)[1][:-5])
    return excel_list,excel_name_lst

def change(x):
	return x.split(".")[0]
def get_accumulated_logs(xlx_path):
    xlxs_files = [os.path.join(xlx_path, xlx) for xlx in os.listdir(xlx_path) if xlx.endswith("xlsx")]
    spectral_file = os.path.join(xlx_path, 'Accumulated_data.csv')
    if os.path.isfile(spectral_file):
        os.remove(spectral_file)
    for xlxs in xlxs_files:
        print("Reading xls = {}".format(xlxs))
        xlxs_name = os.path.split(xlxs)[1]
        xls_data = pd.ExcelFile(xlxs)
        df_train = pd.read_excel(xls_data, 'Train_var', index_col=None, na_values=[''])
        df_train.insert(loc=0,column='id', value=xlxs_name)
        if not os.path.isfile(spectral_file):
            df_train.to_csv(spectral_file, sep=',', encoding='utf-8', mode='w+')
        else:
            df_train.to_csv(spectral_file, sep=',', encoding='utf-8', mode='a', header=None)
def merge_clinical_spectral(spectral_file,clinical_file):
    #clinical_file="/Users/whyshreddy/workspace/may2019/erragadda_2018/erragadda_test/clinical_data.csv"
    a=pd.read_csv(clinical_file)
    #print("a:",a)
    b=pd.read_csv(spectral_file)
    #a=a.drop(['tb_family_history','living_with_tb',])
    b=b.drop(['Unnamed: 0.1','Unnamed: 0'],axis=1)
    #print(b)
    b = b.dropna(axis=1)
    b['id'] = b['id'].apply(change)
    #print(b['id'])

    #print("b after change",b['id'])

    b.id=b.id.astype(str)
    a.id=a.id.astype(str)
    d=pd.merge(a,b)
    d=d.assign(Target_status="")
    cols=d.columns.tolist()
    cols = cols[-1:] + cols[:-1] #starts from first column by subtracting -1 to it i.e. starts from "id"

    d=d[cols]
    a=d
    a['height_feet'] = a.apply(lambda row: str(row.height_feet) + "." + str(row.height_inches), axis=1).astype(float)
    a=a.drop("height_inches",axis=1)
    a['BMI'] = a.apply(lambda row: row.weight_in_kgs/(row.height_feet*0.3)**2, axis=1)
    cols = a.columns.tolist()
    a=a[cols]
    #a.to_csv("abefore.csv",index=True)

    cols=cols[:8]+cols[-1:]+cols[8:-1]
    a=a[cols]
    cols = a.columns.tolist()
    #a.to_csv("awithbmi.csv",index=False)
    a.columns=[
    'Target_Status',
    'Patient ID',
     'Name',
     'Gender',
     'Age',
     'Height',
     'Weight',
     'Marital Status',
     'BMI',
     'Occupation',
     'Organization',
     'Smoking',
     'Alcohol',
     'Prev Family History of TB',
     'Living with TB',
     'Paroxysmal Cough Indication',
     'HIV',
     'Diabetes',
     'Night sweat',
     'Loss of Appetite',
     'Exercise',
     'Any Supplements',
     'Chestpain',
     'Current Fever Pattern',
     'Appetite Pattern',
     'Cough Frequency',
     'Cough type',
     'Travelling Frequency',
     'Current Medications',
     'Pre-Existing Conditions',
     '200Hz_LEFT_SUM',
     '200-500Hz_LEFT_SUM',
     '500-1000Hz_LEFT_SUM',
     '1000-1500Hz_LEFT_SUM',
     '1500-2000Hz_LEFT_SUM',
     '2000-2500Hz_LEFT_SUM',
     '2500-3000Hz_LEFT_SUM',
     '3000-3500Hz_LEFT_SUM',
     '3500-4000Hz_LEFT_SUM',
     '4000-4500Hz_LEFT_SUM',
     '4500-5000Hz_LEFT_SUM',
     '200Hz_LEFT_MEAN',
     '200-500Hz_LEFT_MEAN',
     '500-1000Hz_LEFT_MEAN',
     '1000-1500Hz_LEFT_MEAN',
     '1500-2000Hz_LEFT_MEAN',
     '2000-2500Hz_LEFT_MEAN',
     '2500-3000Hz_LEFT_MEAN',
     '3000-3500Hz_LEFT_MEAN',
     '3500-4000Hz_LEFT_MEAN',
     '4000-4500Hz_LEFT_MEAN',
     '4500-5000Hz_LEFT_MEAN',
     '200Hz_LEFT_SD',
     '200-500Hz_LEFT_SD',
     '500-1000Hz_LEFT_SD',
     '1000-1500Hz_LEFT_SD',
     '1500-2000Hz_LEFT_SD',
     '2000-2500Hz_LEFT_SD',
     '2500-3000Hz_LEFT_SD',
     '3000-3500Hz_LEFT_SD',
     '3500-4000Hz_LEFT_SD',
     '4000-4500Hz_LEFT_SD',
     '4500-5000Hz_LEFT_SD',
     '200Hz_LEFT_VARIANCE',
     '200-500Hz_LEFT_VARIANCE',
     '500-1000Hz_LEFT_VARIANCE',
     '1000-1500Hz_LEFT_VARIANCE',
     '1500-2000Hz_LEFT_VARIANCE',
     '2000-2500Hz_LEFT_VARIANCE',
     '2500-3000Hz_LEFT_VARIANCE',
     '3000-3500Hz_LEFT_VARIANCE',
     '3500-4000Hz_LEFT_VARIANCE',
     '4000-4500Hz_LEFT_VARIANCE',
     '4500-5000Hz_LEFT_VARIANCE',
     '200Hz_LEFT_COEFF_LEFT_VARIANCE',
     '200-500Hz_LEFT_COEFF_LEFT_VARIANCE',
     '500-1000Hz_LEFT_COEFF_LEFT_VARIANCE',
     '1000-1500Hz_LEFT_COEFF_LEFT_VARIANCE',
     '1500-2000Hz_LEFT_COEFF_LEFT_VARIANCE',
     '2000-2500Hz_LEFT_COEFF_LEFT_VARIANCE',
     '2500-3000Hz_LEFT_COEFF_LEFT_VARIANCE',
     '3000-3500Hz_LEFT_COEFF_LEFT_VARIANCE',
     '3500-4000Hz_LEFT_COEFF_LEFT_VARIANCE',
     '4000-4500Hz_LEFT_COEFF_LEFT_VARIANCE',
     '4500-5000Hz_LEFT_COEFF_LEFT_VARIANCE',
     '200Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '200-500Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '500-1000Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '1000-1500Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '1500-2000Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '2000-2500Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '2500-3000Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '3000-3500Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '3500-4000Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '4000-4500Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '4500-5000Hz_LEFT_TOP10_LEFT_AMPLITUDE',
     '200Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '200-500Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '500-1000Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '1000-1500Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '1500-2000Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '2000-2500Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '2500-3000Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '3000-3500Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '3500-4000Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '4000-4500Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '4500-5000Hz_LEFT_SPECTRAL_LEFT_CENTROID',
     '200Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '200-500Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '500-1000Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '1000-1500Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '1500-2000Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '2000-2500Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '2500-3000Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '3000-3500Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '3500-4000Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '4000-4500Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '4500-5000Hz_LEFT_SPECTRAL_LEFT_FLATNESS',
     '200Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '200-500Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '500-1000Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '1000-1500Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '1500-2000Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '2000-2500Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '2500-3000Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '3000-3500Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '3500-4000Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '4000-4500Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '4500-5000Hz_LEFT_SPECTRAL_LEFT_SKEWNESS',
     '200Hz_LEFT_KURTOSIS',
     '200-500Hz_LEFT_KURTOSIS',
     '500-1000Hz_LEFT_KURTOSIS',
     '1000-1500Hz_LEFT_KURTOSIS',
     '1500-2000Hz_LEFT_KURTOSIS',
     '2000-2500Hz_LEFT_KURTOSIS',
     '2500-3000Hz_LEFT_KURTOSIS',
     '3000-3500Hz_LEFT_KURTOSIS',
     '3500-4000Hz_LEFT_KURTOSIS',
     '4000-4500Hz_LEFT_KURTOSIS',
     '4500-5000Hz_LEFT_KURTOSIS',
     '200Hz_LEFT_MFCC',
     '200-500Hz_LEFT_MFCC',
     '500-1000Hz_LEFT_MFCC',
     '1000-1500Hz_LEFT_MFCC',
     '1500-2000Hz_LEFT_MFCC',
     '2000-2500Hz_LEFT_MFCC',
     '2500-3000Hz_LEFT_MFCC',
     '3000-3500Hz_LEFT_MFCC',
     '3500-4000Hz_LEFT_MFCC',
     '4000-4500Hz_LEFT_MFCC',
     '4500-5000Hz_LEFT_MFCC',
     '200Hz_LEFT_Energy',
     '200-500Hz_LEFT_Energy',
     '500-1000Hz_LEFT_Energy',
     '1000-1500Hz_LEFT_Energy',
     '1500-2000Hz_LEFT_Energy',
     '2000-2500Hz_LEFT_Energy',
     '2500-3000Hz_LEFT_Energy',
     '3000-3500Hz_LEFT_Energy',
     '3500-4000Hz_LEFT_Energy',
     '4000-4500Hz_LEFT_Energy',
     '4500-5000Hz_LEFT_Energy',
     '200Hz_RIGHT_SUM',
     '200-500Hz_RIGHT_SUM',
     '500-1000Hz_RIGHT_SUM',
     '1000-1500Hz_RIGHT_SUM',
     '1500-2000Hz_RIGHT_SUM',
     '2000-2500Hz_RIGHT_SUM',
     '2500-3000Hz_RIGHT_SUM',
     '3000-3500Hz_RIGHT_SUM',
     '3500-4000Hz_RIGHT_SUM',
     '4000-4500Hz_RIGHT_SUM',
     '4500-5000Hz_RIGHT_SUM',
     '200Hz_RIGHT_MEAN',
     '200-500Hz_RIGHT_MEAN',
     '500-1000Hz_RIGHT_MEAN',
     '1000-1500Hz_RIGHT_MEAN',
     '1500-2000Hz_RIGHT_MEAN',
     '2000-2500Hz_RIGHT_MEAN',
     '2500-3000Hz_RIGHT_MEAN',
     '3000-3500Hz_RIGHT_MEAN',
     '3500-4000Hz_RIGHT_MEAN',
     '4000-4500Hz_RIGHT_MEAN',
     '4500-5000Hz_RIGHT_MEAN',
     '200Hz_RIGHT_SD',
     '200-500Hz_RIGHT_SD',
     '500-1000Hz_RIGHT_SD',
     '1000-1500Hz_RIGHT_SD',
     '1500-2000Hz_RIGHT_SD',
     '2000-2500Hz_RIGHT_SD',
     '2500-3000Hz_RIGHT_SD',
     '3000-3500Hz_RIGHT_SD',
     '3500-4000Hz_RIGHT_SD',
     '4000-4500Hz_RIGHT_SD',
     '4500-5000Hz_RIGHT_SD',
     '200Hz_RIGHT_VARIANCE',
     '200-500Hz_RIGHT_VARIANCE',
     '500-1000Hz_RIGHT_VARIANCE',
     '1000-1500Hz_RIGHT_VARIANCE',
     '1500-2000Hz_RIGHT_VARIANCE',
     '2000-2500Hz_RIGHT_VARIANCE',
     '2500-3000Hz_RIGHT_VARIANCE',
     '3000-3500Hz_RIGHT_VARIANCE',
     '3500-4000Hz_RIGHT_VARIANCE',
     '4000-4500Hz_RIGHT_VARIANCE',
     '4500-5000Hz_RIGHT_VARIANCE',
     '200Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '200-500Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '500-1000Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '1000-1500Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '1500-2000Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '2000-2500Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '2500-3000Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '3000-3500Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '3500-4000Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '4000-4500Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '4500-5000Hz_RIGHT_COEFF_RIGHT_VARIANCE',
     '200Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '200-500Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '500-1000Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '1000-1500Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '1500-2000Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '2000-2500Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '2500-3000Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '3000-3500Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '3500-4000Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '4000-4500Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '4500-5000Hz_RIGHT_TOP10_RIGHT_AMPLITUDE',
     '200Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '200-500Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '500-1000Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '1000-1500Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '1500-2000Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '2000-2500Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '2500-3000Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '3000-3500Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '3500-4000Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '4000-4500Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '4500-5000Hz_RIGHT_SPECTRAL_RIGHT_CENTROID',
     '200Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '200-500Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '500-1000Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '1000-1500Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '1500-2000Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '2000-2500Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '2500-3000Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '3000-3500Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '3500-4000Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '4000-4500Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '4500-5000Hz_RIGHT_SPECTRAL_RIGHT_FLATNESS',
     '200Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '200-500Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '500-1000Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '1000-1500Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '1500-2000Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '2000-2500Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '2500-3000Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '3000-3500Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '3500-4000Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '4000-4500Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '4500-5000Hz_RIGHT_SPECTRAL_RIGHT_SKEWNESS',
     '200Hz_RIGHT_KURTOSIS',
     '200-500Hz_RIGHT_KURTOSIS',
     '500-1000Hz_RIGHT_KURTOSIS',
     '1000-1500Hz_RIGHT_KURTOSIS',
     '1500-2000Hz_RIGHT_KURTOSIS',
     '2000-2500Hz_RIGHT_KURTOSIS',
     '2500-3000Hz_RIGHT_KURTOSIS',
     '3000-3500Hz_RIGHT_KURTOSIS',
     '3500-4000Hz_RIGHT_KURTOSIS',
     '4000-4500Hz_RIGHT_KURTOSIS',
     '4500-5000Hz_RIGHT_KURTOSIS',
     '200Hz_RIGHT_MFCC',
     '200-500Hz_RIGHT_MFCC',
     '500-1000Hz_RIGHT_MFCC',
     '1000-1500Hz_RIGHT_MFCC',
     '1500-2000Hz_RIGHT_MFCC',
     '2000-2500Hz_RIGHT_MFCC',
     '2500-3000Hz_RIGHT_MFCC',
     '3000-3500Hz_RIGHT_MFCC',
     '3500-4000Hz_RIGHT_MFCC',
     '4000-4500Hz_RIGHT_MFCC',
     '4500-5000Hz_RIGHT_MFCC',
     '200Hz_RIGHT_Energy',
     '200-500Hz_RIGHT_Energy',
     '500-1000Hz_RIGHT_Energy',
     '1000-1500Hz_RIGHT_Energy',
     '1500-2000Hz_RIGHT_Energy',
     '2000-2500Hz_RIGHT_Energy',
     '2500-3000Hz_RIGHT_Energy',
     '3000-3500Hz_RIGHT_Energy',
     '3500-4000Hz_RIGHT_Energy',
     '4000-4500Hz_RIGHT_Energy',
     '4500-5000Hz_RIGHT_Energy']

    #d=pd.read_csv("b.csv")
    #a=d.append(a)
    #a.to_csv("after changing column names.csv",index=False)
    a=a.apply(lambda x: x.astype(str).str.lower())
    a.drop_duplicates(subset='Patient ID',keep='last',inplace=True)

    a.to_csv("goldforsas.csv",index=False)
    print("Goldforsas is created, please check it manually!!")
    return a

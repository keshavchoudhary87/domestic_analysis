import pandas as pd
import numpy as np

path = 'E:\GSTN Data\Working\GSTR1\\'
dict_qtr = {1:4, 2:4, 3:4, 4:1, 5:1, 6:1, 7:2, 8:2, 9:2, 10:3, 11:3, 12:3}
files_gstr1_2017 = ['072017_top_10L_R1_TABLE_12.csv', '082017_top_10L_R1_TABLE_12.csv', '092017_top_10L_R1_TABLE_12.csv', '102017_top_10L_R1_TABLE_12.csv',
                     '112017_top_10L_R1_TABLE_12.csv', '122017_top_10L_R1_TABLE_12.csv', '012018_top_10L_R1_TABLE_12.csv', '022018_top_10L_R1_TABLE_12.csv',
                     '032018_top_10L_R1_TABLE_12.csv']
files_gstr1_2018 = ['042018_top_10L_R1_TABLE_12.csv', '052018_top_10L_R1_TABLE_12.csv', '062018_top_10L_R1_TABLE_12.csv', '072018_top_10L_R1_TABLE_12.csv',
                    '082018_top_10L_R1_TABLE_12.csv', '092018_top_10L_R1_TABLE_12.csv', '102018_top_10L_R1_TABLE_12.csv', '112018_top_10L_R1_TABLE_12.csv',
                    '122018_top_10L_R1_TABLE_12.csv', '012019_top_10L_R1_TABLE_12.csv', '022019_top_10L_R1_TABLE_12.csv', '032019_top_10L_R1_TABLE_12.csv']

'''
Read data from each of the monthly files for FY 2017-18
'''
first_flag = 0
for file in files_gstr1_2017:
    file_name = path + file
    df_temp = pd.read_csv(file_name, dtype={'state_cd': object, 'rtn_prd': object}, engine='python')
    
    # Add CGST, SGST & IGST liabilities to get combined liabilities
    df_temp['tax_liab'] = df_temp.cgst + df_temp.sgst + df_temp.igst
    
    # Segregate month and year from period
    df_temp['year'] = df_temp.rtn_prd.str.slice(2,)
    df_temp['month'] = df_temp.rtn_prd.str.slice(0, 2)
    df_temp['year'] = df_temp['year'].astype(int)
    df_temp['month'] = df_temp['month'].astype(int)
    
    # Assign quarter for months based on dict_qtr
    df_temp['qtr'] = df_temp['month'].map(dict_qtr)
    df_temp['year'] = np.where(df_temp['qtr']==4, df_temp['year']-1, df_temp['year'])
    
    # Append the monthly data into a single dataframe
    if first_flag==0:
        gstr1_2017 = df_temp
    else:
        gstr1_2017 = gstr1_2017.append(df_temp, ignore_index=True)
    first_flag += 1

# Convert monthly data to quarterly
gstr1_2017['gstin_period_hsn'] = gstr1_2017.ann_gstin_id + '_' + gstr1_2017.year.astype(str) + '_' + gstr1_2017.qtr.astype(str) + '_' + gstr1_2017.hsn_sc
grouped = gstr1_2017.groupby('gstin_period_hsn')['taxable_value', 'cgst', 'sgst', 'igst', 'tax_liab', 'cess'].sum()
qtr_gstr1_2017 = pd.DataFrame(data=grouped)
qtr_gstr1_2017 = qtr_gstr1_2017.reset_index()
qtr_gstr1_2017['gstin_hash'] = qtr_gstr1_2017.gstin_period_hsn.str.slice(0,39)
qtr_gstr1_2017['year'] = qtr_gstr1_2017.gstin_period_hsn.str.slice(40, 44)
qtr_gstr1_2017['qtr'] = qtr_gstr1_2017.gstin_period_hsn.str.slice(45, 46)
qtr_gstr1_2017['hsn'] = qtr_gstr1_2017.gstin_period_hsn.str.slice(47,)
qtr_gstr1_2017 = qtr_gstr1_2017.drop(['gstin_period_hsn'], axis=1)
qtr_gstr1_2017['state_cd'] = qtr_gstr1_2017.gstin_hash.str.slice(0, 2)

# Delete gstr1_2017 to free up memory
del gstr1_2017

# Exporting the quarterly dataframe to a csv file
qtr_gstr1_2017.to_csv(path+'qtr_gstr1_2017.csv')

# Convert quarterly data to annual
qtr_gstr1_2017['gstin_year_hsn'] = qtr_gstr1_2017.gstin_hash + '_' + qtr_gstr1_2017.year.astype(str) + '_' + qtr_gstr1_2017.hsn
grouped = qtr_gstr1_2017.groupby('gstin_year_hsn')['taxable_value', 'cgst', 'sgst', 'igst', 'tax_liab', 'cess'].sum()
yr_gstr1_2017 = pd.DataFrame(data=grouped)
yr_gstr1_2017 = yr_gstr1_2017.reset_index()
yr_gstr1_2017['gstin_hash'] = yr_gstr1_2017.gstin_year_hsn.str.slice(0,39)
yr_gstr1_2017['year'] = yr_gstr1_2017.gstin_year_hsn.str.slice(40, 44)
yr_gstr1_2017['hsn'] = yr_gstr1_2017.gstin_year_hsn.str.slice(45,)
yr_gstr1_2017 = yr_gstr1_2017.drop(['gstin_year_hsn'], axis=1)
yr_gstr1_2017['state_cd'] = yr_gstr1_2017.gstin_hash.str.slice(0, 2)

# Exporting the annual dataframe to a csv file
yr_gstr1_2017.to_csv(path+'yr_gstr1_2017.csv')
'''
Read data from each of the monthly files for FY 2018-19
'''
first_flag = 0
for file in files_gstr1_2018:
    file_name = path + file
    df_temp = pd.read_csv(file_name, dtype={'state_cd': object, 'rtn_prd': object})
    
    # Add CGST, SGST & IGST liabilities to get combined liabilities
    df_temp['tax_liab'] = df_temp.cgst + df_temp.sgst + df_temp.igst
    
    # Segregate month and year from period
    df_temp['year'] = df_temp.rtn_prd.str.slice(2,)
    df_temp['month'] = df_temp.rtn_prd.str.slice(0, 2)
    df_temp['year'] = df_temp['year'].astype(int)
    df_temp['month'] = df_temp['month'].astype(int)
    
    # Assign quarter for months based on dict_qtr
    df_temp['qtr'] = df_temp['month'].map(dict_qtr)
    df_temp['year'] = np.where(df_temp['qtr']==4, df_temp['year']-1, df_temp['year'])
    
    # Append the monthly data into a single dataframe
    if first_flag==0:
        gstr1_2018 = df_temp
    else:
        gstr1_2018 = gstr1_2018.append(df_temp, ignore_index=True)
    first_flag += 1

# Convert monthly data to quarterly
gstr1_2018['gstin_period_hsn'] = gstr1_2018.ann_gstin_id + '_' + gstr1_2018.year.astype(str) + '_' + gstr1_2018.qtr.astype(str) + '_' + gstr1_2018.hsn_sc
grouped = gstr1_2018.groupby('gstin_period_hsn')['taxable_value', 'cgst', 'sgst', 'igst', 'tax_liab', 'cess'].sum()
qtr_gstr1_2018 = pd.DataFrame(data=grouped)
qtr_gstr1_2018 = qtr_gstr1_2018.reset_index()
qtr_gstr1_2018['gstin_hash'] = qtr_gstr1_2018.gstin_period_hsn.str.slice(0,39)
qtr_gstr1_2018['year'] = qtr_gstr1_2018.gstin_period_hsn.str.slice(40, 44)
qtr_gstr1_2018['qtr'] = qtr_gstr1_2018.gstin_period_hsn.str.slice(45, 46)
qtr_gstr1_2018['hsn'] = qtr_gstr1_2018.gstin_period_hsn.str.slice(47,)
qtr_gstr1_2018 = qtr_gstr1_2018.drop(['gstin_period_hsn'], axis=1)
qtr_gstr1_2018['state_cd'] = qtr_gstr1_2018.gstin_hash.str.slice(0, 2)

# Delete gstr1_2018 to free up memory
del gstr1_2018

# Convert quarterly data to annual
qtr_gstr1_2018['gstin_year_hsn'] = qtr_gstr1_2018.gstin_hash + '_' + qtr_gstr1_2018.year.astype(str) + '_' + qtr_gstr1_2018.hsn
grouped = qtr_gstr1_2018.groupby('gstin_year_hsn')['taxable_value', 'cgst', 'sgst', 'igst', 'tax_liab', 'cess'].sum()
yr_gstr1_2018 = pd.DataFrame(data=grouped)
yr_gstr1_2018 = yr_gstr1_2018.reset_index()
yr_gstr1_2018['gstin_hash'] = yr_gstr1_2018.gstin_year_hsn.str.slice(0,39)
yr_gstr1_2018['year'] = yr_gstr1_2018.gstin_year_hsn.str.slice(40, 44)
yr_gstr1_2018['hsn'] = yr_gstr1_2018.gstin_year_hsn.str.slice(45,)
yr_gstr1_2018 = yr_gstr1_2018.drop(['gstin_year_hsn'], axis=1)
yr_gstr1_2018['state_cd'] = yr_gstr1_2018.gstin_hash.str.slice(0, 2)

'''
Consolidate 2017 and 2018 data
'''
temp_df = yr_gstr1_2017.append(yr_gstr1_2018, ignore_index=True)
temp_df['gstin_hsn'] = temp_df.gstin_hash + '_' + temp_df.hsn
grouped = temp_df.groupby('gstin_hsn')['taxable_value', 'cgst', 'sgst', 'igst', 'tax_liab', 'cess'].sum()
yr_gstr1_all = pd.DataFrame(data=grouped)
yr_gstr1_all = yr_gstr1_all.reset_index()
yr_gstr1_2018['gstin_hash'] = yr_gstr1_2018.gstin_hsn.str.slice(0,39)
yr_gstr1_2018['hsn'] = yr_gstr1_2018.gstin_hash.str.slice(40,)
yr_gstr1_all['state_cd'] = yr_gstr1_all.gstin_hash.str.slice(0, 2)

# Export consolidated gstr1_table12
yr_gstr1_all.to_csv('gstr1_table12.csv')

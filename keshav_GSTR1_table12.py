import pandas as pd
import numpy as np

path = '/Users/kchoudhary/Desktop/gst_data/raw_files/GSTR1/'
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

# Save monthly FY 2017-18 data as a combined csv file and then delete the dataframe
gstr1_2017.to_csv('gstr1_2017.csv', index=False)
del gstr1_2017

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

# Save monthly FY 2018-19 data as a combined csv file and then delete the data frame
gstr1_2018.to_csv('gstr1_2018.csv', index=False)
del gstr1_2018

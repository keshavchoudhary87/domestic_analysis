import pandas as pd
import numpy as np

path = 'E:\GSTN Data\Working\GSTR3B\\'
files_gstr3b_2017 = ['072017_top_10L_R3B_TABLE.csv', '082017_top_10L_R3B_TABLE.csv', '092017_top_10L_R3B_TABLE.csv', '102017_top_10L_R3B_TABLE.csv',
                     '112017_top_10L_R3B_TABLE.csv', '122017_top_10L_R3B_TABLE.csv', '012018_top_10L_R3B_TABLE.csv', '022018_top_10L_R3B_TABLE.csv',
                     '032018_top_10L_R3B_TABLE.csv']
files_gstr3b_2018 = ['042018_top_10L_R3B_TABLE.csv', '052018_top_10L_R3B_TABLE.csv', '062018_top_10L_R3B_TABLE.csv',
                     '072018_top_10L_R3B_TABLE.csv', '082018_top_10L_R3B_TABLE.csv', '092018_top_10L_R3B_TABLE.csv', '102018_top_10L_R3B_TABLE.csv',
                     '112018_top_10L_R3B_TABLE.csv', '122018_top_10L_R3B_TABLE.csv', '012019_top_10L_R3B_TABLE_12052019.csv', '022019_top_10L_R3B_TABLE_12052019.csv',
                     '032019_top_10L_R3B_TABLE_12052019.csv']

dict_qtr = {1:4, 2:4, 3:4, 4:1, 5:1, 6:1, 7:2, 8:2, 9:2, 10:3, 11:3, 12:3}

'''
Read data from each of the monthly files for FY 2017-18
'''
# jul_2017_raw = pd.read_csv(file_gstr3b_201707, dtype={'state_cd': object, 'rtn_prd': object, 'ITC_SGST': np.float64, 'ITC_CESS': np.float64, 'CASH_IGST': np.float64,
#                                                      'CASH_CGST': np.float64, 'CASH_SGST': np.float64, 'CASH_CESS': np.float64})
first_flag = 0
for file in files_gstr3b_2017:
    file_name = path + file
    # as the data is written as both string and integer converting everything to string first.
    df_temp = pd.read_csv(file_name, dtype={'state_cd': object, 'rtn_prd': object, 'ITC_SGST': object, 'ITC_CESS': object, 'CASH_IGST': object,
                                            'CASH_CGST': object, 'CASH_SGST': object, 'CASH_CESS': object})
    # Remove the preceeding and succeding whitespaces
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].str.strip()
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].str.strip()
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].str.strip()
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].str.strip()
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].str.strip()
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].str.strip()

    # Remove the extra quotes from the beginning and the end
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].replace(regex=True, to_replace = '"', value = '')

    # Convert the values from string to float
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].astype(np.float64)
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].astype(np.float64)
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].astype(np.float64)
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].astype(np.float64)
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].astype(np.float64)
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].astype(np.float64)

    # Add CGST, SGST & IGST liabilities to get combined liabilities
    df_temp['tax_liab'] = df_temp.LIAB_CGST + df_temp.LIAB_SGST + df_temp.LIAB_IGST
    df_temp['tax_cash'] = df_temp.CASH_CGST + df_temp.CASH_SGST + df_temp.CASH_IGST
    df_temp['tax_itc'] = df_temp.ITC_CGST + df_temp.ITC_SGST + df_temp.ITC_IGST
    df_temp['taxable_supply'] = df_temp['3_1_A_taxable'] + df_temp['3_1_D_TAXABLE']
    
    # segregate month and year from period
    df_temp['year'] = df_temp.rtn_prd.str.slice(2,)
    df_temp['month'] = df_temp.rtn_prd.str.slice(0, 2)
    df_temp['year'] = df_temp['year'].astype(int)
    df_temp['month'] = df_temp['month'].astype(int)
    
    # Assign quarter for months based on dict_qtr
    df_temp['qtr'] = df_temp['month'].map(dict_qtr)
    df_temp['year'] = np.where(df_temp['qtr']==4, df_temp['year']-1, df_temp['year'])
    
    # Append the monthly data into a single dataframe
    if first_flag==0:
        gstr3b_2017_raw = df_temp
    else:
        gstr3b_2017_raw = gstr3b_2017_raw.append(df_temp, ignore_index=True)
    first_flag += 1

# Identifiying the erroneous entries
gstr3b_2017_raw['etr'] = gstr3b_2017_raw.tax_liab/gstr3b_2017_raw.taxable_supply
gstr3b_2017 = gstr3b_2017_raw[gstr3b_2017_raw.etr<0.3]

# Convert monthly data to quarterly
gstr3b_2017['gstin_period_q'] = gstr3b_2017.year.astype(str) + '_' + gstr3b_2017.qtr.astype(str) + '_' + gstr3b_2017.ann_gstin_id
grouped = gstr3b_2017.groupby('gstin_period_q')['3_1_A_taxable', '3_1_A_IGST', '3_1_A_CGST', '3_1_A_SGST', '3_1_A_CESS','3_1_B_TAXABLE',
                                                '3_1_B_IGST', '3_1_B_CESS', '3_1_C_TAXABLE', '3_1_D_TAXABLE', '3_1_D_IGST', '3_1_D_CGST',
                                                '3_1_D_SGST', '3_1_D_CESS', '3_1_E_TAXABLE', 'LIAB_IGST', 'LIAB_CGST',
                                                'LIAB_SGST', 'LIAB_CESS', 'ITC_IGST', 'ITC_CGST', 'ITC_SGST', 'ITC_CESS', 'CASH_IGST',
                                                'CASH_CGST', 'CASH_SGST', 'CASH_CESS', 'taxable_supply', 'tax_liab', 'tax_cash', 'tax_itc'].sum()
qtr_gstr3b_2017 = pd.DataFrame(data=grouped)
qtr_gstr3b_2017 = qtr_gstr3b_2017.reset_index()
qtr_gstr3b_2017['year'] = qtr_gstr3b_2017.gstin_period_q.str.slice(0, 4)
qtr_gstr3b_2017['qtr'] = qtr_gstr3b_2017.gstin_period_q.str.slice(5, 6)
qtr_gstr3b_2017['gstin_hash'] = qtr_gstr3b_2017.gstin_period_q.str.slice(7,)
qtr_gstr3b_2017 = qtr_gstr3b_2017.drop(['gstin_period_q'], axis=1)
qtr_gstr3b_2017['state_cd'] = qtr_gstr3b_2017.gstin_hash.str.slice(0, 2)

# Delete gstr3b_2017 to free up memory
del gstr3b_2017_raw
del gstr3b_2017

# Exporting the quarterly dataframe to a csv file
qtr_gstr3b_2017.to_csv('qtr_gstr3b_2017.csv', index=False)

# Convert quarterly data to annual
qtr_gstr3b_2017['gstin_period'] = qtr_gstr3b_2017.year.astype(str) + '_' + qtr_gstr3b_2017.gstin_hash
grouped = qtr_gstr3b_2017.groupby('gstin_period')['3_1_A_taxable', '3_1_A_IGST', '3_1_A_CGST', '3_1_A_SGST', '3_1_A_CESS','3_1_B_TAXABLE',
                                                  '3_1_B_IGST', '3_1_B_CESS', '3_1_C_TAXABLE', '3_1_D_TAXABLE', '3_1_D_IGST', '3_1_D_CGST',
                                                  '3_1_D_SGST', '3_1_D_CESS', '3_1_E_TAXABLE', 'LIAB_IGST', 'LIAB_CGST',
                                                  'LIAB_SGST', 'LIAB_CESS', 'ITC_IGST', 'ITC_CGST', 'ITC_SGST', 'ITC_CESS', 'CASH_IGST',
                                                  'CASH_CGST', 'CASH_SGST', 'CASH_CESS', 'taxable_supply', 'tax_liab', 'tax_cash', 'tax_itc'].sum()
yr_gstr3b_2017 = pd.DataFrame(data=grouped)
yr_gstr3b_2017 = yr_gstr3b_2017.reset_index()
yr_gstr3b_2017['year'] = yr_gstr3b_2017.gstin_period.str.slice(0, 4)
yr_gstr3b_2017['gstin_hash'] = yr_gstr3b_2017.gstin_period.str.slice(5,)
yr_gstr3b_2017 = yr_gstr3b_2017.drop(['gstin_period'], axis=1)
yr_gstr3b_2017['state_cd'] = yr_gstr3b_2017.gstin_hash.str.slice(0, 2)

# Export yr_gstr3b_2017 for FY 2017-18
yr_gstr3b_2017.to_csv('yr_gstr3b_2017.csv', index=False)

'''
Read data from each of the monthly files for FY 2018-19
'''
first_flag = 0
for file in files_gstr3b_2018:
    file_name = path + file
    df_temp = pd.read_csv(file_name, dtype={'state_cd': object, 'rtn_prd': object, 'ITC_SGST': object, 'ITC_CESS': object, 'CASH_IGST': object,
                                            'CASH_CGST': object, 'CASH_SGST': object, 'CASH_CESS': object})
    # Remove the preceeding and succeding whitespaces
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].str.strip()
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].str.strip()
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].str.strip()
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].str.strip()
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].str.strip()
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].str.strip()

    # Remove the extra quotes from the beginning and the end
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].replace(regex=True, to_replace = '"', value = '')
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].replace(regex=True, to_replace = '"', value = '')

    # Convert the values from string to float
    df_temp['ITC_SGST'] = df_temp['ITC_SGST'].astype(np.float64)
    df_temp['ITC_CESS'] = df_temp['ITC_CESS'].astype(np.float64)
    df_temp['CASH_IGST'] = df_temp['CASH_IGST'].astype(np.float64)
    df_temp['CASH_CGST'] = df_temp['CASH_CGST'].astype(np.float64)
    df_temp['CASH_SGST'] = df_temp['CASH_SGST'].astype(np.float64)
    df_temp['CASH_CESS'] = df_temp['CASH_CESS'].astype(np.float64)

    # Add CGST, SGST & IGST liabilities to get combined liabilities
    df_temp['tax_liab'] = df_temp.LIAB_CGST + df_temp.LIAB_SGST + df_temp.LIAB_IGST
    df_temp['tax_cash'] = df_temp.CASH_CGST + df_temp.CASH_SGST + df_temp.CASH_IGST
    df_temp['tax_itc'] = df_temp.ITC_CGST + df_temp.ITC_SGST + df_temp.ITC_IGST
    df_temp['taxable_supply'] = df_temp['3_1_A_taxable'] + df_temp['3_1_D_TAXABLE']
    
    # segregate month and year from period
    df_temp['year'] = df_temp.rtn_prd.str.slice(2,)
    df_temp['month'] = df_temp.rtn_prd.str.slice(0, 2)
    df_temp['year'] = df_temp['year'].astype(int)
    df_temp['month'] = df_temp['month'].astype(int)
    
    # Assign quarter for months based on dict_qtr
    df_temp['qtr'] = df_temp['month'].map(dict_qtr)
    df_temp['year'] = np.where(df_temp['qtr']==4, df_temp['year']-1, df_temp['year'])
    
    # Append the monthly data into a single dataframe
    if first_flag==0:
        gstr3b_2018_raw = df_temp
    else:
        gstr3b_2018_raw = gstr3b_2018_raw.append(df_temp, ignore_index=True)
    first_flag += 1

# Identifiying the erroneous entries
gstr3b_2018_raw['etr'] = gstr3b_2018_raw.tax_liab/gstr3b_2018_raw.taxable_supply
gstr3b_2018 = gstr3b_2018_raw[gstr3b_2018_raw.etr<0.3]

# Convert monthly data to quarterly
gstr3b_2018['gstin_period_q'] = gstr3b_2018.year.astype(str) + '_' + gstr3b_2018.qtr.astype(str) + '_' + gstr3b_2018.ann_gstin_id
grouped = gstr3b_2018.groupby('gstin_period_q')['3_1_A_taxable', '3_1_A_IGST', '3_1_A_CGST', '3_1_A_SGST', '3_1_A_CESS','3_1_B_TAXABLE',
                                                '3_1_B_IGST', '3_1_B_CESS', '3_1_C_TAXABLE', '3_1_D_TAXABLE', '3_1_D_IGST', '3_1_D_CGST',
                                                '3_1_D_SGST', '3_1_D_CESS', '3_1_E_TAXABLE', 'LIAB_IGST', 'LIAB_CGST',
                                                'LIAB_SGST', 'LIAB_CESS', 'ITC_IGST', 'ITC_CGST', 'ITC_SGST', 'ITC_CESS', 'CASH_IGST',
                                                'CASH_CGST', 'CASH_SGST', 'CASH_CESS', 'taxable_supply', 'tax_liab', 'tax_cash', 'tax_itc'].sum()
qtr_gstr3b_2018 = pd.DataFrame(data=grouped)
qtr_gstr3b_2018 = qtr_gstr3b_2018.reset_index()
qtr_gstr3b_2018['year'] = qtr_gstr3b_2018.gstin_period_q.str.slice(0, 4)
qtr_gstr3b_2018['qtr'] = qtr_gstr3b_2018.gstin_period_q.str.slice(5, 6)
qtr_gstr3b_2018['gstin_hash'] = qtr_gstr3b_2018.gstin_period_q.str.slice(7,)
qtr_gstr3b_2018 = qtr_gstr3b_2018.drop(['gstin_period_q'], axis=1)
qtr_gstr3b_2018['state_cd'] = qtr_gstr3b_2018.gstin_hash.str.slice(0, 2)

# Delete gstr3b_2018 to free up memory
del gstr3b_2018_raw
del gstr3b_2018

# Exporting the quarterly dataframe to a csv file
qtr_gstr3b_2018.to_csv('qtr_gstr3b_2018.csv', index=False)

# Convert quarterly data to annual
qtr_gstr3b_2018['gstin_period'] = qtr_gstr3b_2018.year.astype(str) + '_' + qtr_gstr3b_2018.gstin_hash
grouped = qtr_gstr3b_2018.groupby('gstin_period')['3_1_A_taxable', '3_1_A_IGST', '3_1_A_CGST', '3_1_A_SGST', '3_1_A_CESS','3_1_B_TAXABLE',
                                                  '3_1_B_IGST', '3_1_B_CESS', '3_1_C_TAXABLE', '3_1_D_TAXABLE', '3_1_D_IGST', '3_1_D_CGST',
                                                  '3_1_D_SGST', '3_1_D_CESS', '3_1_E_TAXABLE', 'LIAB_IGST', 'LIAB_CGST',
                                                  'LIAB_SGST', 'LIAB_CESS', 'ITC_IGST', 'ITC_CGST', 'ITC_SGST', 'ITC_CESS', 'CASH_IGST',
                                                  'CASH_CGST', 'CASH_SGST', 'CASH_CESS', 'taxable_supply', 'tax_liab', 'tax_cash', 'tax_itc'].sum()
yr_gstr3b_2018 = pd.DataFrame(data=grouped)
yr_gstr3b_2018 = yr_gstr3b_2018.reset_index()
yr_gstr3b_2018['year'] = yr_gstr3b_2018.gstin_period.str.slice(0, 4)
yr_gstr3b_2018['gstin_hash'] = yr_gstr3b_2018.gstin_period.str.slice(5,)
yr_gstr3b_2018 = yr_gstr3b_2018.drop(['gstin_period'], axis=1)
yr_gstr3b_2018['state_cd'] = yr_gstr3b_2018.gstin_hash.str.slice(0, 2)

# Export yr_gstr3b_2018 for FY 2018-19
yr_gstr3b_2018.to_csv('yr_gstr3b_2018.csv', index=False)

'''
Consolidate 2017 and 2018 data
'''
temp_df = yr_gstr3b_2017.append(yr_gstr3b_2018, ignore_index=True)
grouped = temp_df.groupby('gstin_hash')['3_1_A_taxable', '3_1_A_IGST', '3_1_A_CGST', '3_1_A_SGST', '3_1_A_CESS','3_1_B_TAXABLE',
                                        '3_1_B_IGST', '3_1_B_CESS', '3_1_C_TAXABLE', '3_1_D_TAXABLE', '3_1_D_IGST', '3_1_D_CGST',
                                        '3_1_D_SGST', '3_1_D_CESS', '3_1_E_TAXABLE', 'LIAB_IGST', 'LIAB_CGST',
                                        'LIAB_SGST', 'LIAB_CESS', 'ITC_IGST', 'ITC_CGST', 'ITC_SGST', 'ITC_CESS', 'CASH_IGST',
                                        'CASH_CGST', 'CASH_SGST', 'CASH_CESS', 'taxable_supply', 'tax_liab', 'tax_cash', 'tax_itc'].sum()
yr_gstr3b_all = pd.DataFrame(data=grouped)
yr_gstr3b_all = yr_gstr3b_all.reset_index()
yr_gstr3b_all['state_cd'] = yr_gstr3b_all.gstin_hash.str.slice(0, 2)

# Export consolidated gstr3b data
yr_gstr3b_all.to_csv('yr_gstr3b_all.csv', index=False)

'''
Compute the cash to liability ratio
'''
# Based on all data
yr_gstr3b_all['cash_ratio'] = yr_gstr3b_all.tax_cash/yr_gstr3b_all.tax_liab
yr_gstr3b_all['cash_ratio_cess'] = yr_gstr3b_all.CASH_CESS/yr_gstr3b_all.LIAB_CESS
ratio_all = yr_gstr3b_all[['gstin_hash', 'cash_ratio', 'cash_ratio_cess']]

# Based on 2018 data
yr_gstr3b_2017['cash_ratio'] = yr_gstr3b_2017.tax_cash/yr_gstr3b_2018.tax_liab
yr_gstr3b_2017['cash_ratio_cess'] = yr_gstr3b_2017.CASH_CESS/yr_gstr3b_2018.LIAB_CESS
ratio_2017 = yr_gstr3b_2017[['gstin_hash', 'cash_ratio', 'cash_ratio_cess']]

# Based on 2018 data
yr_gstr3b_2018['cash_ratio'] = yr_gstr3b_2018.tax_cash/yr_gstr3b_2018.tax_liab
yr_gstr3b_2018['cash_ratio_cess'] = yr_gstr3b_2018.CASH_CESS/yr_gstr3b_2018.LIAB_CESS
ratio_2018 = yr_gstr3b_2018[['gstin_hash', 'cash_ratio', 'cash_ratio_cess']]

# Export ratio file
ratio_2018.to_csv('ratio_2018.csv', index=False)

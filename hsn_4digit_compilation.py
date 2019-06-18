import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

file_4digit = 'E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_4DIGIT_DATA.csv'
file_6digit = 'E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_6DIGIT_DATA.csv'
file_8digit = 'E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_8DIGIT_DATA.csv'
header_list = ['hsn', 'period', 'taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess']
dict_qtr = {1:4, 2:4, 3:4, 4:1, 5:1, 6:1, 7:2, 8:2, 9:2, 10:3, 11:3, 12:3}

'''
Read data from the 4-digit file
'''
raw_4digit = pd.read_csv(file_4digit, header=None, names=header_list, dtype={0: object})
raw_4digit['hsn_n'] = raw_4digit.hsn.astype(int)
# Finding out taxable value, and taxes for data at 1 & 2 dgit
taxable_value_2digit = raw_4digit[raw_4digit.hsn_n<100].taxable_value.sum()
cgst_2digit = raw_4digit[raw_4digit.hsn_n<100].cgst_cr.sum()
sgst_2digit = raw_4digit[raw_4digit.hsn_n<100].sgst_cr.sum()
igst_2digit = raw_4digit[raw_4digit.hsn_n<100].igst_cr.sum()
# Drop data present at 1 & 2 digit hsn
raw_4digit = raw_4digit[raw_4digit.hsn_n>99]
# Drop entries with zero taxable value
raw_4digit = raw_4digit[raw_4digit.taxable_value>0]
raw_4digit = raw_4digit.reset_index()
raw_4digit = raw_4digit.drop(['index'], axis=1)
# Calculate effective tax rate to correct some of the incorrect reporting
raw_4digit['etr'] = (raw_4digit['cgst_cr'] + raw_4digit['sgst_cr'] + raw_4digit['igst_cr'])/raw_4digit['taxable_value']
# Add zero in front of or at the end of hsn reported at 3 digit
raw_4digit.hsn = np.where(raw_4digit.hsn_n<1000,
                          np.where(raw_4digit.etr<=0.05, ("0"+ raw_4digit.hsn_n.astype(str)), (raw_4digit.hsn_n.astype(str) + "0")),
                          raw_4digit.hsn)
# Aggregating the data to 4-digit hsn level
raw_4digit['hsn_period'] = raw_4digit.hsn + '_' + raw_4digit.period
grouped = raw_4digit.groupby('hsn_period')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
df_4digit = pd.DataFrame(data=grouped)
df_4digit = df_4digit.reset_index()
df_4digit['hsn_4'] = df_4digit.hsn_period.str.slice(0, 4)
df_4digit['period'] = df_4digit.hsn_period.str.slice(5,)
df_4digit = df_4digit.drop(['hsn_period'], axis=1)

'''
Read data from the 6-digit file
'''
raw_6digit = pd.read_csv(file_6digit, header=None, names=header_list, dtype={0: object})
raw_6digit['hsn_n'] = raw_6digit.hsn.astype(int)
# Finding out taxable value, and taxes for data at 1 & 2 dgit
taxable_value_2digit = raw_6digit[raw_6digit.hsn_n<100].taxable_value.sum()
cgst_2digit = raw_6digit[raw_6digit.hsn_n<100].cgst_cr.sum()
sgst_2digit = raw_6digit[raw_6digit.hsn_n<100].sgst_cr.sum()
igst_2digit = raw_6digit[raw_6digit.hsn_n<100].igst_cr.sum()
# Drop data present at 1, 2 & 3digit hsn
raw_6digit = raw_6digit[raw_6digit.hsn_n>999]
# Drop entries with zero taxable value
raw_6digit = raw_6digit[raw_6digit.taxable_value>0]
raw_6digit = raw_6digit.reset_index()
raw_6digit = raw_6digit.drop(['index'], axis=1)
# Calculate effective tax rate to correct some of the incorrect reporting
raw_6digit['etr'] = (raw_6digit['cgst_cr'] + raw_6digit['sgst_cr'] + raw_6digit['igst_cr'])/raw_6digit['taxable_value']
# Add zero in front of or at the end of hsn reported at 5 or 6 digit
raw_6digit.hsn = np.where(raw_6digit.hsn_n<100000,
                          np.where(raw_6digit.etr<=0.05, ("0"+ raw_6digit.hsn_n.astype(str)), (raw_6digit.hsn_n.astype(str) + "0")),
                          raw_6digit.hsn_n.astype(str))
# Converting all HSN to 4 digit
raw_6digit.hsn = raw_6digit.hsn.str.slice(0, 4)
# Aggregating the data to 4-digit hsn level
raw_6digit['hsn_period'] = raw_6digit.hsn + '_' + raw_6digit.period
grouped = raw_6digit.groupby('hsn_period')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
df_6digit = pd.DataFrame(data=grouped)
df_6digit = df_6digit.reset_index()
df_6digit['hsn_4'] = df_6digit.hsn_period.str.slice(0, 4)
df_6digit['period'] = df_6digit.hsn_period.str.slice(5,)
df_6digit = df_6digit.drop(['hsn_period'], axis=1)

'''
Read data from the 8-digit file
'''
raw_8digit = pd.read_csv(file_8digit, header=None, names=header_list, dtype={0: object})
raw_8digit['hsn_n'] = raw_8digit.hsn.astype(int)
# Finding out taxable value, and taxes for data at 1 & 2 dgit
taxable_value_2digit = raw_8digit[raw_8digit.hsn_n<100].taxable_value.sum()
cgst_2digit = raw_8digit[raw_8digit.hsn_n<100].cgst_cr.sum()
sgst_2digit = raw_8digit[raw_8digit.hsn_n<100].sgst_cr.sum()
igst_2digit = raw_8digit[raw_8digit.hsn_n<100].igst_cr.sum()
# Drop data present at upto 5 digit hsn
raw_8digit = raw_8digit[raw_8digit.hsn_n>99999]
# Drop entries with zero taxable value
raw_8digit = raw_8digit[raw_8digit.taxable_value>0]
raw_8digit = raw_8digit.reset_index()
raw_8digit = raw_8digit.drop(['index'], axis=1)
# Calculate effective tax rate to correct some of the incorrect reporting
raw_8digit['etr'] = (raw_8digit['cgst_cr'] + raw_8digit['sgst_cr'] + raw_8digit['igst_cr'])/raw_8digit['taxable_value']
# Add zero in front of or at the end of hsn reported at 7 or 8 digit
raw_8digit.hsn = np.where(raw_8digit.hsn_n<10000000,
                          np.where(raw_8digit.etr<=0.05, ("0"+ raw_8digit.hsn_n.astype(str)), (raw_8digit.hsn_n.astype(str) + "0")),
                          raw_8digit.hsn_n.astype(str))
# Converting all HSN to 4 digit
raw_8digit.hsn = raw_8digit.hsn.str.slice(0, 4)
# Aggregating the data to 4-digit hsn level
raw_8digit['hsn_period'] = raw_8digit.hsn + '_' + raw_8digit.period
grouped = raw_8digit.groupby('hsn_period')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
df_8digit = pd.DataFrame(data=grouped)
df_8digit = df_8digit.reset_index()
df_8digit['hsn_4'] = df_8digit.hsn_period.str.slice(0, 4)
df_8digit['period'] = df_8digit.hsn_period.str.slice(5,)
df_8digit = df_8digit.drop(['hsn_period'], axis=1)

'''
Consolidating all data at 4-digit HSN
'''
# APPEND all data to get a consolidated dataframe at 4-digit hsn
mth_data_4digit = df_4digit.append([df_6digit, df_8digit], ignore_index=True)
# Seperate Year and month from period
mth_data_4digit['year'] = mth_data_4digit.period.str.slice(0, 4).astype(int)
mth_data_4digit['month'] = mth_data_4digit.period.str.slice(5,).astype(int)
# Assign quarter for months based on dict_qtr
mth_data_4digit['qtr'] = mth_data_4digit['month'].map(dict_qtr)
mth_data_4digit['year'] = np.where(mth_data_4digit['qtr']==4, mth_data_4digit['year']-1, mth_data_4digit['year'])
# Convert data to quarterly figures
mth_data_4digit['hsn_period_q'] = mth_data_4digit.hsn_4 + '_' + mth_data_4digit.year.astype(str) + '_' + mth_data_4digit.qtr.astype(str)
grouped = mth_data_4digit.groupby('hsn_period_q')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
qtr_data_4digit = pd.DataFrame(data=grouped)
qtr_data_4digit = qtr_data_4digit.reset_index()
qtr_data_4digit['hsn_4'] = qtr_data_4digit.hsn_period_q.str.slice(0, 4)
qtr_data_4digit['year'] = qtr_data_4digit.hsn_period_q.str.slice(5, 9)
qtr_data_4digit['qtr'] = qtr_data_4digit.hsn_period_q.str.slice(10,)
qtr_data_4digit = qtr_data_4digit.drop(['hsn_period_q'], axis=1)

'''
Projecting for 2019 based on growth of 2018 over 2017
'''
# Seperate q1 data for 2018 from the rest to compute growfactor, as q1 figures for 2017 is not available
q1_2018 = qtr_data_4digit[(qtr_data_4digit.year == '2018') & (qtr_data_4digit.qtr =='1')]
three_qtr = qtr_data_4digit.append(q1_2018).drop_duplicates(keep=False)
# Add data for the 3 quarters to calculate grow factor
three_qtr['hsn_year'] = three_qtr.hsn_4 + '_' + three_qtr.year
grouped = three_qtr.groupby('hsn_year')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
three_qtr_y = pd.DataFrame(data=grouped)
three_qtr_y = three_qtr_y.reset_index()
three_qtr_y['hsn_4'] = three_qtr_y.hsn_year.str.slice(0, 4)
three_qtr_y['year'] = three_qtr_y.hsn_year.str.slice(5,)
three_qtr_y = three_qtr_y.drop(['hsn_year'], axis=1)
# Calculate the grow factors by defiding corresponding figures of 2018 by the figures of 2017
gf_tax_val = three_qtr_y[three_qtr_y.year == '2018'].taxable_value.sum()/three_qtr_y[three_qtr_y.year == '2017'].taxable_value.sum()
gf_cgst = three_qtr_y[three_qtr_y.year == '2018'].cgst_cr.sum()/three_qtr_y[three_qtr_y.year == '2017'].cgst_cr.sum()
gf_sgst = three_qtr_y[three_qtr_y.year == '2018'].sgst_cr.sum()/three_qtr_y[three_qtr_y.year == '2017'].sgst_cr.sum()
gf_igst = three_qtr_y[three_qtr_y.year == '2018'].igst_cr.sum()/three_qtr_y[three_qtr_y.year == '2017'].igst_cr.sum()
gf_cess = three_qtr_y[three_qtr_y.year == '2018'].cess.sum()/three_qtr_y[three_qtr_y.year == '2017'].cess.sum()
# Project 2019 figures by multiplying grow factors to 2018 data
qtr_pred_2019 = qtr_data_4digit[qtr_data_4digit.year == '2018']
qtr_pred_2019.loc[:,'year'] = '2019'
qtr_pred_2019.taxable_value = qtr_pred_2019.taxable_value * gf_tax_val
qtr_pred_2019.cgst_cr = qtr_pred_2019.cgst_cr * gf_igst
qtr_pred_2019.sgst_cr = qtr_pred_2019.sgst_cr * gf_sgst
qtr_pred_2019.igst_cr = qtr_pred_2019.igst_cr * gf_igst
qtr_pred_2019.cess = qtr_pred_2019.cess * gf_cess
# Append 2019 projected data to the quarterly data
qtr_data_final = qtr_data_4digit.append(qtr_pred_2019, ignore_index=True)

'''
Merge rates to the final data file
'''
# read the rates from file
rates = pd.read_stata('rates.dta')
# Merge with the data to get rates for hsn_4
qtr_data_final = qtr_data_final.merge(right=rates, how='left', on='hsn_4')
# Merge at 2 digit level to get rates for the rest of the cases
rates_2 = rates[rates.hsn_4.str.len()==2]
rates_2 = rates_2.rename({'hsn_4': 'hsn_2', 'rate': 'rate_2'}, axis=1)
qtr_data_final['hsn_2'] = qtr_data_final.hsn_4.str.slice(0, 2)
qtr_data_final = qtr_data_final.merge(right=rates_2, how='left', on='hsn_2')
qtr_data_final.rate = np.where(qtr_data_final.rate.isnull(),
                               qtr_data_final.rate_2, qtr_data_final.rate)
# Separate data for the services sector
qtr_data_services = qtr_data_final[qtr_data_final.hsn_2 == '99']
qtr_data_services.to_csv('qtr_data_services.csv')
qtr_data_final = qtr_data_final.drop(['hsn_2', 'rate_2'], axis=1)
qtr_data_final = qtr_data_final.dropna(subset=['rate'])
# Multiply rates with 2 to get the igst rate percentage
qtr_data_final.rate = qtr_data_final.rate * 2
qtr_data_final['tax_lia'] = qtr_data_final.cgst_cr + qtr_data_final.sgst_cr + qtr_data_final.igst_cr

'''
Compute revenue neutral rate for 12 and 18% item
'''
qtr_data_conv = qtr_data_final
# qtr_data_conv = qtr_data_final[qtr_data_final.year=='2019']
qtr_data_18_12 = qtr_data_conv[((qtr_data_conv.rate>=0.12) & (qtr_data_conv.rate<=0.18))]
temp = qtr_data_18_12.groupby('rate')['taxable_value', 'tax_lia'].sum()
temp_df = pd.DataFrame(data=temp)
temp_df = temp_df.reset_index()
temp_df['tax'] = temp_df.rate * temp_df.taxable_value
rnr_18_12 = round((temp_df.tax.sum()/temp_df.taxable_value.sum())*100, 2)
print('\n Convergence Rate for 12% and 18% is ' + str(rnr_18_12) + '%')

'''
Compute revenue neutral rate for 5 and 12 % item
'''
qtr_data_5_12 = qtr_data_conv[((qtr_data_conv.rate>=0.05) & (qtr_data_conv.rate<=0.12))]
temp = qtr_data_5_12.groupby('rate')['taxable_value', 'tax_lia'].sum()
temp_df = pd.DataFrame(data=temp)
temp_df = temp_df.reset_index()
temp_df['tax'] = temp_df.rate * temp_df.taxable_value
rnr_5_12 = round((temp_df.tax.sum()/temp_df.taxable_value.sum())*100, 2)
print('\n Convergence Rate for 5% and 12% is ' + str(rnr_5_12) + '%')

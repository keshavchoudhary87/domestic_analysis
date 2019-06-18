import pandas as pd
import numpy as np

filename_data = 'yr_gstr1_2018.csv'
filename_ratio = 'ratio_2018.csv'

# Read data file
gstr1_2018 = pd.read_csv(filename_data)
# Read ratio file
ratio_2018 = pd.read_csv(filename_ratio)
ratio_2018 = ratio_2018.fillna(0)
data_2018 = gstr1_2018.merge(right=ratio_2018, how='left', on='gstin_hash')
data_2018 = data_2018.fillna(0)

# Calculating HSN wise cash ratio as well as cash ratio cess
data_2018['liab_x_cash_ratio'] = data_2018['tax_liab'] * data_2018['cash_ratio']
data_2018['liab_x_cess_ratio'] = data_2018['cess'] * data_2018['cash_ratio_cess']
grouped = data_2018.groupby('hsn')['taxable_value', 'tax_liab', 'cess', 'liab_x_cash_ratio', 'liab_x_cess_ratio'].sum()
hsn_ratio = pd.DataFrame(data=grouped)
hsn_ratio = hsn_ratio.reset_index()
hsn_ratio['cash_ratio'] = hsn_ratio['liab_x_cash_ratio']/hsn_ratio['tax_liab']
hsn_ratio['cash_ratio_cess'] = hsn_ratio['liab_x_cess_ratio']/hsn_ratio['tax_liab']
hsn_ratio = hsn_ratio.drop(['liab_x_cash_ratio', 'liab_x_cess_ratio'], axis=1)
# Export  ratio to excel file
writer = pd.ExcelWriter('hsn_wise_ratio.xlsx')
hsn_ratio.to_excel(writer,'17.06.2019', index=False)
writer.save()

# Find out GSTINs having single HSNs
grouped = data_2018.groupby('gstin_hash')['hsn'].count()
gstin_hsn_count = pd.DataFrame(data=grouped)
gstin_hsn_count = gstin_hsn_count.reset_index()
gstin_hsn_count = gstin_hsn_count.rename(columns = {'hsn': 'hsn_count'})
gstin_single_hsn = gstin_hsn_count[gstin_hsn_count.hsn_count == 1]
gstin_single_hsn = data_2018.merge(right=gstin_single_hsn, how='left', on='gstin_hash')
gstin_single_hsn = gstin_single_hsn[gstin_single_hsn.hsn_count == 1]

# Exporting the GSTINs with single HSN to a csv file
writer = pd.ExcelWriter('single_hsn.xlsx')
gstin_single_hsn.to_excel(writer,'Sheet1', index=False)
writer.save()

'''
data_2018['hsn_len'] = data_2018.hsn.str.len()
grouped = data_2018.groupby('hsn_len')['gstin_hash'].count()
hsn_len_group = pd.DataFrame(data=grouped)
hsn_len_group = hsn_len_group.reset_index()
'''

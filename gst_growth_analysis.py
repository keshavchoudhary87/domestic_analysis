import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

file_mth_hsn4digit = 'E:\GSTN Data\Working\monthly_hsn_data_18062019.csv'

# Read monthly data consolidated at 4-digit HSN
data_mth_hsn4 = pd.read_csv(file_mth_hsn4digit, dtype={'hsn_4': object, 'period': object})

# consolidate data at two digit HSN level
data_mth_hsn4['hsn_2'] = data_mth_hsn4['hsn_4'].str.slice(0, 2)
data_mth_hsn4['hsn_period'] = data_mth_hsn4['hsn_2'] + '_' + data_mth_hsn4['period']
grouped = data_mth_hsn4.groupby('hsn_period')['taxable_value', 'cgst_cr', 'sgst_cr', 'igst_cr', 'cess'].sum()
mth_data_2digit = pd.DataFrame(data=grouped)
mth_data_2digit = mth_data_2digit.reset_index()
mth_data_2digit['hsn_2'] = mth_data_2digit.hsn_period.str.slice(0, 2)
mth_data_2digit['period'] = mth_data_2digit.hsn_period.str.slice(3,)

# mth_data_2digit = mth_data_2digit.drop(['hsn_period'], axis=1)

# tax liability as the sum of CGST, SGST & IGST
mth_data_2digit['tax_liab'] = mth_data_2digit.cgst_cr + mth_data_2digit.sgst_cr + mth_data_2digit.igst_cr
mth_data_2digit = mth_data_2digit.sort_values(by=['hsn_2', 'period'], ascending = True, axis = 0)

# Export monthly data at 2-digit hsn
mth_data_2digit.to_csv('monthly_data_hsn2.csv', index=False)

# Calculate monthly growth rates at HSN 2-digit level
temp = mth_data_2digit.set_index(['period', 'hsn_2']).tax_liab
data_with_growth_rate = temp.groupby(level='hsn_2').pct_change()
data_with_growth_rate = data_with_growth_rate.reset_index()
data_with_growth_rate['tax_liab'] = np.where(data_with_growth_rate.period=='2017-07', 0,
                                             data_with_growth_rate.tax_liab)
data_with_growth_rate = data_with_growth_rate.fillna(0)
data_with_growth_rate = data_with_growth_rate.rename({'tax_liab':'m_growth'}, axis=1)
data_with_growth_rate['hsn_period'] = data_with_growth_rate['hsn_2'] + '_' + data_with_growth_rate['period']
growth_data = data_with_growth_rate[['hsn_period', 'm_growth']]
data_mth_hsn2 = mth_data_2digit.merge(right=growth_data, how='inner', on='hsn_period')

# sort the data by decreasing contribution to the tax liability
grouped = data_mth_hsn2.groupby('hsn_2')['tax_liab'].sum()
hsn_liab = pd.DataFrame(data=grouped)
hsn_liab = hsn_liab.reset_index()
hsn_liab = hsn_liab.sort_values(by='tax_liab', ascending = False, axis = 0)

# Filter tof 15 HSN by contribution
top_15_hsn = hsn_liab.head(15)
top_15 = data_mth_hsn2.merge(right=top_15_hsn, how='inner', on='hsn_2')
top_15 = top_15.rename({'tax_liab_y':'total_tax_liab'}, axis=1)

# Export data for top 15 HSN
top_15.to_csv('top15hsn_monthly.csv', index=False)

# Plot the monthly growth rate for each chapter
chapters = hsn_liab.hsn_2.unique()
chapters.sort()
for hsn in chapters:
    x = data_mth_hsn2[data_mth_hsn2.hsn_2==hsn].period
    y = data_mth_hsn2[data_mth_hsn2.hsn_2==hsn].m_growth
    plt.plot(x,y)
    plt.title('Monthly Growth for Chapter-' + hsn)
    plt.xlabel('YYYY-MM')
    plt.ylabel('Monthly growth')
    plt.xticks(['2017-10', '2018-02', '2018-06', '2018-10', '2019-02'],)
    # plt.show()
    plt.savefig('E:\GSTN Data\Working\Plots\Growth_Ch' + hsn + '.png')
    plt.clf()




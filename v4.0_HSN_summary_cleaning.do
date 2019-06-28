* Change Working Directory
cd "/Users/kchoudhary/Desktop/gst_data"

* Read in HSN raw data and basic cleaning
cd "raw_files"
import delimited "hsn_all_hsn_raw.csv", stringcols(1) clear
sort hsn
drop hsn_n //Don't need this variable at the moment//
replace cess = cess/10000000 //Converting cess to crore//
rename cess cess_cr
rename taxable_value taxable_value_cr //Taxable Value is already in Crore//
rename period time_period
egen period = group(time_period) //Period runs from 1 (Jul 2017) to 21 (March 2019)
gen year = substr(time_period, 1,4) //Actual year of the data//
gen month = substr(time_period, -2, .) //Actual month of the data//
destring year, replace 
destring month, replace 

* Cleaning the data
gen chapter = substr(hsn,1,2)
gen tax_liab_cr = (cgst_cr + sgst_cr + igst_cr)
drop if chapter == "00" //Total tax liab = 15k crore approx in 21 months//
drop if chapter == "77" //Total tax liab = 112 cr in 21 months//
gen etr = tax_liab_cr/ taxable_value_cr
drop if etr == . //Total tax_liab = 0.15 cr in 21 months//
drop if etr >= 0.3 //Total tax_liab = 99k crore out of 90L crore in 21 months//





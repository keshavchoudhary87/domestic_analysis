* Change working directory
cd "/Users/kchoudhary/Desktop/gst_data"
set more off

/*
* Part 1 - Reading in raw data and creating a master dataset of GSTR1 and GSTR3B
* Import GSTR-1 monthly data for FY 17-18 and FY 18-19
* CSV files were created from Python and appended in one file
* See keshav_GSTR1_table12.py and keshav_GSTR3B.py
* FY17-18
cd "raw_files" 
import delimited "gstr1_2017.csv", encoding(ISO-8859-1) clear
save "gstr1_2017.dta", replace

* FY18-19
import delimited "gstr1_2018.csv", encoding(ISO-8859-1) clear
save "gstr1_2018.dta", replace

* Creating master gstr1 dataset
use "gstr1_2017.dta", clear
append using "gstr1_2018.dta"
erase "gstr1_2017.dta"
erase "gstr1_2018.dta"
save "gstr1_master.dta", replace

* Import GSTR-3B monthly data for FY17-18 and FY18-19
* CSV files were created from Python
* See keshav_GSTR1_table12.py and keshav_GSTR3B.py
* FY17-18
cd "raw_files" 
import delimited "gstr3b_2017.csv", encoding(ISO-8859-1) clear
save "gstr3b_2017.dta", replace

* FY 18-19
import delimited "gstr3b_2018.csv", encoding(ISO-8859-1) clear
save "gstr3b_2018.dta", replace

* Creating master gstr3b dataset
use "gstr3b_2017.dta", clear
append using "gstr3b_2018.dta"
save "gstr3b_master.dta", replace
erase "gstr3b_2017.dta"
erase "gstr3b_2018.dta"
keep ann_gstin_id tax_liab tax_cash tax_itc taxable_supply year month qtr //This data contains only those observations where etr < 30%//


* Part 2 - Monthly cash ratios
* Do basic changes in GSTR1 master then create monthly files in loop
cd "raw_files"
use "gstr1_master.dta", clear
cd .. 
cd "intermediate_files"
drop state_cd rtn_prd  
rename igst igst_1
rename cgst cgst_1
rename sgst sgst_1
rename taxable_value taxable_value_1
rename cess cess_1
rename tax_liab tax_liab_1
replace year = year+1 if qtr==4 //Currently Jan 2019 is shown as 2018 01 04 (Year Month Qtr) //
drop qtr
gen period = .
replace period = 1 if (year==2017 & month==7)
replace period = 2 if (year==2017 & month==8)
replace period = 3 if (year==2017 & month==9)
replace period = 4 if (year==2017 & month==10)
replace period = 5 if (year==2017 & month==11)
replace period = 6 if (year==2017 & month==12)
replace period = 7 if (year==2018 & month==1)
replace period = 8 if (year==2018 & month==2)
replace period = 9 if (year==2018 & month==3)
replace period = 10 if (year==2018 & month==4)
replace period = 11 if (year==2018 & month==5)
replace period = 12 if (year==2018 & month==6)
replace period = 13 if (year==2018 & month==7)
replace period = 14 if (year==2018 & month==8)
replace period = 15 if (year==2018 & month==9)
replace period = 16 if (year==2018 & month==10)
replace period = 17 if (year==2018 & month==11)
replace period = 18 if (year==2018 & month==12)
replace period = 19 if (year==2019 & month==1)
replace period = 20 if (year==2019 & month==2)
replace period = 21 if (year==2019 & month==3)
drop year month
rename period month
save "gstr1_merge.dta", replace
* Creating GSTR1 monthly files
forvalues x=1/21 {
use "gstr1_merge.dta", clear
keep if month==`x'
save "`x'_gstr1.dta", replace
}
erase "gstr1_merge.dta"


* Do basic changes in GSTR3B master and create monthly files in loop
cd ..
cd "raw_files" 
use "gstr3b_master.dta", clear
cd ..
cd "intermediate_files"
keep ann_gstin_id tax_liab tax_cash tax_itc taxable_supply year month qtr
rename tax_liab tax_liab_3b
rename tax_cash tax_cash_3b
rename tax_itc tax_itc_3b
rename taxable_supply taxable_value_3b
gen cash_ratio = tax_cash_3b/tax_liab_3b
duplicates drop ann_gstin_id year qtr month taxable_value_3b, force
replace year = year+1 if qtr==4
drop qtr
gen period = .
replace period = 1 if (year==2017 & month==7)
replace period = 2 if (year==2017 & month==8)
replace period = 3 if (year==2017 & month==9)
replace period = 4 if (year==2017 & month==10)
replace period = 5 if (year==2017 & month==11)
replace period = 6 if (year==2017 & month==12)
replace period = 7 if (year==2018 & month==1)
replace period = 8 if (year==2018 & month==2)
replace period = 9 if (year==2018 & month==3)
replace period = 10 if (year==2018 & month==4)
replace period = 11 if (year==2018 & month==5)
replace period = 12 if (year==2018 & month==6)
replace period = 13 if (year==2018 & month==7)
replace period = 14 if (year==2018 & month==8)
replace period = 15 if (year==2018 & month==9)
replace period = 16 if (year==2018 & month==10)
replace period = 17 if (year==2018 & month==11)
replace period = 18 if (year==2018 & month==12)
replace period = 19 if (year==2019 & month==1)
replace period = 20 if (year==2019 & month==2)
replace period = 21 if (year==2019 & month==3)
drop year month
rename period month
save "gstr3b_merge.dta", replace
forvalues x=1/21 {
use "gstr3b_merge.dta", clear
keep if month==`x'
save "`x'_gstr3b.dta", replace
}
erase "gstr3b_merge.dta"
*/

* Creating GSTR-3B monthly files
forvalues x=1/21 {
use "`x'_gstr3b.dta", clear
gen itc_ratio = tax_itc_3b/ tax_liab_3b
count
keep ann_gstin_id cash_ratio itc_ratio taxable_value_3b tax_liab_3b
save "`x'_gstr3b.dta", replace
}

* Merging GSTR3B with GSTR1
forvalues x= 1/21 {
use "`x'_gstr1.dta", clear
merge m:1 ann_gstin_id using "`x'_gstr3b.dta"
save "`x'_linked_gstr1_3b.dta", replace
erase "`x'_gstr1.dta"
erase "`x'_gstr3b.dta"
}


* Cleaning up linked files
* Keeping only data where GSTR1 matched with GSTR3B
forvalues x =1/21 {
use "`x'_linked_gstr1_3b.dta", clear
keep if _merge==3
drop _merge
save "`x'_linked_gstr_cleaned.dta", replace
}

* Calculating month-wise weighted cash-credit ratio-- no manipulation of HSN. Taken on as-is basis.
forvalues x = 1/21 {
use "`x'_linked_gstr_cleaned.dta", clear
bysort hsn_sc: asgen cash_ratio_hsn = cash_ratio, weights(tax_liab_1)
collapse (sum) taxable_value_1 tax_liab_1 (mean) cash_ratio_hsn, by(hsn_sc) //Keeping the HSN wise total taxable value and tax liability//
save "`x'_cash_ratio.dta", replace
erase "`x'_linked_gstr_cleaned.dta"
}

* Cleaning of HSN
forvalues x = 1/21 {
use "`x'_cash_ratio.dta", clear
gen chapter = substr(hsn_sc,1,2)
gen hsn_final = hsn_sc
replace hsn_final = "00" if chapter == "00" //We label 00 as the error chapter//
replace hsn_final = "00" if chapter == "No" 
replace hsn_final = "00" if chapter == ""
replace hsn_final = "00" if chapter == "77"






























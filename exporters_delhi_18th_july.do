* Change Directory
* Reading in raw data and saving in Stata_raw_files
set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\raw_files\delhi_export"
import delimited "delhi_exp_2a_dtl_13072019.csv", encoding(ISO-8859-2) clear
cd ..
cd ..
cd "stata_raw_files"
save "delhi_2a.dta", replace
cd ..
cd "raw_files\delhi_export"
import delimited "delhi_exp_r1_6a_dtl_13072019.csv", clear 
cd ..
cd ..
cd "stata_raw_files"
save "delhi_6a.dta", replace

* Saving in Intermediate files folder and basic cleaning
* Delhi-2A
use "delhi_2a.dta", clear
cd ..
cd "intermediate_files"
tostring rtn_prd, replace 
gen cal_year = substr(rtn_prd, -4,.) //Calendar year//
destring cal_year, replace
gen digit =strlen(rtn_prd)
gen cal_month = substr(rtn_prd,1,1) if digit==5 //Calendar month//
replace cal_month = substr(rtn_prd,1,2) if digit==6
replace rtn_prd = "0"+rtn_prd if digit==5 //Bringing to the form MMYYYY//
destring cal_month, replace
drop digit
rename rtn_prd time
merge m:1 time using "month_names2.dta" //Obtain month numbering from 1 to 23//
drop _merge
rename time rtn_prd 
destring rtn_prd, replace
foreach x of varlist txval igst cgst sgst cess {
rename `x' `x'_2a
} 
save "delhi_2a.dta", replace

* Delhi-6A
cd ..
cd "stata_raw_files"
use "delhi_6a.dta", clear
cd ..
cd "intermediate_files"
tostring rtn_prd, replace 
gen cal_year = substr(rtn_prd, -4,.) //Calendar year//
destring cal_year, replace
gen digit =strlen(rtn_prd)
gen cal_month = substr(rtn_prd,1,1) if digit==5 //Calendar month//
replace cal_month = substr(rtn_prd,1,2) if digit==6
replace rtn_prd = "0"+rtn_prd if digit==5 //Bringing to the form MMYYYY//
destring cal_month, replace
drop digit
rename rtn_prd time
merge m:1 time using "month_names2.dta" //Obtain month numbering from 1 to 23//
drop _merge
rename time rtn_prd
foreach x of varlist txval igst cess {
rename `x' `x'_6a
}  
destring rtn_prd, replace 
save "delhi_6a.dta", replace

* Growth analysis on 6A data
use "delhi_6a.dta", clear
keep if wp_flag=="Y"
sort ann_gstin_id month

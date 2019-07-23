set more off

* Reading in monthly data of GSTR-1 and saving in Stata format
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
cd "E:\GSTN Data\Raw\latest-02-07-2019\raw_files\R1_12_DATA_TOP_10L_LAIB_2018_19"
import delimited "`x'_top_10L_2018_19_R1_TABLE_12.csv", clear
cd "E:\GSTN Data\Raw\latest-02-07-2019\stata_raw_files"
gen period = "`x'"
save "`x'_gstr1.dta", replace
}

* Reading in monthly data of GSTR-3B and saving in Stata format
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
cd "E:\GSTN Data\Raw\latest-02-07-2019\raw_files\R3B_DATA_TOP_10L_LAIB_2018_19"
import delimited "`x'_top_10L_2018_19_R3B_TABLE_28062019.csv", clear
cd "E:\GSTN Data\Raw\latest-02-07-2019\stata_raw_files"
gen period = "`x'"
save "`x'_gstr3b.dta", replace
}

* Testing whether GSTR1 fully matches with GSTR 3B
* Collapsing GSTR1
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1.dta", clear
collapse (sum) taxable_value igst cgst sgst cess, by (ann_gstin_id)
gen period = "`x'"
cd ..
cd "intermediate_files"
save "`x'_gstr1_collapsed.dta", replace
cd ..
cd "stata_raw_files"
}

* Merging collapsed GSTR1 with GSTR3B
cd ..
cd "intermediate_files"
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_collapsed.dta", replace
merge 1:1 ann_gstin_id using "`x'_gstr3b.dta"
save "`x'_gstr1_gstr3b_collapsed_merged.dta", replace
}

* Reading in and saving Aggregate HSN summary data
* Reading in 4 digit HSN summary data
cd ..
cd "raw_files\HSN_DATA_09052019"
import delimited "HSN_4DIGIT_DATA.csv", stringcols(1) clear
cd ..
cd ..
cd "stata_raw_files"
save "raw_hsn_4_digit.dta", replace

* Reading in 6-digit aggregate HSN summary data
cd ..
cd "raw_files\HSN_DATA_09052019"
import delimited "HSN_6DIGIT_DATA.csv", stringcols(1) clear
cd ..
cd ..
cd "stata_raw_files"
save "raw_hsn_6_digit.dta", replace

* Reading in 8-digit aggregate HSN summary data
cd ..
cd "raw_files\HSN_DATA_09052019"
import delimited "HSN_8DIGIT_DATA.csv", stringcols(1) clear
cd ..
cd ..
cd "stata_raw_files"
save "raw_hsn_8_digit.dta", replace

* Renaming variables in 4-digit, 6-digit and 8-digit files and saving
foreach x in raw_hsn_4_digit.dta raw_hsn_6_digit.dta raw_hsn_8_digit.dta {
use "`x'", clear
rename v1 hsn
rename v2 period
rename v3 taxable_value_cr
rename v4 cgst_cr
rename v5 sgst_cr
rename v6 igst_cr
rename v7 cess_rs
gen cess_cr = cess_rs/10000000
drop cess_rs
sort hsn period
gen year = substr(period, 1,4)
gen month = substr(period, 6,.)
destring year, replace
destring month, replace
gen fy = .
replace fy = 2017 if (year==2017 & month >= 7)
replace fy = 2017 if (year==2018 & month <=3)
replace fy = 2018 if (year==2018 & month >=4)
replace fy = 2018 if (year==2019 & month <=3)
gen liab_cr = cgst_cr + igst_cr + sgst_cr
save "`x'", replace
}

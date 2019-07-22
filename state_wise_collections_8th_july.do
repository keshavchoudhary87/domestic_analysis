* Set working directory
set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\intermediate_files"

* Import individual level monthly GSTR1 files containing cash credit ratios from GSTR-3B
* Then collapse by HSN and State for each month
* Contains only data where GSTR-1 and GSTR-3B matched
* Dropped ETR> 300% and ETR < 0
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_with_cash_ratio.dta", clear
keep if _merge==3 //Keep if GSTR1 and GSTR3B both present
drop _merge
* replace liab = cgst+sgst //************// 
gen collection = (cash_ratio*liab)/100
collapse (sum) taxable_value liab collection, by (state_cd hsn_sc) //State-wise HSN-wise data//
gen time = "`x'"
save "`x'_gstr1_state_wise_hsn_wise_collections.dta", replace
}

* Append all data files into one data file
use "072017_gstr1_state_wise_hsn_wise_collections.dta", clear
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_gstr1_state_wise_hsn_wise_collections.dta"
}
save "complete_gstr1_state_wise_hsn_wise_collections.dta", replace

* Cleaning
use "complete_gstr1_state_wise_hsn_wise_collections.dta", clear
gen taxable_value_cr = taxable_value/10000000
gen liab_cr = liab/10000000
gen collections_cr = collection/10000000
drop taxable_value liab collection
gen etr = liab_cr/taxable_value_cr
drop if etr < 0 //Dropping HSN's with negative ETRs"
drop if etr > 3 //Dropping HSN's with ETR > 300%//
merge m:1 time using "month_names2.dta"
drop _merge
drop time
gen fy = .
replace fy = 2017 if (month >=1 & month <=9)
replace fy = 2018 if (month >=10 & month <=21)
replace fy = 2019 if (month >=22)
gen digit = strlen(hsn_sc) //Number of digits reported//
gen chapter_raw = substr(hsn_sc,1,2) //Raw chapter as reported in the data set//
* Denote error chapter as 0
gen chapter_cleaned = chapter_raw 
destring chapter_cleaned, replace force
replace chapter_cleaned = 0 if chapter_cleaned == . //Error if chapter=.//
replace chapter_cleaned = 0 if chapter_cleaned == 77 //Error if chapter=77//
table fy, c(sum collections_cr) format(%10.0fc)
* Collapsing data to chapter-state-month level
collapse (sum) taxable_value_cr liab_cr collections_cr, by (state_cd month chapter_cleaned)
* Merging state names
merge m:1 state_cd using "state_names.dta"
drop if _merge==2 //Andhra Pradesh (Before Division) is not present in data//
replace state_name = "Unknown" if state_cd == 97 //We don't know yet which state this is//
drop _merge
save "complete_chapter_state_month_gstr1_collections.dta", replace

* Calculating all-India values and calculate month-wise HSN-wise percentages
use "complete_chapter_state_month_gstr1_collections.dta", clear
collapse (sum) taxable_value_cr liab_cr collections_cr , by(month chapter_cleaned)
gen state_name = "All India"
gen state_cd = 999
append using "complete_chapter_state_month_gstr1_collections.dta"
sort month chapter_cleaned state_cd 
by month chapter_cleaned: gen percent_collections = 100*(collections_cr[_n]/collections_cr[_N]) //Each month and Chapter as proportion of all India//
* Collapsing monthly data to over-all totals of 23 months and calculating HSN-wise percentages
collapse (sum) taxable_value_cr liab_cr collections_cr, by(chapter_cleaned state_cd state_name)
sort chapter_cleaned state_cd
by chapter_cleaned: gen percent_collections = 100*(collections_cr[_n]/collections_cr[_N]) //Chapter as proportion of all India//
* Merging chapter names
merge m:1 chapter_cleaned using "chapter_names.dta"
drop if _merge==2 //77 did not merge because we have already included it in Chapter 77//
drop _merge
* Generating Graphs for chapters, state-wise
replace state_cd = 38 if state_cd == 97
replace state_cd = 39 if state_cd == 999
labmask state_cd, values(state_name) //Label of State Name//
labmask chapter_cleaned, values(desc) //Label of chapter description//
cd ..
cd "output_files"
* Run from 1 to 76 and then from 78 to 99 as 77 does not exist
forvalues x = 1/76 {
local f0: label chapter_cleaned `x'
graph hbar (asis) percent_collections if (chapter_cleaned==`x' & state_cd < 39), over(state_cd, lab(angle(horizontal) labsize(vsmall))) ///
blabel (total, angle(45) size(vsmall) format (%2.1f)) title("Chapter `x' - `f0'") subtitle("State-Wise Total Collections (pre-IGST settlement)") ///
ytitle("Percentage of All-India collections")
graph export "`x'_state_wise_collections_intra.png", width(10000) replace
}
forvalues x = 78/99 {
local f0: label chapter_cleaned `x'
graph hbar (asis) percent_collections if (chapter_cleaned==`x' & state_cd < 39), over(state_cd, lab(angle(horizontal) labsize(vsmall))) ///
blabel (total, angle(45) size(vsmall) format (%2.1f)) title("Chapter `x' - `f0'") subtitle("State-Wise Total Collections (pre-IGST settlement)") ///
ytitle("Percentage of All-India collections")
graph export "`x'_state_wise_collections_intra.png", width(10000) replace
}
cd ..
cd "intermediate_files"






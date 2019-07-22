set more off

* Change working directory
cd "E:\GSTN Data\Raw\latest-02-07-2019\stata_raw_files"

* WORKING WITH TOP 10 LAKH TAXPAYER DATA
* Creating combined, appended data of GSTR-3B
use "072017_gstr3b.dta"
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_gstr3b.dta"
}
gen month = substr(period,1,2)
gen year = substr(period, 3, .)
destring month, replace 
destring year, replace
gen fy = .
replace fy = 2017 if (year==2017 & month >= 7)
replace fy = 2017 if (year==2018 & month <=3)
replace fy = 2018 if (year==2018 & month >=4)
replace fy = 2018 if (year==2019 & month <=3)
replace fy = 2019 if (year==2019 & month >=4)
cd ..
cd "intermediate_files"
save "gstr_3b_complete.dta", replace

* Creating combined, appended data of collapsed GSTR-1
use "072017_gstr1_collapsed.dta"
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_gstr1_collapsed.dta"
}
gen month = substr(period,1,2)
gen year = substr(period, 3, .)
destring month, replace 
destring year, replace
gen fy = .
replace fy = 2017 if (year==2017 & month >= 7)
replace fy = 2017 if (year==2018 & month <=3)
replace fy = 2018 if (year==2018 & month >=4)
replace fy = 2018 if (year==2019 & month <=3)
replace fy = 2019 if (year==2019 & month >=4)
save "gstr_1_collapsed_complete.dta", replace

* Creating combined, appended data of linked GSTR-1 and GSTR-3B
use 072017_gstr1_gstr3b_collapsed_merged.dta, clear 
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_gstr1_gstr3b_collapsed_merged.dta"
}
gen month = substr(period,1,2)
gen year = substr(period, 3, .)
destring month, replace 
destring year, replace
gen fy = .
replace fy = 2017 if (year==2017 & month >= 7)
replace fy = 2017 if (year==2018 & month <=3)
replace fy = 2018 if (year==2018 & month >=4)
replace fy = 2018 if (year==2019 & month <=3)
replace fy = 2019 if (year==2019 & month >=4)
save "complete_linked_gstr1_gstr3b.dta", replace

* Cleaning up of the linked GSTR-1 and GSTR-3B data
* Note: _merge==3 are matched observations
* _merge==2 is only GSTR-3B. Data of GSTR-1 not present.
* _merge==1 is only GSTR-1. Data of GSTR-3B not present.
use "complete_linked_gstr1_gstr3b.dta", clear
rename taxable_value taxable_value_1
gen liab_1 = igst+cgst+sgst
gen taxable_value_3b =  _1_a_taxable + _1_d_taxable
gen liab_3b = liab_igst + liab_sgst + liab_cgst
gen cash_3b = cash_igst + cash_sgst + cash_cgst
gen etr_1 = 100*(liab_1/taxable_value_1)
gen etr_3b = 100*(liab_3b/taxable_value_3b)
replace etr_1 = int(etr_1)
replace etr_3b = int(etr_3b)
gsort period -cash_3b
order taxable_value_1, after(taxable_value_3b)
save "complete_linked_gstr1_gstr3b_cleaned.dta", replace
by period: gen counter = _n
drop if counter > 100
sort year month -cash_3b
save "complete_linked_gstr_top_100.dta", replace

* Validating tables of totals
use "complete_linked_gstr1_gstr3b_cleaned.dta", clear
replace taxable_value_1 = taxable_value_1/10000000
replace taxable_value_3b = taxable_value_3b/10000000
replace liab_1 = liab_1/10000000
replace liab_3b = liab_3b/10000000
replace cash_3b = cash_3b/10000000
* total taxable_value_1 taxable_value_3b liab_1 liab_3b cash_3b, over(fy) //This will give totals for merged cases as it will look for observations where all the variables are non .//
* total taxable_value_1, over(fy) //This will give complete totals//
* total taxable_value_3b, over(fy)
* total liab_1, over(fy)
* total liab_3b, over(fy)
* total cash_3b, over(fy)


* Cleaning data from linked GSTR-1 and GSTR-3B so that it matches with aggregate collections
use "complete_linked_gstr1_gstr3b_cleaned.dta", clear
order taxable_value_3b, after(taxable_value_1)
order liab_1, after(taxable_value_1)
order liab_3b, after(liab_1)
order cash_3b, after(liab_3b)
replace taxable_value_3b = taxable_value_3b/10000000
replace taxable_value_1 = taxable_value_1/10000000
replace liab_1 = liab_1/10000000
replace liab_3b = liab_3b/10000000
replace cash_3b = cash_3b/10000000
rename taxable_value_3b value_3b_cr
rename taxable_value_1 value_1_cr
rename liab_1 liab_1_cr
rename liab_3b liab_3b_cr
rename cash_3b cash_3b_cr
replace value_1_cr = int(value_1_cr)
replace value_3b_cr = int(value_3b_cr)
replace liab_1_cr = int(liab_1_cr)
replace liab_3b_cr = int(liab_3b_cr)
replace cash_3b_cr = int(cash_3b_cr)
keep if _merge==3 //Step 1- Only keeping data where both GSTR-1 and GSTR-3B is present//
drop if (etr_1 <0|etr_3b <0) //Step 2- Drop if ETR_1 or ETR_3B is negative//
drop if (etr_1 > 30 | etr_3b > 30) //Step 3- Drop if Effective Tax Rate exceeds 300%//


* Generating Cash Credit Ratios from GSTR-3B
* Need to start from stata_raw_files folder and then save in intermediate files folder
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr3b.dta", clear
gen liab = liab_igst + liab_cgst + liab_sgst
gen cash = cash_igst + cash_cgst + cash_sgst
gen cash_ratio = 100*(cash/liab)
replace cash_ratio = int(cash_ratio)
keep ann_gstin_id cash_ratio
cd ..
cd "intermediate_files"
save "`x'_3b_cash_ratio.dta", replace
cd ..
cd "stata_raw_files"
} 

* Merging cash credit ratio of GSTR3B with GSTR1
* Need to start from stata_raw_files folder and then save in intermediate files folder
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1.dta", clear
cd ..
cd "intermediate_files"
merge m:1 ann_gstin_id using "`x'_3b_cash_ratio.dta"
keep if _merge==3 //Keep if GSTR and GSTR-3B match//
gen liab = cgst+igst+sgst
gen etr = 100*(liab/taxable_value)
replace etr = int(etr)
drop if etr > 300 //Drop if etr in GSTR-1 more than 300%//
drop if etr < 0 //Drop if etr is negative//
drop etr
save "`x'_gstr1_with_cash_ratio.dta", replace
cd ..
cd "stata_raw_files"
}

* Collapsing GSTR-1 data into HSN wise data. Getting cash -ratio by weighting with liability
cd ..
cd "intermediate_files"
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_with_cash_ratio.dta", clear
bysort hsn_sc: asgen cash_ratio_hsn = cash_ratio, weights(liab)
collapse (sum) taxable_value liab cess (mean)cash_ratio_hsn, by(hsn_sc)
gen period = "`x'"
replace taxable_value = taxable_value/10000000
replace cess = cess/10000000
replace liab = liab/10000000
replace cash_ratio_hsn = int(cash_ratio_hsn)
rename taxable_value value_cr
rename liab liab_cr
rename cess cess_cr
save "`x'_gstr1_hsn_wise_with_cash_ratio.dta", replace
}

* Combining the HSN-wise data into one data set
use "072017_gstr1_hsn_wise_with_cash_ratio.dta", clear
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_gstr1_hsn_wise_with_cash_ratio.dta"
}
gen month = .
replace month = 1 if period=="072017"
replace month = 2 if period=="082017"
replace month = 3 if period=="092017"
replace month = 4 if period=="102017"
replace month = 5 if period=="112017"
replace month = 6 if period=="122017"
replace month = 7 if period=="012018"
replace month = 8 if period=="022018"
replace month = 9 if period=="032018"
replace month = 10 if period=="042018"
replace month = 11 if period=="052018"
replace month = 12 if period=="062018"
replace month = 13 if period=="072018"
replace month = 14 if period=="082018"
replace month = 15 if period=="092018"
replace month = 16 if period=="102018"
replace month = 17 if period=="112018"
replace month = 18 if period=="122018"
replace month = 19 if period=="012019"
replace month = 20 if period=="022019"
replace month = 21 if period=="032019"
replace month = 22 if period=="042019"
replace month = 23 if period=="052019"
gen fy = .
replace fy = 2017 if (month >=1 & month <=9)
replace fy = 2018 if (month >=10 & month <=21)
replace fy = 2019 if (month >=22)
drop period
save "complete_gstr1_hsn_wise_with_cash_ratio.dta", replace

* Adding labels of months
cd ..
cd "raw_files"
import excel "month_names.xlsx", sheet("Sheet1") firstrow clear
cd ..
cd "intermediate_files"
save "month_names.dta", replace

* Calculations on combined HSN wise data
use "complete_gstr1_hsn_wise_with_cash_ratio.dta", replace
drop if cash_ratio_hsn == .
gen collections_cr = (liab_cr * cash_ratio_hsn)/100
total collections_cr, over(fy)
merge m:1 month using "month_names.dta"
drop _merge
destring hsn_sc, force replace
sort month hsn_sc
save "rates_analysis_master_file.dta", replace
/*
* Checking the distribution of digit wise collections
tostring hsn_sc, force replace //Some loss of information occurs//
total collections, over(fy) //No perceptible loss of information//
gen digit = length(hsn_sc)
table digit fy, c(sum collections)
*/

* Merging the 28% rate files
* Saving rates file
cd ..
cd "raw_files"
import excel "rate changes 28.xlsx", sheet("eventwiserate28") firstrow clear
cd ..
cd "intermediate_files"
destring hsn_sc, replace
reshape long rate, i(hsn_sc) j(event)
save "rate_changes_28.dta", replace

*Merging- 2 step procedure
use "rates_analysis_master_file.dta", clear
replace hsn_sc = int(hsn_sc)
merge m:1 hsn_sc event using "rate_changes_28.dta" //Merge as is. Will match at 6 digits also//
drop _merge
tostring hsn_sc, replace force
gen hsn_4 = substr(hsn_sc,1,4) //Generate hsn at 4 digit//
rename rate rate_merge1
rename hsn_sc hsn_original
rename hsn_4 hsn_sc
destring hsn_sc, replace
merge m:1 hsn_sc event using "rate_changes_28.dta" //Merge at 4 digit//
replace rate_merge1 = rate if rate_merge1==.
replace rate=int(rate)
drop rate
rename rate_merge1 rate
drop if _merge==2
table month rate, c(sum collections_cr)
drop _merge
collapse (sum) value_cr liab_cr cess_cr collections_cr (mean) fy event (max) rate, by(hsn_sc month)
replace rate= int(rate) 
save "rates_analysis_merged_file_28_percent_final.dta", replace

* Tracking commodities
use "rates_analysis_merged_file_28_percent_final.dta", clear
keep if rate != .
collapse (max) rate, by (hsn_sc event)
replace rate = int(rate)
gen event2_comm=.
bysort hsn_sc: replace event2_comm=1 if  rate[2] != rate[1] 
* Event 2 commodities- affected in second rate change
keep if event2_comm==1
keep hsn_sc event2_comm
duplicates drop hsn_sc, force
save "tempfile.dta", replace
use "rates_analysis_merged_file_28_percent_final.dta", clear
merge m:1 hsn_sc using "tempfile.dta"
erase "tempfile.dta"
drop _merge
* Making graphs and tables
keep if event2_comm==1
egen count_event2 = group(hsn_sc)
summarize count_event2
collapse (sum) collections_cr value_cr, by(month)
table month, c( sum collections_cr sum value_cr)
merge m:1 month using "month_names.dta"
labmask month, values(desc)
drop _merge
cd ..
cd "output_files"
* Graph 1
twoway line value_cr month|| scatter value_cr month , ///
 legend(off) xline(5) title("Trends in Taxable value") subtitle("175 Commodities- Rate Reduced from 28% to 18/12% in November 2017" ,size(small)) ///
 ytitle("Taxable Value (in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_175_commodities_taxable_value.png", width(10000)replace
* Graph 2
twoway line collections_cr month  || scatter collections_cr month , ///
 legend(off) xline(5) title("Trends in Tax Collections") subtitle("175 Commodities- Rate Reduced from 28% to 18/12% in November 2017" ,size(small)) ///
 ytitle("Tax Collections(in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_175_commodities_collections.png", width(10000) replace
cd ..
cd "intermediate_files"

* Event 3 commodities - affected in 3rd rate change
use "rates_analysis_merged_file_28_percent_final.dta", clear
keep if rate != .
collapse (max) rate, by (hsn_sc event)
replace rate = int(rate)
gen event3_comm=.
bysort hsn_sc: replace event3_comm=1 if  rate[3] != rate[2] 
keep if event3_comm==1
keep hsn_sc event3_comm
duplicates drop hsn_sc, force
save "tempfile.dta", replace
use "rates_analysis_merged_file_28_percent_final.dta", clear
merge m:1 hsn_sc using "tempfile.dta"
erase "tempfile.dta"
drop _merge
* Making graphs and tables
keep if event3_comm==1
egen count_event3 = group(hsn_sc)
summarize count_event3
collapse (sum) collections_cr value_cr, by(month)
table month, c( sum collections_cr sum value_cr)
merge m:1 month using "month_names.dta"
labmask month, values(desc)
drop _merge
cd ..
cd "output_files"
* Graph 1
twoway line value_cr month|| scatter value_cr month , ///
 legend(off) xline(13) title("Trends in Taxable value") subtitle("16 Commodities- Rate Reduced from 28% to 18/12% in July 2018" ,size(small)) ///
 ytitle("Taxable Value (in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_16_commodities_taxable_value.png", width(10000) replace
* Graph 2
twoway line collections_cr month  || scatter collections_cr month , ///
 legend(off) xline(13) title("Trends in Tax Collections") subtitle("16 Commodities- Rate Reduced from 28% to 18/12% in July 2018" ,size(small)) ///
 ytitle("Tax Collections(in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_16_commodities_collections.png", width(10000) replace
cd ..
cd "intermediate_files"


* Commodities unchanged at 28% from start to finish
use "rates_analysis_merged_file_28_percent_final.dta", clear
keep if rate != .
collapse (max) rate, by (hsn_sc event)
replace rate = int(rate)
gen unchanged = .
bysort hsn_sc: replace unchanged=1 if  (rate[1] == rate[2])&(rate[1] == rate[3]) 
keep if unchanged==1
keep hsn_sc unchanged
duplicates drop hsn_sc, force
save "tempfile.dta", replace
use "rates_analysis_merged_file_28_percent_final.dta", clear
merge m:1 hsn_sc using "tempfile.dta"
erase "tempfile.dta"
drop _merge
* Making graphs and tables
keep if unchanged==1
egen count_unchanged = group(hsn_sc)
summarize count_unchanged
collapse (sum) collections_cr value_cr, by(month)
table month, c( sum collections_cr sum value_cr)
merge m:1 month using "month_names.dta"
labmask month, values(desc)
drop _merge
cd ..
cd "output_files"
* Graph 1
twoway line value_cr month|| scatter value_cr month , ///
 legend(off) xline(5 13) title("Trends in Taxable value") subtitle("31 Commodities- Rate unchanged at 28% between July 2017 till date" ,size(small)) ///
 ytitle("Taxable Value (in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_unchanged_commodities_taxable_value.png", width(10000)replace
* Graph 2
twoway line collections_cr month  || scatter collections_cr month , ///
 legend(off) xline(5 13) title("Trends in Tax Collections") subtitle("31 Commodities- Rate unchanged at 28% between July 2017 till date" ,size(small)) ///
 ytitle("Tax Collections(in Rs. Crore)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(small) format(%10.0fc))
graph export "rate_reduction_unchanged_commodities_collections.png", width(10000) replace
cd ..
cd "intermediate_files"

















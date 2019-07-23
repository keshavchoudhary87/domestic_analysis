set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\intermediate_files"
* Extracting Services data for Rajiv Sir
* Taking GSTR-1, keepin only data with services and saving
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_with_cash_ratio.dta"
gen chapter = substr(hsn_sc,1,2)
keep if chapter=="99"
save "`x'_chap_99_gstr1.dta", replace
}

* Appending all services data into one file
use "072017_chap_99_gstr1.dta", clear
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_chap_99_gstr1.dta"
}
keep if period == "042018" | period=="052018" | period=="062018" | period=="072018" | period=="082018" | period=="092018" | period=="102018" | period=="112018" | period=="122018" ///
| period=="012019" | period=="022019" | period=="032019"
save "complete_chap_99_gstr1.dta", replace
gen liab_cr = (igst+cgst+sgst)/10000000
gen taxable_value_cr = taxable_value/10000000
table chapter, c(sum liab_cr sum taxable_value_cr)
distinct ann_gstin_id

* State wise data for indira madam
use "complete_chapter_state_month_gstr1_collections.dta", clear
collapse (sum) taxable_value_cr liab_cr collections_cr, by (state_cd month)
merge m:1 state_cd using "state_names.dta"
drop if _merge==2 //Andhra Pradesh (Before Division) is not present in data//
replace state_name = "Unknown" if state_cd == 97 //We don't know yet which state this is//
drop _merge
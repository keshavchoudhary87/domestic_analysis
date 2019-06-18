* Read in 2017 data
clear
cd "/Users/kchoudhary/Desktop/import_data"
import excel "IM00790026_Data.xlsx", sheet("Data for FY1718") cellrange(A2:F1201) firstrow
format HSN4digit %04.0f
drop SNo
rename BCDAmountinRs bcd_2017
rename IGSTAmountinRs igst_2017
rename CessAmountinRs cess_2017
rename FinancialYear year_2017
replace year_2017= 2017
save "importdata201718.dta", replace

* Read in 2018 data
clear
import excel "IM00790026_Data.xlsx", sheet("Data for FY1819") cellrange(A2:F1209) firstrow clear
format HSN4digit %04.0f
drop SNo
rename BCDAmountinRs bcd_2018
rename IGSTAmountinRs igst_2018
rename CessAmountinRs cess_2018
rename FinancialYear year_2018
replace year_2018=2018
save "importdata201819.dta", replace

* Merging 2017 with 2018 data
use "importdata201718.dta", clear
merge 1:1 HSN4digit using "importdata201819.dta"
keep if _merge==3
drop _merge
format HSN4digit %04.0f
save "merged_import_data.dta", replace
erase "importdata201819.dta"
erase "importdata201718.dta"

* Converting into 2 digit HSN
drop if HSN4digit < 1000
tostring HSN4digit, replace
gen HSN2digit = substr(HSN4digit, 1,2)
destring HSN4digit, replace
destring HSN2digit, replace

* Picking the import data of the top 20 sectors
gen dummy= 0
foreach i in 99 87 72 85 84 39 25 30 27 73 29 38 40 33 24 {
replace dummy =1 if HSN2digit == `i'
}
keep if dummy==1
drop year_2017 year_2018 HSN4digit dummy
collapse (sum) bcd_2017 bcd_2018 igst_2017 igst_2018 cess_2017 cess_2018, by(HSN2digit)
gen growth_bcd = (bcd_2018/bcd_2017)-1
gen growth_igst = (igst_2018/igst_2017)-1
export excel using "import_summary", replace first(var)









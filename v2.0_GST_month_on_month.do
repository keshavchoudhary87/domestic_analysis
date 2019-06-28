* Change working directory
cd "/Users/kchoudhary/Desktop/gst_data"

* Create a file containing relevant chpater descriptions
set more off
cd "raw_files"
import excel "chap_desc.xlsx", sheet("Sheet1") firstrow clear
cd ..
cd "intermediate_files"
save "chap_desc.dta", replace
cd ..
**********************************************
* Part 1 - Analysis of Import Tax Collections
* Read in 2017 data
clear
cd "raw_files"
import excel "IM00790026_Data.xlsx", sheet("Data for FY1718") cellrange(A2:F1201) firstrow
format HSN4digit %04.0f
drop SNo
rename BCDAmountinRs bcd_2017
rename IGSTAmountinRs igst_2017
rename CessAmountinRs cess_2017
rename FinancialYear year_2017
replace year_2017= 2017
cd ..
cd "intermediate_files"
save "importdata201718.dta", replace

/** Read in 2018 data- yearly data. Inconsistent with 2017 data as 2017 is for 3 quarters and 2018 is for whole year.
clear
import excel "IM00790026_Data.xlsx", sheet("Data for FY1819") cellrange(A2:F1209) firstrow clear
format HSN4digit %04.0f
drop SNo
rename BCDAmountinRs bcd_2018
rename IGSTAmountinRs igst_2018
rename CessAmountinRs cess_2018
rename FinancialYear year_2018
replace year_2018=2018
* Convert from yearly 2018 data to three quarters
replace bcd_2018=bcd_2018*0.75
replace igst_2018 = igst_2018*0.75
replace cess_2018 = cess_2018*0.75
save "importdata201819.dta", replace
*/

* Read in 2018 data - july to march data (consistent with 2017 data)
clear
cd ..
cd "raw_files"
import excel "importdatafor2018-19julytomarch.xlsx", sheet("Data for July18-Mar19") cellrange(A2:F1205) firstrow
drop SNo
rename BCDAmountinRs bcd_2018
rename IGSTAmountinRs igst_2018
rename CessAmountinRs cess_2018
rename FinancialYear year_2018
replace year_2018=2018
cd ..
cd "intermediate_files"
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
save "tempfile.dta"

* Getting Total Imports as an Observation (HSN=0)
collapse (mean) year_2017 year_2018 HSN4digit HSN2digit (sum) bcd_2017 igst_2017 cess_2017 bcd_2018 igst_2018 cess_2018 
replace HSN4digit = .
replace HSN2digit = 0
append using "tempfile.dta"
erase "tempfile.dta"

* Picking the import data of the top 20 sectors
gen dummy= 0
foreach i in 0 24 25 27 28 29 30 32 33 34 38 39 40 48 72 73 76 84 85 87 99 {
replace dummy =1 if HSN2digit == `i'
}
keep if dummy==1
drop year_2017 year_2018 HSN4digit dummy
collapse (sum) bcd_2017 bcd_2018 igst_2017 igst_2018 cess_2017 cess_2018, by(HSN2digit)
gen growth_bcd = (bcd_2018/bcd_2017)-1
gen growth_igst = (igst_2018/igst_2017)-1
export excel using "import_summary", replace first(var)
rename HSN2digit hsn_2

* Converting to Crores
replace bcd_2017 = bcd_2017/10000000
replace bcd_2018 = bcd_2018/10000000
replace igst_2017 = igst_2017/10000000
replace igst_2018 = igst_2018/10000000
replace cess_2017 = cess_2017/10000000
replace cess_2018 = cess_2018/10000000

* Merging in description of HSN
merge 1:m hsn_2 using "chap_desc.dta"
drop if _merge !=3
drop _merge
labmask hsn_2, values(desc)

* Plotting graphs
graph hbar (asis) bcd_2017 bcd_2018 if hsn_2 > 0, over(hsn_2, lab(angle(horizontal) labsize(vsmall))) ///
 blabel(total, size(tiny) format(%7.0fc)) ///
 legend(order(1 "Basic Customs Duty 2017" 2 "Basic Customs Duty 2018") size(small)) ///
 title("HSN wise BCD Collections") ytitle("Tax Collection in Rs. Crore") ///
 ylabel(#5, labsize(vsmall)) bargap(10) subtitle("Q2 to Q4 2018 vs 2017")
cd ..
cd "outputs"
graph export "BCD_trend.png", replace
graph hbar (asis) igst_2017 igst_2018 if hsn_2 > 0, over(hsn_2, lab(angle(horizontal) labsize(vsmall))) ///
blabel(total, angle(vertical) size(tiny) format(%7.0fc)) legend(order(1 "IGST Imports 2017" 2 "IGST Imports 2018")) ///
 title("HSN wise IGST Collections") ytitle("Tax Collection in Rs. Crore") ///
 ylabel(#5, labsize(vsmall)) bargap(10) subtitle("Q2 to Q4 2018 vs 2017")
graph export "IGST_imports.png", replace
keep hsn_2 growth_bcd growth_igst
cd ..
cd "intermediate_files"
save "import_summary.dta", replace

* Month-on-Month import data DGCIS - Reading and Saving
cd ..
cd "raw_files"
import excel "import_summary_dgcis.xlsx", sheet("Selected HSNs") cellrange(A6:X27) firstrow clear
drop SNo Commodity
replace HSCode = 0 if HSCode==.
rename HSCode hsn_2
* Renaming Variables in loop
local counter = 1
foreach x in Jul2017 Aug2017 Sep2017 Oct2017 Nov2017 Dec2017 Jan2018 Feb2018 Mar2018 ///
Apr2018 May2018 Jun2018 Jul2018 Aug2018 Sep2018 Oct2018 Nov2018 ///
Dec2018 Jan2019 Feb2019 Mar2019 {
rename `x' imp_usdm`counter'
local counter =`counter'+1 
}
reshape long imp_usdm, i(hsn_2) j(month)
tostring hsn_2, replace format(%02.0f)
tostring month, replace format(%02.0f)
gen merge_key = hsn_2 + month
drop hsn_2 month
cd .. 
cd "intermediate_files" 
save "imp_dgcis.dta", replace

*********************************************************************
* Part 2 - GST month-on-month analysis
* Reading GST data of top 15 HSN (Abhinav)
cd ..
cd "raw_files"
* import delimited "top15hsn_monthly.csv", encoding(ISO-8859-1)
import excel "top20hsn_monthly.xlsx", sheet("top20hsn_monthly") firstrow clear
cd ..
cd "intermediate_files"
replace hsn_2 = "00" if hsn_2=="Total"
destring hsn_2, replace
sort hsn_2 period
keep hsn_2 period tax_liab taxable_value
egen number = group (period)
drop period
tostring hsn_2, replace format(%02.0f)
tostring number, replace format(%02.0f)
gen merge_key = hsn_2 + number
drop hsn_2 number

* Merging in month-on-month import data from DGCIS
merge 1:1 merge_key using "imp_dgcis.dta"
drop _merge
gen hsn_2 = substr(merge_key, 1,2)
gen number = substr(merge_key, 3,.)
destring hsn_2, replace
destring number, replace
drop merge_key

reshape wide tax_liab taxable_value imp_usdm, i(hsn_2) j(number)
forvalues i = 1/9 {
local y = `i'+12
gen growth_liab`i' = ((tax_liab`y')/(tax_liab`i'))-1
gen growth_value`i' = ((taxable_value`y')/(taxable_value`i'))-1
gen growth_imp`i'= ((imp_usdm`y')/(imp_usdm`i'))-1
}
reshape long growth_liab growth_value growth_imp, i(hsn_2) j(month)
drop if growth_liab==.
gen month_desc = ""
replace month_desc = "jul_2017-2018" if month == 1
replace month_desc = "aug_2017-2018" if month == 2
replace month_desc = "sep_2017-2018" if month == 3
replace month_desc = "oct_2017-2018" if month == 4
replace month_desc = "nov_2017-2018" if month == 5
replace month_desc = "dec_2017-2018" if month == 6
replace month_desc = "jan_2018-2019" if month == 7
replace month_desc = "feb_2018-2019" if month == 8
replace month_desc = "mar_2018-2019" if month == 9
labmask month, values(month_desc)
replace growth_liab = growth_liab*100
format growth_liab %4.2f
replace growth_value = growth_value*100
format growth_value %4.2f
replace growth_imp = growth_imp*100
format growth_imp %4.2f

* Merging descriptions of HSN
merge m:1 hsn_2 using "chap_desc.dta"
drop _merge
labmask hsn_2, values(desc)

* Month on Month Graphs on Loop
cd ..
cd "outputs"
foreach x in 0 24 25 27 28 29 30 32 33 34 38 39 40 48 72 73 76 84 85 87 {
local f0: label hsn_2 `x' 
twoway scatter growth_liab month if hsn_2==`x', mcolor(green) mlabel(growth_liab) mlabsize(vsmall)|| line growth_liab month if hsn_2==`x' , lc(green)|| ///
 scatter growth_value month if hsn_2==`x', mcolor(blue) mlabel(growth_value) mlabsize(vsmall)|| line growth_value month if hsn_2==`x', lc(blue) || ///
 scatter growth_imp month if hsn_2==`x',  mcolor(dkorange) mlabel(growth_imp) mlabsize(vsmall) || line growth_imp month if hsn_2==`x', lc(dkorange) ///
 xlabel(1/9, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Growth Rate in Percent") title("Month-on-Month Growth Rate") subtitle("Chapter - `f0'") ///
 legend(order (1 "Growth in Liability" 3 "Growth in Taxable Value" 5 "Growth in Imports")) 
graph export "`x'_growth.png", replace
}

* Separately for Chapter 99 because there are no imports of Services
local x = 99
local f0: label hsn_2 `x' 
twoway scatter growth_liab month if hsn_2==`x', mcolor(green) mlabel(growth_liab) mlabsize(vsmall)|| line growth_liab month if hsn_2==`x' , lc(green)|| ///
 scatter growth_value month if hsn_2==`x', mcolor(blue) mlabel(growth_value) mlabsize(vsmall)|| line growth_value month if hsn_2==`x', lc(blue) ///
 xlabel(1/9, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Growth Rate in Percent") title("Month-on-Month Growth Rate") subtitle("Chapter - `f0'") ///
 legend(order (1 "Growth in Liability" 3 "Growth in Taxable Value")) 
graph export "`x'_growth.png", replace

********************************************************************
* Part 3 - Plotting graphs of absolute values HSN wise
cd ..
cd "raw_files"
* import delimited "top15hsn_monthly.csv", encoding(ISO-8859-1)
import excel "top20hsn_monthly.xlsx", sheet("top20hsn_monthly") firstrow clear
cd ..
cd "intermediate_files"
replace hsn_2 = "00" if hsn_2=="Total"
destring hsn_2, replace
sort hsn_2 period
keep hsn_2 period tax_liab taxable_value
egen number = group (period)
drop period
tostring hsn_2, replace format(%02.0f)
tostring number, replace format(%02.0f)
gen merge_key = hsn_2 + number
drop hsn_2 number

* Merging in month-on-month import data from DGCIS
merge 1:1 merge_key using "imp_dgcis.dta"
drop _merge
gen hsn_2 = substr(merge_key, 1,2)
gen number = substr(merge_key, 3,.)
destring hsn_2, replace
destring number, replace
drop merge_key

* Converting imports from USD million to Rs. crore
rename number month
gen month_desc = ""
replace month_desc = "jul_2017" if month == 1
replace month_desc = "aug_2017" if month == 2
replace month_desc = "sep_2017" if month == 3
replace month_desc = "oct_2017" if month == 4
replace month_desc = "nov_2017" if month == 5
replace month_desc = "dec_2017" if month == 6
replace month_desc = "jan_2018" if month == 7
replace month_desc = "feb_2018" if month == 8
replace month_desc = "mar_2018" if month == 9
replace month_desc = "apr_2018" if month == 10
replace month_desc = "may_2018" if month == 11
replace month_desc = "jun_2018" if month == 12
replace month_desc = "jul_2018" if month == 13
replace month_desc = "aug_2018" if month == 14
replace month_desc = "sep_2018" if month == 15
replace month_desc = "oct_2018" if month == 16
replace month_desc = "nov_2018" if month == 17
replace month_desc = "dec_2018" if month == 18
replace month_desc = "jan_2019" if month == 19
replace month_desc = "feb_2019" if month == 20
replace month_desc = "mar_2019" if month == 21
labmask month, values(month_desc)

* Merging chapter descriptions
merge m:1 hsn_2 using "chap_desc.dta"
drop _merge
labmask hsn_2, values(desc)

* Plotting graphs
cd ..
cd "outputs"
format tax_liab %12.0fc
format taxable_value %12.0fc
format imp_usdm %12.0fc
* Tax Liability
foreach x in 0 24 25 27 28 29 30 32 33 34 38 39 40 48 72 73 76 84 85 87 99 {
local f0: label hsn_2 `x' 
twoway scatter tax_liab month if hsn_2==`x', ///
mcolor(green) mlabel(tax_liab) mlabsize(vsmall)|| line tax_liab month ///
if hsn_2==`x' , lc(green) xlabel(1/21, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Figures in Rs. Crore") title("Monthly Trends in Tax Liability") subtitle("Chapter - `f0'") ///
 legend(off)
 graph export "`x'_tax_liab.png", replace
 }
* Taxable Value
foreach x in 0 24 25 27 28 29 30 32 33 34 38 39 40 48 72 73 76 84 85 87 99 {
local f0: label hsn_2 `x' 
twoway scatter taxable_value month if hsn_2==`x', ///
mcolor(navy) mlabel(taxable_value) mlabsize(vsmall)|| line taxable_value month ///
if hsn_2==`x' , lc(navy) xlabel(1/21, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Figures in Rs. Crore") title("Monthly Trends in Taxable Value") subtitle("Chapter - `f0'") ///
 legend(off)
 graph export "`x'_taxable_value_.png", replace
 }
* Imports
* No Chapter 99 as there are no imports of services
foreach x in 0 24 25 27 28 29 30 32 33 34 38 39 40 48 72 73 76 84 85 87 {
local f0: label hsn_2 `x' 
twoway scatter imp_usdm month if hsn_2==`x', ///
mcolor(dkorange) mlabel(imp_usdm) mlabsize(vsmall)|| line imp_usdm month ///
if hsn_2==`x' , lc(dkorange) xlabel(1/21, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Figures in USD Million") title("Monthly Trends in Import Values") subtitle("Chapter - `f0'") ///
 legend(off)
 graph export "`x'_imports_.png", replace
 }
 


* Read in 2017 data
set more off
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

/** Read in 2018 data- yearly data
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
import excel "importdatafor2018-19julytomarch.xlsx", sheet("Data for July18-Mar19") cellrange(A2:F1205) firstrow
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
rename HSN2digit hsn_2

* Plotting graphs for Imports
replace bcd_2017 = bcd_2017/10000000
replace bcd_2018 = bcd_2018/10000000
replace igst_2017 = igst_2017/10000000
replace igst_2018 = igst_2018/10000000

* Description of HSNs
gen chapter = ""
replace chapter = "Tobacco" if hsn_2==24
replace chapter = "Cement" if hsn_2==25
replace chapter = "Petroleum Products" if hsn_2==27
replace chapter = "Organic Chemicals" if hsn_2==29
replace chapter = "Pharmaceuticals" if hsn_2==30
replace chapter = "Essential Oils" if hsn_2==33
replace chapter = "Miscellaneous Chemicals" if hsn_2==38
replace chapter = "Plastics" if hsn_2==39
replace chapter = "Rubber" if hsn_2==40
replace chapter = "Iron and Steel" if hsn_2==72
replace chapter = "Articles of Iron and Steel" if hsn_2==73
replace chapter = "Machinery" if hsn_2==84
replace chapter = "Electronics" if hsn_2==85
replace chapter = "Vehicles and Parts" if hsn_2==87
labmask hsn_2, values(chapter)
graph bar (asis) bcd_2017 bcd_2018, over(hsn_2, lab(angle(45) labsize(vsmall))) b2title("HSN at 2-digit") ///
 blabel(total, size(tiny) format(%7.0fc)) ///
 legend(order(1 "Basic Customs Duty 2017" 2 "Basic Customs Duty 2018")) ///
 title("HSN wise BCD Collections 2018 vs 2017") ytitle("Tax Collection in Rs. Crore") ///
 ylabel(#5, labsize(vsmall))
graph export "BCD_trend.png", replace
graph bar (asis) igst_2017 igst_2018, over(hsn_2, lab(angle(45) labsize(vsmall))) b2title("HSN at 2-digit") ///
blabel(total, angle(vertical) size(tiny) format(%7.0fc)) legend(order(1 "IGST Imports 2017" 2 "IGST Imports 2018")) ///
 title("HSN wise IGST Collections 2018 vs 2017") ytitle("Tax Collection in Rs. Crore") ylabel(#5, labsize(vsmall))
graph export "IGST_imports.png", replace
keep hsn_2 growth_bcd growth_igst
save "import_summary.dta", replace

* Reading GST data of top 15 HSN (Abhinav)
clear
import delimited "top15hsn_monthly.csv", encoding(ISO-8859-1)
keep hsn_2 period tax_liab_x taxable_value
* Adding import growths
merge m:1 hsn_2 using "import_summary.dta"
drop _merge
egen number = group (period)
drop period
reshape wide tax_liab_x taxable_value growth_bcd growth_igst, i(hsn_2) j(number)
forvalues i = 1/9 {
local y = `i'+12
gen growth_liab`i' = ((tax_liab_x`y')/(tax_liab_x`i'))-1
gen growth_value`i' = ((taxable_value`y')/(taxable_value`i'))-1
}
reshape long growth_liab growth_value growth_bcd growth_igst, i(hsn_2) j(month)
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

* Description of HSNs
gen chapter = ""
replace chapter = "Tobacco" if hsn_2==24
replace chapter = "Cement" if hsn_2==25
replace chapter = "Petroleum Products" if hsn_2==27
replace chapter = "Organic Chemicals" if hsn_2==29
replace chapter = "Pharmaceuticals" if hsn_2==30
replace chapter = "Essential Oils" if hsn_2==33
replace chapter = "Miscellaneous Chemicals" if hsn_2==38
replace chapter = "Plastics" if hsn_2==39
replace chapter = "Rubber" if hsn_2==40
replace chapter = "Iron and Steel" if hsn_2==72
replace chapter = "Articles of Iron and Steel" if hsn_2==73
replace chapter = "Machinery" if hsn_2==84
replace chapter = "Electronics" if hsn_2==85
replace chapter = "Vehicles and Parts" if hsn_2==87
replace chapter = "Services" if hsn_2 == 99
labmask hsn_2, values(chapter)

foreach x in 87 72 85 84 39 25 30 27 73 29 38 40 33 24 {
local f0: label hsn_2 `x'
local g_bcd = r(mean)*100
su growth_igst if hsn_2==`x'
local g_igst = r(mean)*100  
twoway scatter growth_liab month if hsn_2==`x', mcolor(green) mlabel(growth_liab) mlabsize(vsmall)|| line growth_liab month if hsn_2==`x' , lc(green) || ///
 scatter growth_value month if hsn_2==`x' , mcolor(blue) mlabel(growth_value) mlabsize(vsmall)|| line growth_value month if hsn_2==`x' ,lc(blue) ///
 xlabel(1/9, valuelabel angle(45) labsize(vsmall)) ylabel(,labsize(vsmall)) xtitle("Time Period") ///
 yline(0) ytitle("Growth Rate in Percent") title("Month-on-Month Growth Rate for HSN = `x'") subtitle("`f0'") ///
 legend(order (1 "Growth in Liability" 3 "Growth in Taxable Value"))
graph export "hsn`x'.png", replace
}
* Separately for Ch 99 because there are no imports
local x=99
local f0: label hsn_2 99
twoway scatter growth_liab month if hsn_2==`x' , mcolor(green) mlabel(growth_liab) mlabsize(vsmall)|| line growth_liab month if hsn_2==`x' , lc(green)|| ///
 scatter growth_value month if hsn_2==`x', mcolor(blue) mlabel(growth_value) mlabsize(vsmall)|| line growth_value month if hsn_2==`x', lc(blue) ///
 xlabel(1/9, valuelabel angle(45) labsize(vsmall)) ylabel(, labsize(vsmall)) yline(0) xtitle("Time Period") ///
 ytitle("Growth Rate in Percent") title("Month-on-Month Growth Rate for HSN = `x'") subtitle("`f0'") ///
 legend(order(2 "Growth in Liability" 4 "Growth in Taxable Value"))
graph export "hsn`x'.png", replace






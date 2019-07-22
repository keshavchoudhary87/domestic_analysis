* Set working directory
set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\intermediate_files"

* Import chapter names and save
cd ..
cd "raw_files"
import excel "chapter_names.xlsx", firstrow clear
cd .. 
cd "intermediate_files"
save "chapter_names.dta", replace

* Open file containing HSN wise collections
use "complete_gstr1_hsn_wise_with_cash_ratio.dta", replace
drop if cash_ratio_hsn == .
gen collections_cr = (liab_cr * cash_ratio_hsn)/100
gen digit = strlen(hsn_sc) //Number of digits reported//
table digit fy, c(sum collections_cr) format(%10.1f) //Summary ofcollections digit wise//
sort hsn_sc
gen chapter_raw = substr(hsn_sc,1,2) //Raw chapter as reported in the data set//
table chapter_raw fy, c(sum collections_cr) format(%10.1f)
* Denote error chapter as "00"
gen chapter_cleaned = chapter_raw 
destring chapter_cleaned, replace force
replace chapter_cleaned = 0 if chapter_cleaned == . //Error if chapter=.//
replace chapter_cleaned = 0 if chapter_cleaned == 77 //Error if chapter=77//
table chapter_raw fy, c(sum collections_cr) format(%10.0fc)
table chapter_cleaned fy, c(sum collections_cr) format(%10.0fc)
collapse (sum) value_cr liab_cr cess_cr collections_cr, by(chapter_cleaned month)
gen cash_ratio= 100*(collections_cr/liab_cr) //Effective cash ratio per chapter//
replace cash_ratio = int(cash_ratio)
rename month supply_month
gen fy = .
replace fy = 2017 if (supply_month >=1 & supply_month <=9)
replace fy = 2018 if (supply_month >=10 & supply_month <=21)
replace fy = 2019 if (supply_month >=22)
save "complete_chapter_wise_gstr1_summary_with_collections_month_wise.dta", replace

* Generating monthly cash credit ratio graphs for important chapters
use "complete_chapter_wise_gstr1_summary_with_collections_month_wise.dta", clear
rename supply_month month
table fy, c( sum collections_cr sum value_cr)
merge m:1 month using "month_names.dta"
labmask month, values(desc)
drop _merge desc
merge m:1 chapter_cleaned using "chapter_names.dta"
drop if _merge==2
drop _merge
labmask chapter_cleaned, values(desc)
cd ..
cd "output_files"
foreach x in 25 27 29 30 72 73 84 85 87 99 {
local f0: label chapter_cleaned `x'
twoway scatter cash_ratio month if chapter_cleaned==`x', ///
 legend(off) title("Trends in Cash Ratio") subtitle("Chapter- `x' - `f0'" ,size(small)) ///
 ytitle("Cash Ratio (in percent)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(0(10)100, labsize(small) format(%10.0fc)) mlabel(cash_ratio) mlabangle(45) mlabsize (vsmall)
graph export "`x'_`f0'_monthly_cash_ratios.png", width(10000)replace
}
cd ..
cd "intermediate_files"

* Collapsing to chapter level across 23 months
use "complete_chapter_wise_gstr1_summary_with_collections_month_wise.dta", clear
collapse (sum) value_cr liab_cr cess_cr collections_cr, by(chapter_cleaned)
gen cash_ratio = int(100*(collections_cr/liab_cr))
merge 1:1 chapter_cleaned using "chapter_names.dta"
drop _merge
save "complete_chapter_wise_gstr1_summary_with_collections_aggregate.dta", replace 

* Generating graphs of cash-credit ratio
* Graph of 1 to 50 chapters
use "complete_chapter_wise_gstr1_summary_with_collections_aggregate.dta", clear
labmask chapter_cleaned , values(desc)
graph bar (asis) cash_ratio if (chapter_cleaned>0 & chapter_cleaned <= 50), over(chapter_cleaned, lab(angle(vertical) labsize(tiny))) ///
blabel (bar, angle(horizontal) size(tiny)) title("Chapter Wise Cash Ratios") ytitle("Cash Ratios in Percent") ///
subtitle("Average over July 2017 to May 2019- Chapters 1 to 50") ylabel() 
cd ..
cd "output_files"
graph export "cash_ratios_by_chapter_1_to_50.png", width(10000)replace
cd ..
cd "intermediate_files"

* Graph of 50 to 99 chapters
use "complete_chapter_wise_gstr1_summary_with_collections_aggregate.dta", clear
labmask chapter_cleaned , values(desc)
graph bar (asis) cash_ratio if (chapter_cleaned>50 & chapter_cleaned <= 99), over(chapter_cleaned, lab(angle(vertical) labsize(tiny))) ///
blabel (bar, angle(horizontal) size(tiny)) title("Chapter Wise Cash Ratios") ytitle("Cash Ratios in Percent") ///
subtitle("Average over July 2017 to May 2019- Chapters 51 to 99") ylabel() 
cd ..
cd "output_files"
graph export "cash_ratios_by_chapter_50_to_99.png", width(10000)replace
cd ..
cd "intermediate_files"


* All-India ratios for comparison with Karnataka
use "complete_chapter_wise_gstr1_summary_with_collections_aggregate.dta", clear
labmask chapter_cleaned , values(desc)
graph bar (asis) cash_ratio if (chapter_cleaned==24|  chapter_cleaned==25 | ///
chapter_cleaned==27 | chapter_cleaned==39 | chapter_cleaned==72 | chapter_cleaned==84 ///
| chapter_cleaned==85 | chapter_cleaned==87 | chapter_cleaned==99 ), over(chapter_cleaned, lab(angle(45) labsize(small))) ///
blabel (bar, angle(horizontal) size(tiny)) title("All-India Cash Ratios") ytitle("Cash Ratios in Percent") ///
subtitle("Average over July 2017 to May 2019") ylabel() 
cd ..
cd "output_files"
graph export "cash_ratios_all_india_karnataka.png", width(10000)replace
cd ..
cd "intermediate_files"


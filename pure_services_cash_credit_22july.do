* Change working directory
set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\intermediate_files"

* Picking out pure service suppliers and checking their cash credit ratio
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_with_cash_ratio.dta", clear
gen chap_hsn = substr(hsn_sc,1,2)
collapse (sum) taxable_value liab (mean) cash_ratio, by (ann_gstin_id chap_hsn)
bysort ann_gstin_id: gen counter = _N
keep if counter == 1 & chap_hsn=="99"
asgen cash_ratio_hsn = cash_ratio, weights(liab)
collapse (sum) taxable_value liab (mean) cash_ratio_hsn 
gen rtn_prd = "`x'"
gen chap_hsn="99"
save "`x'_tempfile_services.dta", replace
}

* Appending monthly cash credit ratios for services into a single file
use "072017_tempfile_services.dta", clear
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_tempfile_services.dta"
}
save "pure_services.dta", replace

* Deleting unnecessary tempfiles
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
erase "`x'_tempfile_services.dta"
}

* Plotting graph of cash credit ratio
use "pure_services.dta", clear
gen liab_cr = liab/10000000
drop liab
gen value_cr = taxable_value/10000000
drop taxable_value
gen month = _n
drop rtn_prd
format cash_ratio_hsn %10.0fc
format liab_cr %10.0fc
format value_cr %10.0fc
merge m:1 month using "month_names.dta"
drop event _merge
labmask month, values(desc)
twoway lfit cash_ratio_hsn month || scatter cash_ratio_hsn month , ///
 legend(off) title("Trends in Cash Ratio") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Cash Ratio (in percent)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(0(10)100, labsize(small) format(%10.0fc)) mlabel(cash_ratio) mlabangle(45) mlabsize (vsmall)
cd ..
cd "output_files"
graph export "cash_ratio_pure_services.png", replace width(10000)
cd ..
cd "intermediate_files"

* Plotting graph of Liabilities
twoway lfit liab_cr month || scatter liab_cr month, ///
legend(off) title("Trends in Liabilities") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Liabilities in Rs. Crore") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(0(5000)35000, labsize(tiny) format(%10.0fc)) mlabel(liab_cr) mlabangle(45) mlabsize (tiny)  
cd ..
cd "output_files"
graph export "liabilities_pure_services.png", replace width(10000)
cd ..
cd "intermediate_files"

* Plotting graph of Taxable Value
twoway lfit value_cr month || scatter value_cr month, ///
legend(off) title("Trends in Taxable Value") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Taxable Value in Rs. Crore") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(50000(20000)200000, labsize(tiny) format(%10.0fc)) mlabel(value_cr) mlabangle(45) mlabsize (tiny)  
cd ..
cd "output_files"
graph export "taxable_value_pure_services.png", replace width(10000)
cd ..
cd "intermediate_files"

* Fresh analysis tracking the same taxpayers over 23 months
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
use "`x'_gstr1_with_cash_ratio.dta", clear
gen chap_hsn = substr(hsn_sc,1,2)
collapse (sum) taxable_value liab (mean) cash_ratio, by (ann_gstin_id chap_hsn)
bysort ann_gstin_id: gen counter = _N
keep if counter == 1 & chap_hsn=="99"
collapse (sum) taxable_value liab (mean) cash_ratio, by(ann_gstin_id) 
gen rtn_prd = "`x'"
gen chap_hsn="99"
save "`x'_tempfile_services_part2.dta", replace
}

* Appending monthly files at GSTIN level
use "072017_tempfile_services_part2.dta", clear
foreach x in 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
append using "`x'_tempfile_services_part2.dta"
}
save "pure_services_part2.dta", replace

* Deleting unnecessary files
foreach x in 072017 082017 092017 102017 112017 122017 012018 022018 032018 042018 052018 062018 072018 082018 092018 102018 112018 122018 012019 022019 032019 042019 052019 {
erase "`x'_tempfile_services_part2.dta"
}

* Putting filter of taxpayers who have filed returns throughout the period
use "pure_services_part2.dta", clear
rename rtn_prd time
merge m:1 time using "month_names2.dta"
drop _merge time
sort ann_gstin_id month
bysort ann_gstin_id: gen counter = _N
keep if counter >=6
bysort month: asgen cash_ratio_hsn = cash_ratio, weights(liab)
collapse (sum) taxable_value liab (mean) cash_ratio_hsn, by(month)
gen value_cr = taxable_value/10000000
gen liab_cr = liab/10000000
drop liab taxable_value

* Generating new graphs
format cash_ratio_hsn %10.0fc
format liab_cr %10.0fc
format value_cr %10.0fc
merge m:1 month using "month_names.dta"
drop event _merge
labmask month, values(desc)
twoway lfit cash_ratio_hsn month || scatter cash_ratio_hsn month , ///
 legend(off) title("Trends in Cash Ratio") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Cash Ratio (in percent)") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(0(10)100, labsize(small) format(%10.0fc)) mlabel(cash_ratio) mlabangle(45) mlabsize (vsmall)
cd ..
cd "output_files"
graph export "cash_ratio_pure_services_part2.png", replace width(10000)
cd ..
cd "intermediate_files"

* Plotting graph of Liabilities
twoway lfit liab_cr month || scatter liab_cr month, ///
legend(off) title("Trends in Liabilities") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Liabilities in Rs. Crore") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(0(5000)35000, labsize(tiny) format(%10.0fc)) mlabel(liab_cr) mlabangle(45) mlabsize (tiny)  
cd ..
cd "output_files"
graph export "liabilities_pure_services_part2.png", replace width(10000)
cd ..
cd "intermediate_files"

* Plotting graph of Taxable Value
twoway lfit value_cr month || scatter value_cr month, ///
legend(off) title("Trends in Taxable Value") subtitle("Pure Chapter- 99 -Services" ,size(small)) ///
 ytitle("Taxable Value in Rs. Crore") xtitle("Month of Supply") xlabel(1/23, valuelabel angle(45) ///
 labsize(vsmall)) ylabel(50000(20000)200000, labsize(tiny) format(%10.0fc)) mlabel(value_cr) mlabangle(45) mlabsize (tiny)  
cd ..
cd "output_files"
graph export "taxable_value_pure_services_part2.png", replace width(10000)
cd ..
cd "intermediate_files"



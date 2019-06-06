* Reading in the rates file
cd "E:\GSTN Data\Working"
clear
import delimited "E:\GSTN Data\Working\rates.csv", stringcols(2)
gsort hsn_4 -rate
duplicates drop hsn_4, force
save "rates.dta", replace

* 6 -digit conversion to 4 digit
clear
import delimited "E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_6DIGIT_DATA.csv", stringcols(1) //Import original 6 digit data//
rename v1 hsn
rename v2 period
rename v3 taxable_value
rename v4 cgst
rename v5 sgst
rename v6 igst
rename v7 cess //naming the variables as informed by GSTN//
sort period
destring hsn, generate(hsn_n) 
drop if hsn_n < 1000 //Drop all data upto 3 digits i.e hsn upto 999//
drop if taxable_value == 0 
gen eff_rate = (cgst+sgst+igst)/taxable_value
tostring hsn_n, gen(temp) 
gen hsn_4 = substr(temp,1,4)
replace hsn_4 = substr(hsn,1,4) if (eff_rate <= 0.05 & hsn_n >= 10000) //For hsn having 5 digits eg 12345, if effective rate is less than 5%, then read the hsn as 012345 otherwise keep as 12345//
egen hsn_period = concat(hsn_4 period), p("_")
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_period)
gen hsn_4 = substr(hsn_period, 1,4)
gen period = substr(hsn_period, 6,.)
drop hsn_period
cd "E:\GSTN Data\Working"
save "6_digit_data.dta", replace

* 8 digit conversion to 4 digit
clear
import delimited "E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_8DIGIT_DATA.csv", stringcols(1)
rename v1 hsn
rename v2 period
rename v3 taxable_value
rename v4 cgst
rename v5 sgst
rename v6 igst
rename v7 cess
sort period
destring hsn, generate(hsn_n)
drop if hsn_n < 1000
drop if taxable_value == 0
gen eff_rate = (cgst+sgst+igst)/taxable_value
tostring hsn_n, gen(temp)
gen hsn_4 = substr(temp,1,4)
replace hsn_4 = substr(hsn,1,4) if (eff_rate <= 0.05 & hsn_n >= 1000000)
egen hsn_period = concat(hsn_4 period), p("_")
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_period)
gen hsn_4 = substr(hsn_period, 1,4)
gen period = substr(hsn_period, 6,.)
drop hsn_period
cd "E:\GSTN Data\Working"
save "8_digit_data.dta", replace

* cleaning 4 digit
clear
import delimited "E:\GSTN Data\Raw\HSN_DATA_09052019\HSN_4DIGIT_DATA.csv", stringcols(1)
rename v1 hsn
rename v2 period
rename v3 taxable_value
rename v4 cgst
rename v5 sgst
rename v6 igst
rename v7 cess
sort period
destring hsn, generate(hsn_n)
drop if hsn_n < 100
drop if taxable_value == 0
gen eff_rate = (cgst+sgst+igst)/taxable_value
tostring hsn_n, gen(temp)
rename hsn hsn_4
replace hsn_4 = (temp+"0") if (eff_rate > 0.05 & hsn_n < 1000)
egen hsn_period = concat(hsn_4 period), p("_")
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_period)
gen hsn_4 = substr(hsn_period, 1,4)
gen period = substr(hsn_period, 6,.)
drop hsn_period
cd "E:\GSTN Data\Working"
save "4_digit_data.dta", replace

* Append 6 digit and 8 digit files to 4 digit
use "4_digit_data.dta", replace
append using "6_digit_data.dta"
append using "8_digit_data.dta"
egen hsn_period = concat(hsn_4 period), p("_")
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_period)
gen hsn_4 = substr(hsn_period, 1,4)
gen period = substr(hsn_period, 6,.)
drop hsn_period
cd "E:\GSTN Data\Working"
save "4_digit_final.dta", replace

* Convert to quarterly data
cd "E:\GSTN Data\Working"
use "4_digit_final.dta", clear
gen month = substr(period, 6,.)
gen quarter = ""
replace quarter = "01" if (month == "04" | month == "05" | month == "06")
replace quarter = "02" if (month == "07" | month == "08" | month == "09")
replace quarter = "03" if (month == "10" | month == "11" | month == "12")
replace quarter = "04" if (month == "01" | month == "02" | month == "03")
gen year = substr(period,1,4)
destring year, replace
replace year = year-1 if quarter=="04" 
tostring year, replace
egen hsn_period_q = concat(hsn_4 year quarter), p("-")
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_period_q)
gen hsn_4 = substr(hsn_period_q, 1,4)
gen period_q = substr(hsn_period_q, 6,.)
drop hsn_period_q
gen year = substr(period_q,1,4)
gen q = substr(period_q, 6,.)
save "quarterly_final.dta", replace

/*
* Generating data for 2017-18 and 2018-19 separately
use "quarterly_final.dta", clear
keep if year =="2017"
save "2017_quarterly.dta", replace
use "quarterly_final.dta", clear
keep if year=="2018"
save "2018_quarterly.dta", replace

* Merging 28 percent rates
cd "E:\GSTN Data\Working"
use "quarterly_final.dta", clear
merge m:1 hsn_4 using "rates.dta"
keep if _merge ==3 
save "28percent_quarterly.dta", replace
egen hsn_year = concat(hsn_4 year),p("-") 
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess (mean) rate, by (hsn_year)
gen hsn_4 = substr(hsn_year, 1,4)
gen year = substr(hsn_year, 6,.)
total taxable_value if year=="2018"
total cgst if year=="2018"
total sgst if year=="2018"
total igst if year=="2018"
total cess if year=="2018"
*/

* Projecting to 2019
cd "E:\GSTN Data\Working"
use "quarterly_final.dta", clear
drop if (year == "2018" & q=="01")
egen hsn_year = concat(hsn_4 year),p("-") 
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess, by (hsn_year)
gen hsn_4 = substr(hsn_year, 1,4)
gen year = substr(hsn_year, 6,.)
destring year, replace
sort hsn_4 year
save "annual_data_2017_2018.dta", replace
scalar gf_taxable_value = (3.27/2.82)
gen taxable_value_2019 = (gf_taxable_value)*taxable_value
scalar gf_cgst = 1130348/1052892
scalar gf_sgst = 1136256/1063008
scalar gf_igst = 1757375/1531030
scalar gf_cess = 2.99/2.80
gen igst_2019 = (gf_igst)*igst
gen cgst_2019 = (gf_cgst)*cgst
gen sgst_2019 = (gf_sgst)*sgst
gen cess_2019 = (gf_cess)*cess
destring hsn_4, generate(hsn_numeric)
bysort hsn_numeric: egen count=count(hsn_numeric)
replace taxable_value_2019 = gf_taxable_value*taxable_value_2019 if (count==1 & year==2017)
drop if count==2 & year==2017
drop count
drop taxable_value igst cgst sgst cess
rename taxable_value_2019  taxable_value
rename igst_2019 igst
rename sgst_2019 sgst
rename cess_2019 cess
rename cgst_2019 cgst
replace year=2019
drop hsn_numeric
save "2019_projected.dta", replace

*Merging projected data of 2019 with 2017 and 2018 data
use "annual_data_2017_2018.dta", clear
append using "2019_projected.dta"
sort hsn_4 year
drop hsn_year
save "annual_data_final.dta", replace

* Merging rates into annual data file
use "annual_data_final.dta", clear
merge m:1 hsn_4 using "rates.dta"
replace cess=cess/10000000 //Convert cess into crore//
drop if _merge==2
gen hsn_2=substr(hsn_4, 1,2)
drop _merge
save "annual_data_final.dta", replace
keep if rate==.
save "temporary_merge_file.dta", replace
use "annual_data_final.dta", clear
drop if rate==.
save "annual_data_final.dta", replace
use "temporary_merge_file.dta", clear
drop rate
rename hsn_4 hsn
rename hsn_2 hsn_4
merge m:1 hsn_4 using "rates.dta"
drop if _merge==2
replace rate = 0.09 if hsn_4 == "99" //Chapter 99 is services taxable at 18%//
drop _merge
drop if rate==. //We are dropping all observations with no rates. Total figure is coming up to 4200 crore cash collections for these items. Hence ignoring//
rename hsn_4 hsn_2
rename hsn hsn_4
save "temporary_merge_file.dta", replace
use "annual_data_final.dta", clear
append using "temporary_merge_file.dta"
replace rate=rate*100*2 
save "annual_data_final.dta", replace

* Aggregating data into buckets
* Drop HSN 99 i.e services
* Year= 2019
use "annual_data_final.dta", clear
drop if hsn_2=="99"
drop hsn_2
gen revenue = (cgst+igst+sgst)*0.2
gen cess_revenue = cess*0.2
gen revenue_incl_cess = revenue+cess_revenue
keep if year==2019
gen revenue_new = revenue
local hsn_change 2523 4011 4012 8407 8408 8415 8422 8483 8507 8511 8525 8528 8701 8702 8704 8706 8707 8708 8711 8714 //List of 28% HSN which has to be changed//
foreach i in `hsn_change' {
replace revenue_new = revenue_new*(9/14) if hsn_4=="`i'"
}
gen revenue_new_incl_cess = revenue_new + cess_revenue
gen revenue_change = revenue_new_incl_cess - revenue_incl_cess
total revenue_change
tabstat cgst sgst igst cess taxable_value revenue revenue_incl_cess revenue_change, by(rate) stat(sum)

* Converting all values to 12 months from 3 quarters
gen taxable_value_year=taxable_value*(4/3)
gen cgst_year = cgst*(4/3)
gen sgst_year = sgst*(4/3)
gen igst_year = igst*(4/3)
gen cess_year = cess*(4/3)
gen cess_revenue_year = cess_revenue*(4/3)
gen revenue_year = revenue*(4/3)
gen revenue_year_incl_cess = revenue_year + cess_revenue_year
gen revenue_per_quarter = revenue_year/4
gen revenue_new_year = revenue_new + revenue_per_quarter
gen revenue_new_year_incl_cess = revenue_new_year + cess_revenue_year
gen revenue_change_year = revenue_new_year - revenue_year
gen revenue_change_year_incl_cess = revenue_new_year_incl_cess - revenue_year_incl_cess
tabstat cgst_year sgst_year igst_year cess_year taxable_value_year revenue_year revenue_new_year revenue_change_year, by(rate) stat(sum) 
tabstat cgst_year sgst_year igst_year cess_year taxable_value_year revenue_year_incl_cess revenue_new_year_incl_cess revenue_change_year_incl_cess, by(rate) stat(sum) 

* Aggregating Data for 2017 and 2018
* Year = 2018
use "annual_data_final.dta", clear
drop if hsn_2=="99"
drop hsn_2
gen revenue = (cgst+igst+sgst)*0.2
gen cess_revenue = cess*0.2
gen revenue_incl_cess = revenue+cess_revenue
keep if year==2018
tabstat cgst sgst igst cess taxable_value revenue revenue_incl_cess, by(rate) stat(sum)

* Year = 2017
use "annual_data_final.dta", clear
drop if hsn_2=="99"
drop hsn_2
gen revenue = (cgst+igst+sgst)*0.2
gen cess_revenue = cess*0.2
gen revenue_incl_cess = revenue+cess_revenue
keep if year==2017
tabstat cgst sgst igst cess taxable_value revenue revenue_incl_cess, by(rate) stat(sum)


* Plotting month-wise trends HSN wise
clear
import excel "E:\GSTN Data\Working\hsn code for changes.xlsx", sheet("Sheet1") firstrow
drop if HSN_SC==.
rename HSN_SC hsn_4
tostring hsn_4, replace
gen trend=1
save "hsn_for_trends.dta", replace 
use "4_digit_final.dta", clear
merge m:1 hsn_4 using "hsn_for_trends.dta"
gen cut = "YES" 
replace cut = "NO" if trend == .
egen variable = concat(period cut), p ("-")
drop _merge
drop trend
gen revenue = (cgst+igst+sgst)*0.2
collapse (sum) taxable_value (sum) cgst (sum) sgst (sum) igst (sum) cess (sum) revenue, by (variable)
gen time = substr(variable, 1,7)
gen cut = substr(variable, 9, .)
gen month = .
replace month = 1 if time == "2017-07"
replace month = 2 if time == "2017-08"
replace month = 3 if time == "2017-09"
replace month = 4 if time == "2017-10"
replace month = 5 if time == "2017-11"
replace month = 6 if time == "2017-12"
replace month = 7 if time == "2018-01"
replace month = 8 if time == "2018-02"
replace month = 9 if time == "2018-03"
replace month = 10 if time == "2018-04"
replace month = 11 if time == "2018-04"
replace month = 12 if time == "2018-05"
replace month = 13 if time == "2018-06"
replace month = 14 if time == "2018-07"
replace month = 16 if time == "2018-08"
replace month = 17 if time == "2018-09"
replace month = 18 if time == "2018-10"
replace month = 19 if time == "2018-11"
replace month = 20 if time == "2018-12"
replace month = 21 if time == "2019-01"
replace month = 22 if time == "2019-02"
replace month = 23 if time == "2019-03"
gen dummy = .
replace dummy = 1 if cut=="YES"
replace dummy = 0 if cut=="NO"
twoway (line revenue month if dummy==1, xtitle(Time Period) ytitle(Revenue in Rs. crore) title(Trend in Revenue Collections)) 

/*
bysort hsn_4: replace growfactor=(taxable_value[2]/taxable_value[1])-1
replace growfactor=. if year==2017
gen taxable_value_2019 = (1+growfactor)*taxable_value
gen igst_2019 = (1+growfactor)*igst
gen cgst_2019 = (1+growfactor)*cgst
gen sgst_2019 = (1+growfactor)*sgst
gen cess_2019 = (1+growfactor)*cess
destring hsn_4, generate(hsn_numeric)
bysort hsn_numeric: egen count=count(hsn_numeric)
*/


* Change working directory
set more off
cd "E:\GSTN Data\Raw\latest-02-07-2019\intermediate_files" //Start from the intermediate files folder//


/*
* Reading in and saving comprehensive rates file 
cd ..
cd "raw_files"
import excel "rate_file_master_all_rates.xlsx", sheet("Rates (2)") cellrange(A3:AA1390) firstrow clear
cd ..
cd "intermediate_files"
drop Z AA

* Cleaning up comprehensive rates file
forvalues x = 1/23 {
replace m`x'="0" if hsn_sc=="39264011" //Replace Nil by 0//
replace m`x'="0" if hsn_sc=="050790" //Replace Nil by 0//
replace m`x'="0" if hsn_sc=="19041020" //Replace Nil by 0//
replace m`x'="3" if hsn_sc=="7101" //Correct error in this entry showing 403 percent//
}
forvalues x=1/2 {
replace m`x'="0" if hsn_sc=="24868550" //Replace - by 0//
}
forvalues x = 1/23 { //Destring rate variables//
destring m`x', replace
}
rename month desc //checking rates distribution to check that there are no outliers
forvalues x=1/23{
tab m`x'
}
replace hsn_sc = subinstr(hsn_sc, " ", "",.) //Removing spaces between HSN's//
strip hsn_sc, of(" ") gen(hsn)
drop hsn_sc
rename hsn hsn_sc
replace hsn_sc="87081010" if hsn_sc=="24868550"  //This entry does not exist//
drop if hsn_sc=="-"
rename desc ch_desc
gen hsn_4 = substr(hsn_sc,1,4)
order hsn_4 hsn_sc
sort hsn_4 hsn_sc 
save "rates_file_all_rates.dta", replace  
export excel using "rates.xlsx", replace // This is the rates file given to Shekhar Sir for assigning weights//
*/


* Reading in weighted rates file prepared by Shekhar sir and saving
cd ..
cd "raw_files"
import excel "rates_assigning_weights_new.xlsx", sheet("Sheet4") firstrow clear
cd ..
cd "intermediate_files"
save "rates_assigning_weights_new.dta", replace

* Basic cleaning/checks on weighted rates file
* Generating temporary file containing split number of each HSN
use "rates_assigning_weights_new.dta", clear
replace weights = weights*100
bysort hsn_4: egen checksum = total(weights) //Sum of all weights should be 100//
tab checksum //There should be no other value except for 100 in the table//
gen dummy = 1
gen digit = strlen(hsn_4) //HSN_4 is at either 2 digit or 4 digit//
bysort hsn_4: egen splits = total(dummy) //Count number of repetitions for each HSN//
keep hsn_4 splits digit
duplicates drop hsn_4, force //Keep unique HSN_4 and their count for merge//
preserve
keep if digit == 2
drop digit
rename hsn_4 hsn_2
save "split_tempfile_2_digit.dta", replace
restore
preserve
keep if digit == 4
drop digit
save "split_tempfile_4_digit.dta", replace 
restore

* Opening HSN wise GSTR-1 data file containing collections
use "complete_gstr1_hsn_wise_with_cash_ratio.dta", clear
drop if cash_ratio_hsn == .
gen collections_cr = (liab_cr * cash_ratio_hsn)/100 //We won't be using actul collections for this analysis//
table fy, c(sum collections_cr)
merge m:1 month using "month_names.dta"
drop _merge
drop event

* Cleaning up of HSN wise GSTR-1 data file containing collections 
gen chapter_raw = substr(hsn_sc,1,2) //Pick leftmost 2 digits from HSN//
gen chapter_new = chapter_raw
destring chapter_new, gen(chapter_no) force //Will drop all alphabets etc//
replace chapter_new = "00" if chapter_no==. //Put all such alphabet etc missing values into error chapter//
replace chapter_new = "00" if chapter_new=="0" //Putting single digit HSN into Chapters 1 to 9//
replace chapter_new = "01" if chapter_new=="1"
replace chapter_new = "02" if chapter_new=="2"
replace chapter_new = "03" if chapter_new=="3"
replace chapter_new = "04" if chapter_new=="4"
replace chapter_new = "05" if chapter_new=="5"
replace chapter_new = "06" if chapter_new=="6"
replace chapter_new = "07" if chapter_new=="7"
replace chapter_new = "08" if chapter_new=="8"
replace chapter_new = "09" if chapter_new=="9"
replace chapter_new = "00" if chapter_new=="77" //77= unused chapter//
drop chapter_no
table chapter_new, c(sum collections_cr)
replace cash_ratio_hsn = int(cash_ratio_hsn)
drop if collections_cr < 0 //Drop if negative collections shown//
table fy, c(sum collections_cr)
save "rates_analysis_master_file_all_rates.dta", replace

* Starting rates analysis- splitting GSTR-1 data set
* First create monthly files 
forvalues x = 1/23 { 
use "rates_analysis_master_file_all_rates.dta", clear
keep if month == `x'
save "rates_analysis_master_file_all_rates_m`x'.dta", replace
}

* Splitting at 2 digit level for each month
forvalues x = 1/23 { 
use "rates_analysis_master_file_all_rates_m`x'.dta", clear
rename chapter_new hsn_2 
merge m:1 hsn_2 using "split_tempfile_2_digit.dta" //Obtain count of repeititons for each hsn_2//
keep if _merge==3
drop _merge
drop cess_cr collections_cr chapter_raw desc //we will compute our own collections//
bysort hsn_2: asgen cash_ratio_new = cash_ratio_hsn, weights(liab_cr) //Calculate Cash credit ratio for HSN at 2 digit in each month//
collapse (sum) value_cr (mean) cash_ratio_new month fy splits, by(hsn_2)
expand splits //Weights will automatically take care of value_cr repeating//
bysort hsn_2: gen split_no = _n
rename cash_ratio_new cash_ratio_hsn
rename hsn_2 hsn
save "rates_analysis_master_file_all_rates_2_digit_collapsed_m`x'.dta", replace
}

* Splitting at 4-digit level
forvalues x = 1/23 { 
use "rates_analysis_master_file_all_rates_m`x'.dta", clear
drop desc chapter_raw chapter_new
gen hsn_4 = substr(hsn_sc,1,4)
merge m:1 hsn_4 using "split_tempfile_4_digit.dta"
keep if _merge==3
drop _merge
drop cess_cr collections_cr 
bysort hsn_4: asgen cash_ratio_new = cash_ratio_hsn, weights(liab_cr)
collapse (sum) value_cr (mean) cash_ratio_new month fy splits, by(hsn_4)
expand splits 
bysort hsn_4: gen split_no = _n
rename cash_ratio_new cash_ratio_hsn
rename hsn_4 hsn
save "rates_analysis_master_file_all_rates_4_digit_collapsed_m`x'.dta", replace
}

* Appending all the split data of 2 digit and 4 digit level of different months
use "rates_analysis_master_file_all_rates_2_digit_collapsed_m1.dta"
append using "rates_analysis_master_file_all_rates_4_digit_collapsed_m1.dta"
forvalues x=2/23{
append using "rates_analysis_master_file_all_rates_2_digit_collapsed_m`x'.dta"
append using "rates_analysis_master_file_all_rates_4_digit_collapsed_m`x'.dta"
}
save "rates_analysis_master_file_all_rates_split_complete.dta", replace

* Merging in weights and rates 
use "rates_assigning_weights_new.dta", clear
bysort hsn_4: egen checksum = total(weights) //Sum of all weights should be 1//
tab checksum //verified//
drop hsn_sc checksum
rename hsn_4 hsn
bysort hsn: gen split_no = _n
egen key = concat (hsn split_no), punct("_")
order key
sort key
reshape long m, i(key) j(month)
rename m rate
drop key
save "rates_assigning_weights_tempfile.dta", replace
* Merging 
use "rates_analysis_master_file_all_rates_split_complete.dta", clear
drop splits
merge m:1 hsn split_no month using "rates_assigning_weights_tempfile.dta"
drop _merge
sort hsn month split_no
replace value_cr = value_cr * weights
gen liab_cr = (value_cr * rate)/100
gen collections_cr = (liab_cr * cash_ratio_hsn)/100
gen event = .
replace event = 1 if (month >=1 & month <=4)
replace event =2 if (month >=6 & month<=13)
replace event =3 if (month>=14 & month <=18)
replace event = 4 if (month >=19 & month <=21)
drop if event==. //Drop months that do not fall in any event//

* Generating tables
* Overall aggregates
table fy, c(sum value_cr sum liab_cr sum collections_cr)

* Aggregate- Collections
table fy rate, c(sum collections_cr)
table event rate, c(sum collections_cr)
table month rate, c(sum collections_cr)

* Aggregate Taxable Value
table fy rate, c(sum value_cr)
table event rate, c(sum value_cr)
table month rate, c(sum value_cr)

* Aggregate Liability
table fy rate, c(sum liab_cr)
table event rate, c(sum liab_cr)
table month rate, c(sum liab_cr)

* Delete unnecessary files
erase "split_tempfile_2_digit.dta"
erase "split_tempfile_4_digit.dta"
forvalues x = 1/23 { 
erase "rates_analysis_master_file_all_rates_m`x'.dta"
erase "rates_analysis_master_file_all_rates_2_digit_collapsed_m`x'.dta"
erase "rates_analysis_master_file_all_rates_4_digit_collapsed_m`x'.dta"
}

* Plot graphs
gen event_desc = ""
replace event_desc = "Jul-17 to Oct-17" if event==1
replace event_desc = "Dec-17 to Jul-18" if event==2
replace event_desc = "Aug-18 to Dec-18 " if event==3
replace event_desc = "Jan-19 to Mar-19" if event==4
labmask event, values(event_desc)
collapse (sum) value_cr liab_cr collections_cr, by (rate event) 

bysort event: egen collections_event = sum(collections_cr)
gen percent_collections_event = 100*(collections_cr/collections_event)
bysort event: egen value_event = sum(value_cr)
gen percent_value_event = 100*(value_cr/value_event)
bysort event: egen liab_event = sum(liab_cr)
gen percent_liab_event = 100*(liab_cr/liab_event)

cd ..
cd "output_files"
* Collections 
graph bar (asis) percent_collections_event,  over(event, lab(angle(vertical) labsize(tiny))) over(rate)  blabel(bar, format(%10.1fc) size(tiny)) ytitle("Share in percent") title("Event-Wise Composition of Goods in Rate Buckets", size(medium)) subtitle("Collections")
graph export "rate_bucket_collections.png", width(10000) replace 
* Taxable Value
graph bar (asis) percent_value_event,  over(event, lab(angle(vertical) labsize(tiny))) over(rate)  blabel(bar, format(%10.1fc) size(tiny)) ytitle("Share in percent") title("Event-Wise Composition of Goods in Rate Buckets", size(medium)) subtitle("Taxable Value")
graph export "rate_bucket_value.png", width(10000) replace 
* Liability
graph bar (asis) percent_liab_event,  over(event, lab(angle(vertical) labsize(tiny))) over(rate)  blabel(bar, format(%10.1fc) size(tiny)) ytitle("Share in percent") title("Event-Wise Composition of Goods in Rate Buckets", size(medium)) subtitle("Tax Liability")
graph export "rate_bucket_liability.png", width(10000) replace 




/*Credit Card Transaction Data*/
adhoc5.EOSS_cc_txn

proc print data=targpord.portfolio_data_updated(obs=10);
run;

proc sql;
create table EOSS_cc_txn1 as
select t1.*,t2.custid
from 
(select * from adhoc5.EOSS_cc_txn
group by tserno
	having count(*)=1 and upcase(cr_dr)="DEBIT") t1 left join
(select card_number,custid from targpord.portfolio_data_updated where month=201607) t2
on t1.card_number=t2.card_number;
quit;

/*Debit Card Transaction Data*/
adhoc5.EOSS_dc_txn
 
/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;
/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;
/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

%macro cust(data,card_num,cust_id,flag);

	proc sql;
	update &data. set &cust_id.="" where &cust_id. like "&flag._UNKWN_%" ;
	quit;

	proc sort data = &data.;
	by descending &card_num.;
	run;

	data &data.;	
	length cust_id_1 $100.;
	set &data.;
	r=lag(&card_num.);
	retain i 0;
		if &cust_id.='' then do;
			if &card_num.=r then cust_id_1=compress("&flag._UNKWN_"||i);
			else do ;
			i=i+1;
			cust_id_1=compress("&flag._UNKWN_"||i);
			end;
		end;
	run;

	data  &data. (drop= cust_id_1 r i) ;
	length cust_id_1 $100.;
	set &data.;
	if &cust_id.="" then &cust_id.=cust_id_1;
	run;

%mend;

%cust(work.EOSS_cc_txn1,card_number,custid,CC);
%cust(adhoc5.EOSS_dc_txn,card_num,custid,DC);

data adhoc5.shop_EOSS;
	set EOSS_cc_txn1(in=a keep=merch_name CARD_NUMBER TRXN_DT bill_amt is_online custid rename=(CARD_NUMBER=CARD_NUM bill_amt=final_amt))
		adhoc5.EOSS_dc_txn(rename=(tran_date=TRXN_DT merch_name1=merch_name) in= b keep=merch_name1 CARD_NUM tran_date is_online final_amt custid);

	if a then
		product="CC";

	if b then
		product="DC";
run;

proc sql;
create table shop_roll_up as
select TRXN_DT,product,merch_name,count(*) as num_txn,sum(final_amt) as txn_amt,count(distinct custid) as no_cust
from adhoc5.shop_EOSS
group by 1,2,3;
quit;

proc sql;
create table eras as
select *,case 
				   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
				   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST"
				   when TRXN_DT between "16AUG2016"D and "16AUG2016"d+60 then "POST_TEST"
					end as flag
	from adhoc5.shop_EOSS
	having flag ne "";
	quit;

	proc sql;
	select flag,TRXN_DT,count(*) from eras group by 1,2 order by 2;
	quit;

proc sql;
create table EOSS as
select * from 
(
	select 
		flag,
		merch_name,
		product,
		count(*) as num_txn,
		sum(final_amt) as txn_amt,
		count(distinct custid) as no_cust,
		calculated txn_amt/calculated num_txn as txn_amt_per_txn,
		calculated txn_amt/calculated no_cust as txn_amt_per_cust,
		calculated num_txn/calculated no_cust as num_txn_per_cust
	from
	(select *,case 
				   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
				   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST"
				   when TRXN_DT between "16AUG2016"D and "16AUG2016"d+60 then "POST_TEST"
					end as flag
	from adhoc5.shop_EOSS
	having flag ne "")
	group by 1,2,3
union
	select 
		flag,
		merch_name,
		"OVERALL" as product,
		count(*) as num_txn,
		sum(final_amt) as txn_amt,
		count(distinct custid) as no_cust,
		calculated txn_amt/calculated num_txn as txn_amt_per_txn,
		calculated txn_amt/calculated no_cust as txn_amt_per_cust,
		calculated num_txn/calculated no_cust as num_txn_per_cust	from
	(select *,case 
				   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
				   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST"
				   when TRXN_DT between "16AUG2016"D and "16AUG2016"d+60 then "POST_TEST"
					end as flag
	from adhoc5.shop_EOSS
	having flag ne "")
	group by 1,2
)
order by 3,2;
quit;

/*title "CC";*/
/*proc tabulate data= eoss(where=(product="CC"));*/
/*var num_txn txn_amt txn_amt;*/
/*class flag merch_name product;*/
/*table merch_name,flag*(num_txn txn_amt txn_amt)*sum ;*/
/*run;*/
/**/
/*title "DC";*/
/*proc tabulate data= eoss(where=(product="DC"));*/
/*var num_txn;*/
/*class flag merch_name product;*/
/*table merch_name,flag*(num_txn txn_amt txn_amt)*sum ;*/
/*run;*/

proc transpose data=EOSS out=eoss_1;
by product merch_name;
id flag;
run;

proc format;
value $ fmt
"no_cust"="Number of Customers"
"num_txn"="Number of Transactions"
"txn_amt"="Transaction Amount"

"txn_amt_per_txn"="Transaction Amount Per Transaction" 
"txn_amt_per_cust"="Transaction Amount Per Customer"
"num_txn_per_cust"="Number of Transactions Per Customer";
run;

data eoss_MID;
length _name_ $70.;
set eoss_1;
pre_during_lift=(test-pre_test)/pre_test;
post_during_lift=(post_test-test)/test;
pre_post_lift=(post_test-pre_test)/pre_test;
array t _numeric_;
do over t;
if t=. or t<=0 then t=0;
end;
format pre_during_lift post_during_lift pre_post_lift percent12.2;
run;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*GROWTH CALCULATION*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

data Other_than_EOSS_CC(where=(merch_name = " ") drop= I022_POS_ENTRY);
length merch_name $40.;
set targpord.crd_trxn_ods_data(where=("14JUN2016"d-60<=trxn_dt<="15AUG2016"d+60)
keep=card_number bill_amt trxn_dt merchant_name mcc_category I022_POS_ENTRY tserno cr_dr);

if index(upcase(compress(merchant_name,,'d')),'PANTALOON')>0 then merch_name='PANTALOON';
ELSE IF index(upcase(compress(merchant_name,,'d')),'ALDO')>0  then merch_name='ALDO';
ELSE IF index(upcase(compress(merchant_name,,'d')),'BEVERLY')>0 
or index(upcase(compress(merchant_name,,'d')),'BHPC') then merch_name='BHPC';
ELSE IF index(upcase(compress(merchant_name)),'BEBE')>0 then merch_name='BEBE';
ELSE IF index(upcase(compress(merchant_name)),'CALLIT')>0 then merch_name='CALLIT';
ELSE IF index(upcase(compress(merchant_name,,'d')),'CENTRAL')>0 then merch_name='CENTRAL';
ELSE IF index(upcase(compress(merchant_name,,'d')),'CHARLESNKEITH')>0 OR index(upcase(compress(merchant_name,,'d')),'CHARLES&KEITH')>0 OR
index(upcase(compress(merchant_name,,'d')),'CHARLENKEITH')>0 OR index(upcase(compress(merchant_name,,'d')),'CHARLE&KEITH')>0
OR index(upcase(compress(merchant_name,,'d')),'CHARLESNKIETH')>0 OR index(upcase(compress(merchant_name,,'d')),'CHARLES&KIETH')>0 OR
index(upcase(compress(merchant_name,,'d')),'CHARLENKIETH')>0 OR index(upcase(compress(merchant_name,,'d')),'CHARLE&KIETH')>0
 then merch_name='CHARLESNKIETH';
/*ELSE IF index(upcase(compress(merchant_name,,'d')),'EZONE')>0 and */
/*index(upcase(compress(merchant_name,,'d')),'TIMEZONE') = 0 and*/
/*index(upcase(compress(merchant_name,,'d')),'EYEZONE') = 0 and*/
/*index(upcase(compress(merchant_name,,'d')),'MOBILEZONE') = 0 and*/
/*index(upcase(compress(merchant_name,,'d')),'SAFEZONE') = 0*/
/*then merch_name='EZONE';*/
ELSE IF index(upcase(compress(merchant_name,,'d')),'GUESS')>0 then merch_name='GUESS';
ELSE IF index(upcase(compress(merchant_name,,'d')),'HYPERCITY')>0 then merch_name='HYPERCITY';
ELSE IF index(upcase(compress(merchant_name,,'d')),'INGLOT')>0 then merch_name='INGLOT';
ELSE IF index(upcase(compress(merchant_name,,'d')),'LANDMARK')>0 then merch_name='LANDMARK';
/*ELSE IF index(upcase(compress(merchant_name,,'d')),'JASHN')>0 then merch_name='JASHN';*/
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'MARKSNSPENCER')>0 OR index(upcase(compress(merchant_name,,'d')),'MARKSANDSPENCER')>0
OR index(upcase(compress(merchant_name,,'d')),'MARKSPEN')>0 OR index(upcase(compress(merchant_name,,'d')),'MARKS&SPEN')>0
 then merch_name='MARKSNSPENCER';
ELSE IF
index(upcase(compress(merchant_name,,'d')),'NINEWEST')>0 then merch_name='NINEWEST'; 
/*ELSE IF*/
/*index(upcase(compress(merchant_name,,'d')),'MILANO')>0 then merch_name='MILANO'; */
/*ELSE IF*/
/*index(upcase(compress(merchant_name,,'d')),'RUSSO')>0 or index(upcase(compress(merchant_name,,'d')),'BRUNELLO')>0 then merch_name='RUSSO BRUNELLO'; */
ELSE IF
index(upcase(compress(merchant_name,,'d')),'WESTSIDE')>0 then merch_name='WESTSIDE'; 
/*ELSE IF */
/*index(upcase(compress(merchant_name,,'d')),'ZIVAME')>0 then merch_name='ZIVAME';*/
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'PARK AVENUE')>0 then merch_name='PARK AVENUE';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'NEW BALANCE')>0 then merch_name='NEW BALANCE';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'LA SENZA')>0 then merch_name='LA SENZA';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'PARX')>0 then merch_name='PARX';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'RAYMONDS')>0 then merch_name='RAYMONDS';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'COLOR PLUS')>0 then merch_name='COLOR PLUS';
ELSE IF 
index(upcase(compress(merchant_name,,'d')),'AND')*index(upcase(compress(merchant_name,,'d')),'GLOBAL DESI')>0 then merch_name='GLOBAL DESI/AND';

if upcase(compress(merchant_name,,'d'))="MAX" or upcase(compress(merchant_name,,'d'))="MAX STORE" then merch_name="MAX";
if mcc_category='MOTO' or I022_POS_ENTRY in (1,0) then is_online=1; else is_online=0;
RUN;

proc sql;
create table other_than_eoss_cc as
select t1.*,t2.cust_id1 as custid
from 
(select * from other_than_eoss_cc
group by tserno
	having count(*)=1 and upcase(cr_dr)="DEBIT") t1 left join
(select distinct card_no1,cust_id1 from adhoc5._new) t2
on t1.card_number=t2.card_no1;
quit;

%size(other_than_eoss_cc)

proc sql;
create table other_than_eoss_cc as
	select * from other_than_eoss_cc
		where mcc_category in ("CLOTH STORES","DEPT STORES");
quit;

proc print data= adhoc5.mcc_category_mapping (obs=10);
run;

proc sql;
select distinct mcc_category from adhoc5.mcc_category_mapping;
quit;

proc sql;
select mcc_code into: code_list separated by ","
from adhoc5.mcc_category_mapping 
where upcase(mcc_category) in ("CLOTHSTORES","DEPTSTORES");
quit;

data other_than_eoss_dc(where=(merch_name1 = "") drop=ecom_indicator);
length merch_name1 $30.;
set dcdm.ptlf_data
(keep=card_num final_Amt tran_date merch_name custid ecom_indicator mcc_Code where=("14JUN2016"d-60<=tran_date<="15AUG2016"d+60))
;
if index(upcase(compress(merch_name,,'d')),'PANTALOON')>0 then merch_name1='PANTALOON';
ELSE IF index(upcase(compress(merch_name,,'d')),'ALDO')>0  then merch_name1='ALDO';
ELSE IF index(upcase(compress(merch_name,,'d')),'BEVERLY')>0 
or index(upcase(compress(merch_name,,'d')),'BHPC') then merch_name1='BHPC';
ELSE IF index(upcase(compress(merch_name)),'BEBE')>0 then merch_name1='BEBE';
ELSE IF index(upcase(compress(merch_name)),'CALLIT')>0 then merch_name1='CALLIT';
ELSE IF index(upcase(compress(merch_name,,'d')),'CENTRAL')>0 then merch_name1='CENTRAL';
ELSE IF index(upcase(compress(merch_name,,'d')),'CHARLESNKEITH')>0 OR index(upcase(compress(merch_name,,'d')),'CHARLES&KEITH')>0 OR
index(upcase(compress(merch_name,,'d')),'CHARLENKEITH')>0 OR index(upcase(compress(merch_name,,'d')),'CHARLE&KEITH')>0
OR index(upcase(compress(merch_name,,'d')),'CHARLESNKIETH')>0 OR index(upcase(compress(merch_name,,'d')),'CHARLES&KIETH')>0 OR
index(upcase(compress(merch_name,,'d')),'CHARLENKIETH')>0 OR index(upcase(compress(merch_name,,'d')),'CHARLE&KIETH')>0
 then merch_name1='CHARLESNKIETH';
ELSE IF index(upcase(compress(merch_name,,'d')),'EZONE')>0 and 
index(upcase(compress(merch_name,,'d')),'TIMEZONE') = 0 and
index(upcase(compress(merch_name,,'d')),'EYEZONE') = 0 and
index(upcase(compress(merch_name,,'d')),'MOBILEZONE') = 0 and
index(upcase(compress(merch_name,,'d')),'SAFEZONE') = 0
then merch_name1='EZONE';
ELSE IF index(upcase(compress(merch_name,,'d')),'GUESS')>0 then merch_name1='GUESS';
ELSE IF index(upcase(compress(merch_name,,'d')),'HYPERCITY')>0 then merch_name1='HYPERCITY';
ELSE IF index(upcase(compress(merch_name,,'d')),'INGLOT')>0 then merch_name1='INGLOT';
ELSE IF index(upcase(compress(merch_name,,'d')),'LANDMARK')>0 then merch_name1='LANDMARK';
ELSE IF index(upcase(compress(merch_name,,'d')),'JASHN')>0 then merch_name1='JASHN';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'MARKSNSPENCER')>0 OR index(upcase(compress(merch_name,,'d')),'MARKSANDSPENCER')>0
OR index(upcase(compress(merch_name,,'d')),'MARKSPEN')>0 OR index(upcase(compress(merch_name,,'d')),'MARKS&SPEN')>0
 then merch_name1='MARKSNSPENCER';
ELSE IF
index(upcase(compress(merch_name,,'d')),'NINEWEST')>0 then merch_name1='NINEWEST'; 
ELSE IF
index(upcase(compress(merch_name,,'d')),'MILANO')>0 then merch_name1='MILANO'; 
ELSE IF
index(upcase(compress(merch_name,,'d')),'RUSSO')>0 or index(upcase(compress(merch_name,,'d')),'BRUNELLO')>0 then merch_name1='RUSSO BRUNELLO'; 
ELSE IF
index(upcase(compress(merch_name,,'d')),'WESTSIDE')>0 then merch_name1='WESTSIDE'; 
ELSE IF 
index(upcase(compress(merch_name,,'d')),'ZIVAME')>0 then merch_name1='ZIVAME';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'PARK AVENUE')>0 then merch_name1='PARK AVENUE';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'NEW BALANCE')>0 then merch_name1='NEW BALANCE';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'LA SENZA')>0 then merch_name1='LA SENZA';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'PARX')>0 then merch_name1='PARX';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'RAYMONDS')>0 then merch_name1='RAYMONDS';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'COLOR PLUS')>0 then merch_name1='COLOR PLUS';
ELSE IF 
index(upcase(compress(merch_name,,'d')),'AND')*index(upcase(compress(merch_name,,'d')),'GLOBAL DESI')>0 then merch_name1='GLOBAL DESI/AND';

if upcase(compress(merch_name,,'d'))="MAX" or upcase(compress(merch_name,,'d'))="MAX STORE" then merch_name1="MAX";
if ecom_indicator in ("15","59") then is_online=1; else is_online=0;
run;

%size(other_than_eoss_dc)
proc sql;
create table other_than_eoss_dc as
	select * from other_than_eoss_dc
		where input(mcc_code,4.) in (&code_list.);
quit;

%cust(work.Other_than_EOSS_CC,card_number,custid,CC);
%cust(work.Other_than_EOSS_DC,card_num,custid,DC);

proc sql;
create table growth_append as
	select bill_amt,custid,"CC" as product,
					case
				   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
				   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST" end as flag
	from Other_than_EOSS_CC where  "13JUN2015"D-60 <= TRXN_DT <= "15AUG2016"d
	union all
	select final_amt,custid,"DC" as product,
					case
				   when tran_date between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
				   when tran_date between "14JUN2016"D and "15AUG2016"d then "TEST" end as flag 
	from Other_than_EOSS_DC where  "13JUN2015"D-60 <= tran_date <= "15AUG2016"d;
quit;

proc sql;
create table growth_roll_up as
select * from 
(
	select flag,product,count(*) as num_txn,sum(bill_amt) as txn_amt,count(distinct custid) as no_cust,
		calculated txn_amt/calculated num_txn as txn_amt_per_txn,
		calculated txn_amt/calculated no_cust as txn_amt_per_cust,
		calculated num_txn/calculated no_cust as num_txn_per_cust
	from growth_append
	group by 1,2
union
	select flag,"OVERALL" as product,count(*) as num_txn,sum(bill_amt) as txn_amt,count(distinct custid) as no_cust,
	
		calculated txn_amt/calculated num_txn as txn_amt_per_txn,
		calculated txn_amt/calculated no_cust as txn_amt_per_cust,
		calculated num_txn/calculated no_cust as num_txn_per_cust
	from growth_append
	group by 1
)
order by product
;
quit;

proc transpose data=growth_roll_up out=growth_roll_up_1;
by product;
id flag;
run;

data growth_roll_up_mid;
set growth_roll_up_1;
lift=(TEST-PRE_TEST)/PRE_TEST;
array t _numeric_;
do over t;
	if t=. or t<=0 then t=0;
end;
format lift percent12.2;
run;

proc sql;
create table growth_map(drop=_name_ rename=(_name_1=_name_)) as
select t1.*,t2.lift as pre_during_growth,
ifn(pre_during_lift-pre_during_growth<0,0,pre_during_lift-pre_during_growth) as adjusted_pre_during_lift format=percent12.2,
put(t1._name_,$fmt.) as _name_1
from eoss_mid t1 left join growth_roll_up_mid t2
on t1.product=t2.product and t1._name_=t2._name_;
quit;

ods tagsets.ExcelXP file="/SAS/BIU/ADHOC/OUTPUT/EOSS.xls" style=journal
		options(sheet_interval='None');

ods tagsets.excelxp options(sheet_interval='none' sheet_name="CC");


title "CC";
proc tabulate data= growth_map(where=(product="CC"));
var _numeric_;
class _name_ merch_name;
table merch_name="",_name_=""*((Pre_Test Test Post_Test)*max="" (Pre_During_Lift Pre_During_Growth Adjusted_Pre_During_Lift Post_During_Lift Pre_Post_lift)*max=""*f=percent12.2)/misstext="0" ;
run;

ods tagsets.excelxp options(sheet_interval='none' sheet_name="DC");

title "DC";
proc tabulate data= growth_map(where=(product="DC"));
var _numeric_;
class _name_ merch_name;
table merch_name="",_name_=""*((Pre_Test Test Post_Test)*max="" (Pre_During_Lift Pre_During_Growth Adjusted_Pre_During_Lift Post_During_Lift Pre_Post_lift)*max=""*f=percent12.2)/misstext="0" ;
run;

ods tagsets.excelxp options(sheet_interval='none' sheet_name="OVERALL");

title "OVERALL";
proc tabulate data= growth_map(where=(product="OVERALL"));
var _numeric_;
class _name_ merch_name;
table merch_name="",_name_=""*((Pre_Test Test Post_Test)*max="" (Pre_During_Lift Pre_During_Growth Adjusted_Pre_During_Lift Post_During_Lift Pre_Post_lift)*max=""*f=percent12.2)/misstext="0" ;
run;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*OVERALL*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
select count(*) as num_txn,sum(final_amt)/10**7 as txn_amt,count(distinct custid) as no_cust 
from adhoc5.shop_EOSS
where TRXN_DT between "14JUN2016"D and "15AUG2016"d;
quit;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*INACTIVES*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
create table dc_vintage as
select t1.card_num,t1.custid,t2.card_num as key,t2.tran_date,t2.merch_name,t3.mcc_category 
from 
(select distinct card_num,custid from adhoc5.eoss_dc_txn
where tran_date between "14JUN2016"D and "15AUG2016"d) t1 
left join dcdm.ptlf_data(where=(intnx('month',"14JUN2016"d,-6)<=tran_date<"14JUN2016"d)) t2
on t1.card_num=t2.card_num
left join adhoc5.mcc_category_mapping t3
on t2.mcc_code=t3.mcc_code;
quit;

proc sql;
create table cc_vintage as
select t1.card_number,t1.custid,t2.card_number as key,t2.trxn_dt as tran_date,t2.merchant_name as merch_name,
t2.mcc_category
from 
(Select distinct card_number,custid from eoss_cc_txn1
where trxn_dt between "14JUN2016"D and "15AUG2016"d) t1 
left join targpord.crd_trxn_ods_data(where=(intnx('month',"14JUN2016"d,-6)<=trxn_dt<="14JUN2016"d)) t2
on t1.card_number=t2.card_number;
quit;

/*%size(cc_vintage);*/
/*%size(dc_vintage);*/

data adhoc5.dc_eoss_vintage;
set dc_vintage;
run;

data adhoc5.cc_eoss_vintage;
set  cc_vintage;
run;

proc sql;
select count(distinct custid) as dc_inactives from adhoc5.dc_eoss_vintage where key="";/*17044*/
select count(distinct custid) as cc_inactives from adhoc5.cc_eoss_vintage where key="";/*5343*/
select count(distinct custid) as total_inactives from 
(select custid from adhoc5.dc_eoss_vintage where key="" 
union
select custid from adhoc5.cc_eoss_vintage where key="");
quit;

proc sql;
create table total_inactives as
(select custid from adhoc5.dc_eoss_vintage where key="" 
union
select custid from adhoc5.cc_eoss_vintage where key="");
quit;
/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*PERSISTORS*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
create table flagged_base as
select *,case 
				   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "During 0"
				   when "16AUG2016"d<=TRXN_DT <= "15SEP2016"D  then "Post 1 Month"
				   when "16SEP2016"d<=TRXN_DT <= "15OCT2016"D  then "Post 2 Month"
				   when "16OCT2016"d<=TRXN_DT <= "15NOV2016"D  then "Post 3 Month"
				   when "16NOV2016"d<=TRXN_DT <= "15DEC2016"D  then "Post 4 Month"
		 end as flag
from adhoc5.shop_eoss
having flag ne "";
quit;

proc sql;
create table flag_roll_up as
select flag,count(*) as num_txn,sum(final_amt) as txn_amt,count(distinct custid) as no_cust
from flagged_base
group by 1;
quit;

proc sql;
create table activated_on as
select t1.*,t2.merch_name,t2.is_online,t2.trxn_dt 
from total_inactives t1 left join flagged_base t2
on t1.custid=t2.custid
order by custid,trxn_dt desc;
quit;

proc sort data=activated_on out=adhoc5.activated_on_no_dup nodupkey;
by custid;
run;

proc sql;
create table start_date_map as
select 
	t1.*,t2.start_date,-t2.start_date+t1.trxn_dt as vintage,case when calculated vintage<180 then "NTB" else "ETB" end as flag,
	t3.min_Acc_Open_Date,
	t4.min_Acc_Open_Date as min_Acc_Open_Date_ca
from adhoc5.activated_on_no_dup t1 left join adhoc5.start_date_DATA T2
on t1.custid=t2.cust_id
left join maindata.i_grid_110_c t3
on t1.custid=t3.cust_id
left join maindata.i_grid_ca_110_c t4
on t1.custid=t4.cust_id
where t1.custid ne "";
quit;

data start_date_map1;
set start_date_map;
format final_date date9.;
final_Date=coalesce(start_date,min_Acc_Open_Date,min_Acc_Open_Date_ca);
vintage_new=-final_date+trxn_dt;
flag_new=ifc(vintage_new<180,"NTB","ETB");
flag_new=ifc(vintage_new=.,"",flag_new);
run;

proc sql;
select *,count/sum(count) as percent format=percent. from(
select merch_name,count(*) as count
from adhoc5.activated_on_no_dup
group by 1)
order by 2 desc;
select count(*) from adhoc5.activated_on_no_dup;
quit;

proc sql;
create table persistor_eoss as
select t1.*,case when t2.custid ne "" then 1 else 0 end as was_inactive
from (select 
	custid,flag,count(*) as no_of_txns
from flagged_base
group by 1,2) t1 left join total_inactives t2
on t1.custid=t2.custid;
quit;

proc transpose data= persistor_eoss out=persistor_eoss_TT;
var no_of_txns;
by  custid was_inactive;
id flag;
run;

proc sql;
   select 
            (count(t1.'During 0'n)) as count_of_during, 
            (count(t1.'Post 1 Month'n)) as 'count_of_post 1 month'n, 
            (count(t1.'Post 2 Month'n)) as 'count_of_post 2 month'n 
      from work.persistor_eoss_tt t1
      where t1.'During 0'n not = . and t1.was_inactive = 1;
quit;

data cc_post_campaign_base;
set targpord.crd_trxn_ods_data(where=("15AUG2016"d<=trxn_dt) keep=card_number trxn_dt);
run;

proc sql;
create table cc_post_campaign_base1 as
select t1.*,t2.cust_id1 as custid
from cc_post_campaign_base t1 left join (select distinct card_no1,cust_id1 from adhoc5._new) t2
on t1.card_number=t2.card_no1;
quit;

data dc_post_campaign_base;
set dcdm.ptlf_data(where=("15AUG2016"d<=tran_date) keep=custid tran_date);
run;

proc sql;
create table total_post_campaign as
select custid,trxn_dt from cc_post_campaign_base1
union 
select custid,tran_date from dc_post_campaign_base
;
quit;

proc sql;
create table total_portfolio_post as
select t1.*,t2.custid as key,t2.trxn_dt
from total_inactives t1 left join total_post_campaign t2
on t1.custid=t2.custid;
quit;

proc sql;
select put(trxn_dt,monyy7.) as month,count(distinct key)
from total_portfolio_post
group by 1;
quit;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*Basic metrics*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
create table roll_up1 as
select 
	flag,
	count(distinct cust_id) as count_total,
	count(distinct case when active=1 then cust_id end) as count_active,
	round(mean(case when active=1 then sum(total_amt,0) end),0.01) as mean_txn_amount_active,
	round(sum(case when active=1 then total_amt end)/10**7,0.01) as total_txn_amount_active,
	count(distinct case when active=1 then cust_id end)/count(distinct cust_id) as active_rate
from (select custid as cust_id,1 as active,final_amt as total_amt,
		      case when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST"
				   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "CONTROL" end as flag
				   from adhoc5.shop_EOSS where product="CC"
	having flag ne "" )
group by 1;
quit;

%macro x3();

data roll_up2;
set roll_up1;
active_lift=active_rate-lag(active_rate);
/*conversion_lift=conversion_rate-lag(conversion_rate);*/

active_incremental=active_lift*count_total*mean_txn_amount_active+
	ifn(mean_txn_amount_active-lag(mean_txn_amount_active)>0,(mean_txn_amount_active-lag(mean_txn_amount_active))*(count_active-(active_lift*count_total)),0);

/*convert_incremental=conversion_lift*count_total*mean_txn_amount_convert*/
/*	ifn(mean_txn_amount_convert-lag(mean_txn_amount_convert)>0,(mean_txn_amount_convert-lag(mean_txn_amount_convert))*(count_convert-(conversion_lift*count_total)),0);*/

/*if mean_txn_amount_active>lag(mean_txn_amount_active) then do;*/
/*active_incremental=(total_txn_amount_active-(count_total*lag(active_rate)*lag(mean_txn_amount_active)))/10**7; end;*/
/*else do;*/
/*active_incremental=(active_lift*count_total*mean_txn_amount_active)/10**7; end;*/
/**/
/*if mean_txn_amount_convert>lag(mean_txn_amount_convert) then do;*/
/*convert_incremental=(total_txn_amount_convert-(count_total*lag(conversion_rate)*lag(mean_txn_amount_convert)))/10**7; end;*/
/*else do;*/
/*convert_incremental=(conversion_lift*count_total*mean_txn_amount_convert)/10**7; end;*/
run;























































proc transpose data=roll_up2 out=roll_up3;
id flag;
run;

proc format;
value $ myfmt
'count_total_active'='# Customers Targeted'
'count_total_convert'='# Customers Targeted'
'count_total'='# Customers Targeted'
'count_active'='# Active'
'count_convert'='# Winners'
'mean_txn_amount_active'='Avg. Spends'
'total_txn_amount_active'='Total Spends (Cr.)'
'mean_txn_amount_convert'='Avg. Spends'
'total_txn_amount_convert'='Total Spends (Cr.)'
'active_rate'='% Active'
'conversion_rate'='% Winners'
'active_lift'='Lift'
'conversion_lift'='Lift'
'active_incremental'="Incremental"
'convert_incremental'="Incremental";
run;

data Active Converts Total;
set roll_up3;
if index(_name_,"conver") ne 0 then output Converts; 
	else if index(_name_,"active") ne 0 then output Active;
			else output total;
run;

ods tagsets.ExcelXP file="/SAS/BIU/ADHOC/OUTPUT/Metrics.xls" style=journal
		options(absolute_column_width="20, 8, 8, 8, 8"  sheet_name='Metrics' suppress_bylines='yes' sheet_interval='None' skip_space='0,0,0,0,0' embedded_titles="yes");
title;
proc report data= Total 
style(header)=[color=black background=#BDD7EE fontfamily=calibri fontsize=11pt borderwidth=.2pt font_weight=bold]
style(report)=[borderwidth=.2pt fontfamily=calibri] style(column)=[borderwidth=.2pt fontfamily=calibri];
col _name_ ("Overall" (TEST CONTROL));
define _name_/"Metrics" style={font_weight=bold} format=$myfmt. center;
define TEST/"Test" style={font_weight=bold} center;
define CONTROL/"Control" style={font_weight=bold} center;
compute _name_;
	call define(_col_,'style','style=[background=#D7D7D7]');
endcomp;
run;
title bcolor=black color=white bold font=Calibri "Active" ;
proc report data= Active noheader
style(header)=[color=black background=#BDD7EE fontfamily=calibri fontsize=11pt borderwidth=.2pt font_weight=bold]
style(report)=[borderwidth=.2pt fontfamily=calibri] style(column)=[borderwidth=.2pt fontfamily=calibri];
col _name_ ("Overall" (TEST CONTROL));
define _name_/"Metrics" style={font_weight=bold} format=$myfmt. center;
define TEST/"Test" style={font_weight=bold} center;
define CONTROL/"Control" style={font_weight=bold} center;
compute _name_;
	call define(_col_,'style','style=[background=#D7D7D7]');
endcomp;
compute test;
	if _name_ in ("conversion_lift","active_lift","active_rate","conversion_rate") then do;
		call define(_col_,'format','percent8.2'); end;
endcomp;
compute control;
	if _name_ in ("conversion_lift","active_lift","active_rate","conversion_rate") then do;
	call define(_col_,'format','percent8.2');end;
endcomp;
run;
title bcolor=black color=white bold font=Calibri "Winner" ;
proc report data= Converts noheader
style(header)=[color=black background=#BDD7EE fontfamily=calibri fontsize=11pt borderwidth=.2pt font_weight=bold]
style(report)=[borderwidth=.2pt fontfamily=calibri] style(column)=[borderwidth=.2pt fontfamily=calibri];
col ("WINNERS"(_name_ ("Overall" (TEST CONTROL))));
define _name_/"Metrics" style={font_weight=bold} format=$myfmt. center;
define TEST/"Test" style={font_weight=bold} center;
define CONTROL/"Control" style={font_weight=bold} center;
compute _name_;
	call define(_col_,'style','style=[background=#D7D7D7]');
endcomp;
compute test;
	if _name_ in ("conversion_lift","active_lift","active_rate","conversion_rate") then do;
		call define(_col_,'format','percent8.2'); end;
endcomp;
compute control;
	if _name_ in ("conversion_lift","active_lift","active_rate","conversion_rate") then do;
	call define(_col_,'format','percent8.2');end;
endcomp;
run;
%mend;

%X3();
proc print data=DCDM.DC_AUG16_4(obs=10);
run;

PROC SQL;
   CREATE TABLE DC_base AS 
   SELECT t1.cust_id as CUSTID,t1.card_no
      FROM DCDM.DC_AUG16_4 t1
      WHERE is_live=1;
quit;

%cust(DC_base,card_no,custid,DC)

proc sql;
create table CC_base as
select custid,card_number
from targpord.portfolio_data_updated
where cif_flag=1 and month=201608;
QUIT;
%cust(CC_base,card_number,custid,CC)

proc sql;
select count(distinct custid) as CC_count from CC_base;
select count(distinct custid) as DC_count from DC_base;
select count(distinct custid) as Total_count from (select custid from CC_base union select custid from DC_base);
quit;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*CUSTOMER ANGLE*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;


proc sql;
create table eras as
select *,case 
			   when TRXN_DT between "13JUN2016"D-60 and "13JUN2016"d then "PRE_TEST"
			   when TRXN_DT between "14JUN2016"D and "15AUG2016"d then "TEST"
			   when TRXN_DT between "16AUG2016"D and "16AUG2016"d+60 then "POST_TEST"
				end as flag
from adhoc5.shop_EOSS
having flag ne "";
quit;

proc sql;
create table activated_base as
select * from eras where custid in (select custid from eras where flag="TEST");
quit;

proc sql;
create table cust_level_actives as
select 
	custid,
	flag,
	count(*) as no_txns,
	sum(final_amt) as spends,
	count(distinct merch_name) as merch_variety,
	count(case when merch_name in ('CHARLESNKIETH','NEW BALANCE','MARKSNSPENCER','BEBE','LA SENZA','RUSSO BRUNELLO','RAYMONDS','EZONE','COLOR PLUS',
									'GUESS','PARK AVENUE','ALDO','MILANO','JASHN') then custid end) as txns_high_end,
	count(case when merch_name not in ('CHARLESNKIETH','NEW BALANCE','MARKSNSPENCER','BEBE','LA SENZA','RUSSO BRUNELLO','RAYMONDS','EZONE','COLOR PLUS',
									'GUESS','PARK AVENUE','ALDO','MILANO','JASHN') then custid end) as txns_low_end
from activated_base
where custid not like "%UNKWN%" and flag ne "POST_TEST"
group by 1,2;
quit;

proc sql;
create table adhoc5.cust_grid_map as
select t1.*,t2.product_c,t2.segment_c,t2.region_cat_name_c,t2.centre_name as city,t2.age_112
from cust_level_actives t1 left join maindata.i_grid_112 t2
on t1.custid=t2.cust_id;
quit;

data cust_grid_map;
set adhoc5.cust_grid_map;
if age>105 or age<19 then age=.;
run;

proc univariate data=cust_grid_map;
var age;
hist age;
run;

data cust_grid_map1;
length age_buckets spends_buckets $15.;
set cust_grid_map;

if age<25 then age_buckets="1.<25";
else if 25<=age<30 then age_buckets="2.25-30";
else if 30<=age<35 then age_buckets="2.30-35";
else if 35<=age<50 then age_buckets="3.35-50";
else if age>50 then age_buckets="4.>50";

if spends<=500 then spends_buckets="1.<=500";
else if 500<spends<=1000 then spends_buckets="2.500-1000";
else if 1000<spends<=3000 then spends_buckets="3.1000-3000";
else if 3000<spends<=8000 then spends_buckets="4.3000-8000";
else if spends>8000 then spends_buckets="5.>8000";
run;

 
%macro x2(var);
proc sql;
create table age_roll_up as
select *,no_cust/sum(no_cust) as count_percent from
(
select flag,&var.,count(*) as no_cust,mean(spends) as mean_spends
from cust_grid_map1
where &var. ne ""
group by 1,2
)
group by flag
order by &var.,flag;
quit;

data age_roll_up1;
set age_roll_up;
by &var.;
cust_lift=count_percent-lag(count_percent);
spends_lift=(mean_spends-lag(mean_spends))/lag(mean_spends);
if first.&var.=1 then do;cust_lift=.;spends_lift=.;end;
run;

proc tabulate data=age_roll_up1;
var no_cust mean_spends cust_lift spends_lift;
class &var. flag;
table &var.="",flag*((no_cust mean_spends)*max="") (cust_lift spends_lift)*max=""*f=percent12.2;
run;
%mend;

%x2(segment_c);
%x2(product_c);
%x2(region);
%x2(age_buckets);
%x2(spends_buckets);



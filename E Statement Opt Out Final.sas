
proc sql;
create table adhoc5.estatement_opt_out_new as
select 
	channel LENGTH=10,
	contactstatusid,
	datepart(contactdatetime) as contact_date format=date9.,
	accountno,
	put(accountno,z16.) as account_no 
from _axuresp.vw_contact_account 
where campaigncode='C000001321' and "01FEB2016"D<=datepart(contactdatetime);
quit;

proc sql;
select contact_date,contactstatusid,count(*)
from adhoc5.estatement_opt_out_new
group by 1,2
order by 1;
quit;

%doitnow(adhoc5,estatement_opt_out_new,account_no)

proc print data= adhoc5.estatement_opt_out_new (obs=10);
run;

proc datasets lib=work kill;
run;

%let start=21JUL2016;
%let end=20AUG2016;
data _null_;
call symputx('_1_before',put(intnx('month',"&start."d,-1,'s'),date9.));
call symputx('_1_after',put(intnx('month',"&end."d,1,'s'),date9.));
call symputx('_2_after',put(intnx('month',"&end."d,2,'s'),date9.));
call symputx('_3_after',put(intnx('month',"&end."d,3,'s'),date9.));
call symputx('_4_after',put(intnx('month',"&end."d,4,'s'),date9.));
call symputx('_5_after',put(intnx('month',"&end."d,5,'s'),date9.));
run;

data _null_;
call symputx('before_beginning',put(intnx('month',"&_1_before."d,0,'b'),date9.));
call symputx('after_end',put(intnx('month',"&_1_after."d,0,'e'),date9.));
run;

%put &start &end &_1_after &_2_after. &_1_before &before_beginning. &after_end; 

data estatement_opt_out_new;
set adhoc5.estatement_opt_out_new;
if channel="eMessage" and contactstatusid ne 4 then contactstatusid=2;
run;

proc sql;
select channel,contactstatusid,count(*)
from estatement_opt_out_new
group by 1,2;
quit;

proc sql;
create table spends_map as 
select 
	t1.*,
	t2.bill_amt,
	t2.tserno,
	t2.trxn_dt,
	put(t2.trxn_dt,monyy7.) as month,
	case when contact_date<=trxn_dt<=contact_date+29 then 1 else 0 end as active
from (select distinct 
			put(accountno,z16.) as card_no_unmasked,
			contact_date,
			account_no as card_no,
			case when contactstatusid=2 then "TEST" else "CONTROL" end as flag
	from estatement_opt_out_new
	where contactstatusid in (2,4) and 	"&start."d <= contact_date <= "&end."d) t1 
left join targpord.crd_trxn_ods_data(where=("&before_beginning."d<=trxn_dt<="&after_end."d)) t2
on t1.card_no=t2.card_number;
quit;

proc sql;
   create table spends_map1 as 
   select t1.card_no_unmasked, 
          t1.card_no, 
          t1.bill_amt, 
          t1.tserno, 
          t1.trxn_dt, 
          t1.month, 
          max(t1.active) as active,
          max(t1.flag) as flag
      from work.spends_map t1
      group by 1,2,3,4,5,6;
quit;

proc sql;
create table cust_map as
select t1.*,t2.custid as cust_id
from spends_map1 t1 left join targpord.portfolio_data_updated(where=(month=201607)) t2
on t1.card_no=t2.card_number;
quit;

proc sql;
create table cust_map1 as
select t1.*,t2.cust_id1 as cust_id,t2.acc_no
from spends_map1 t1 left join adhoc5._new t2
on t1.card_no=t2.card_no1;
quit;


proc sql;
select count(*) as count,count(cust_id) as cust_count,nmiss(cust_id) as nmiss
from cust_map1;
quit;

data cust_map;
set cust_map1;
run;


proc sql;
create table ttest_Data as
select t1.cust_id,t1.flag,t2.bill_amt
from (select distinct cust_id,card_no,flag from cust_map) t1 
left join targpord.crd_trxn_ods_data(where=("&_1_before."d<=trxn_dt<"&start."d )) t2
on t1.card_no=t2.card_number;
quit;

proc sql;
create table ttest_Data1 as
select cust_id,flag,sum(bill_amt) as bill_amt
from ttest_Data
group by 1,2;
quit;

proc ttest data=ttest_Data1;
var bill_amt;
class flag;
quit;

%ttest_v3(ttest_Data1,bill_amt)

/*data x;*/
/*y="101";flag="TEST";output;*/
/*y="101";flag="CONTROL";output;*/
/*run;*/
/*proc sql;*/
/*select y,max(flag) from x group by 1;*/
/*select y,min(flag) from x group by 1;*/
/*run;*/

proc sql;
create table like_to_like_data as
(select distinct cust_id,tserno,bill_amt,month,flag,trxn_dt,active from cust_map where cust_id in 
		(select cust_id from ttest_Data1_mean));
quit;

proc sql;
create table pre_during_post_spends as
select 
	case 
		 when "&_1_before."d<=trxn_dt<"&start."d then "A.PRE"
		 when "&start."d<=trxn_dt<="&end."d then "B.DURING" 
		 when "&end."D<trxn_dt<="&_1_after."d then "C.POST" end as period,
	flag,
	mean(bill_amt) as mean_txn_amt
from 
like_to_like_data
group by 1,2
having period ne " ";
quit;

proc sql;
create table monthly_spends as
select 
	cust_id,
	month,
	max(flag) as flag,
	sum(bill_amt) as txn_amt
from 
like_to_like_data
group by 1,2;
quit;

proc sql;
create table month_summary as
select month,flag,mean(txn_amt)	as mean_txn_amt
from monthly_spends
where compress(month) ne "."
group by 1,2
order by 1;
quit;

proc transpose data= month_summary out=month_summary_tt(drop=_name_);
id flag;
by month;
run;

proc sql;
select * from month_summary_tt
order by input(month,monyy7.);
quit;

proc sql;
create table activity_data as
select cust_id,max(flag) as flag,max(active) as active_flag
from like_to_like_data
group by 1;
quit;
 	
proc sql;
select flag,sum(active_flag) as active, count(*) as count,
	calculated active/calculated count as activity_rate
from activity_data
group by 1;
quit;

/*proc sql;*/
/*create table temp as*/
/*select distinct cust_id,tserno,bill_amt,month,flag from cust_map;*/
/*quit;*/
/**/
/*data test control;*/
/*set temp;*/
/*if flag="TEST" then output test;else output control;*/
/*run;*/
/* */
/*proc sql;*/
/*create table monthly_spends as*/
/*select */
/*	cust_id,*/
/*	month,*/
/*	max(flag) as flag,*/
/*	sum(bill_amt) as txn_amt*/
/*from like_to_like_data*/
/*group by 1,2;*/
/*quit;*/
/**/
/*proc sql;*/
/*create table month_summary as*/
/*select month,flag,mean(txn_amt)	as mean_txn_amt*/
/*from monthly_spends*/
/*where compress(month) ne "."*/
/*group by 1,2;*/
/*quit;*/
/**/
/*proc transpose data= month_summary out=month_summary_tt(drop=_name_);*/
/*id flag;*/
/*by month;*/
/*run;*/
/**/
/*proc print data= month_summary_tt;*/
/*run;*/

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*Delinquency*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

/*%let start=21JAN2016;*/
/*%let end=20FEB2016;*/
/*%let _1_month=20MAR2016;*/

proc sql;
create table cal_date_list as
select t1.*,t2.cal_date 
from (select  cal_date_skey,count(*) as count
      from rpt_dm.cc_provision group by 1) t1 left join rpt_dm.date_dim t2
on t1.CAL_DATE_SKEY=t2.CAL_DATE_SKEY
having count>2000000;
quit;

proc sql;
select  datepart(cal_date) format=date9. into: before
from cal_date_list
having abs(datepart(cal_date)-"&start."d)=min(abs(datepart(cal_date)-"&start."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: after
from cal_date_list
having abs(datepart(cal_date)-"&end."d)=min(abs(datepart(cal_date)-"&end."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: _1_after_new
from cal_date_list
having abs(datepart(cal_date)-"&_1_after."d)=min(abs(datepart(cal_date)-"&_1_after."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: _2_after_new
from cal_date_list
having abs(datepart(cal_date)-"&_2_after."d)=min(abs(datepart(cal_date)-"&_2_after."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: _3_after_new
from cal_date_list
having abs(datepart(cal_date)-"&_3_after."d)=min(abs(datepart(cal_date)-"&_3_after."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: _4_after_new
from cal_date_list
having abs(datepart(cal_date)-"&_4_after."d)=min(abs(datepart(cal_date)-"&_4_after."d));
quit;

proc sql;
select datepart(cal_date) format=date9. into: _5_after_new
from cal_date_list
having abs(datepart(cal_date)-"&_5_after."d)=min(abs(datepart(cal_date)-"&_5_after."d));
quit;

proc sql;
select cal_date_skey into :date_list separated by ',' from rpt_dm.date_dim
where datepart(cal_date) in ("&before."d,"&after."d,"&_1_after_new."d,"&_2_after_new."d,"&_3_after_new."d,"&_4_after_new."d,"&_5_after_new."d);
quit;

%put &date_list.;

options symbolgen mprint mlogic;
proc sql;
create table cc_provision as
select account_no,dpd,cal_Date_skey,overdue_amt  from rpt_dm.cc_provision
where cal_Date_skey in (&date_list.);
quit;

proc sql;
create table delinquency1 as
select t1.*,t2.cal_date_skey,t2.overdue_amt,case when abs(t2.overdue_amt)<=100 then 0 else t2.dpd end as dpd
from (select cust_id,acc_no,max(flag) as flag from cust_map 
/*		where cust_id in (select cust_id from ttest_Data1_mean) */
group by 1,2 ) t1 left join cc_provision t2
on  t1.acc_no=t2.account_no;
quit;

proc sort data= delinquency1 nodupkey out= delinquency_mid;
by  cust_id acc_no cal_date_skey;
run;

proc sql;
create table delinquency2 as
select t1.*,datepart(t2.cal_date) as dpd_as_on format=date9.,
		case when dpd=0  then "A.0" 
		when 30>= dpd>0  then "B.1-30"
		when dpd>30 then "C.>30" end as buckets
from delinquency_mid t1 left join rpt_dm.date_dim t2
on t1.cal_date_skey=t2.cal_date_skey;
quit;

/*data adhoc5.delinquency2_DEC;*/
/*set delinquency2;*/
/*run;*/

proc sql;
   create table delinquency3 as 
   select t1.dpd_as_on, 
          t1.buckets, 
          t1.flag, 
            (count(t1.cust_id)) as count
      from work.delinquency2 t1
      group by 1,2,3
/*	  having buckets ne " "*/
	  order by 3,2
;
quit;

proc transpose data=delinquency3 out=delinquency4 prefix=_;
by flag buckets;
id dpd_as_on;
run;

proc contents data= delinquency4  out=delinquency_list(where=(anydigit(name) ne 0));
run;

proc sql;
select name into: retain separated by "," from delinquency_list 
order by input(compress(name,"_"),date9.);
quit;

%put &retain.;
proc sort data=delinquency4;
by buckets descending flag;
run;

data delinquency5;
set delinquency4(drop=_name_);
format "Flow % DEC to JAN"n "Flow % JAN to FEB"n percent.;
"Flow from DEC to JAN"n=_30JAN2016-_21DEC2015;
"Flow from JAN to FEB"n=_29FEB2016-_30JAN2016;

"Flow % DEC to JAN"n="Flow from DEC to JAN"n/_21DEC2015;
"Flow % JAN to FEB"n="Flow from JAN to FEB"n/_30JAN2016;
run;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*bucket-flow*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
create table bucket_flow as
select distinct
	t1.cust_id,
	t1.acc_no,
	t1.flag,
	t2.buckets as before_campaign,
	t3.buckets as after_campaign,
	t4.buckets as _1_month_after_campaign,
	t5.buckets as _2_month_after_campaign,
	t6.buckets as _3_month_after_campaign,
	t7.buckets as _4_month_after_campaign,
	t8.buckets as _5_month_after_campaign
from delinquency2(where=(cal_date_skey ne .)) t1
left join delinquency2(where=(dpd_as_on="&before."d)) t2
on t1.acc_no=t2.acc_no
left join delinquency2(where=(dpd_as_on="&after."d)) t3
on t1.acc_no=t3.acc_no
left join delinquency2(where=(dpd_as_on="&_1_after_new."d)) t4
on t1.acc_no=t4.acc_no
left join delinquency2(where=(dpd_as_on="&_2_after_new."d)) t5
on t1.acc_no=t5.acc_no
left join delinquency2(where=(dpd_as_on="&_3_after_new."d)) t6
on t1.acc_no=t6.acc_no
left join delinquency2(where=(dpd_as_on="&_4_after_new."d)) t7
on t1.acc_no=t7.acc_no
left join delinquency2(where=(dpd_as_on="&_5_after_new."d)) t8
on t1.acc_no=t8.acc_no;
quit;

proc sql;
   create table before_after as 
   select t1.flag, 
          t1.before_campaign, 
          t1.after_campaign, 
		(count(t1.cust_id)) as count_of_cust_id
      from work.bucket_flow t1
      group by t1.flag,
               t1.before_campaign,
               t1.after_campaign
	order by 2,3;
quit;

proc transpose data=before_after out=before_after_t(drop=_name_);
by before_campaign after_campaign;
id flag;
run;

proc sql;
select flag,buckets,&retain. from delinquency4;
quit;

proc sql;
select 
before_campaign,
after_campaign,
test,control,
test/sum(test) as test_percent format=percent12.1,
control/sum(control) as control_percent format=percent12.1
from before_after_t
where before_campaign ne "C.>30";
quit;

%macro display1(i);

proc sql;
   create table before_&i._month_after as 
   select t1.flag, 
          t1.before_campaign, 
          t1._&i._month_after_campaign, 
		(count(t1.cust_id)) as count_of_cust_id
      from work.bucket_flow t1
      group by t1.flag,
               t1.before_campaign,
               t1._&i._month_after_campaign
	order by 2,3;
quit;

proc transpose data=before_&i._month_after out=before_&i._month_after_t(drop=_name_);
by before_campaign _&i._month_after_campaign;
id flag;
run;

proc sql;
select 
before_campaign,
_&i._month_after_campaign,
test,
control,
Test/sum(test) as test_percent format=percent12.1, 
control/sum(control) as control_percent format=percent12.1
from before_&i._month_after_t where before_campaign ne "C.>30";
quit;
%mend;

%display1(1)
%display1(2)
%display1(3)
%display1(4)
%display1(5)

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*>30 and <30 */*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;


proc sql;
create table delinquency_30 as
select t1.*,datepart(t2.cal_date) as dpd_as_on format=date9.,
		case when 0<dpd<=30  then "1-30" 
			when dpd>30 then ">30" 
			when dpd=0 then "0" end as buckets
from delinquency_mid t1 left join rpt_dm.date_dim t2
on t1.cal_date_skey=t2.cal_date_skey;
quit;

proc freq data=delinquency_30(where=(flag="TEST"));
table dpd_as_on*buckets;
run;

proc transpose data=before_after out=before_after_t(drop=_name_);
by before_campaign after_campaign;
id flag;
run;

/*data delinquency_30;*/
/*set adhoc5.DELINQUENCY_30_DEC;*/
/*run;*/

proc sql;
create table delinquency_30_1 as
select buckets, dpd_as_on,flag,count(distinct cust_id) as cust_count
from delinquency_30
group by 1,2,3
order by 3,2;
quit;

proc transpose data= delinquency_30_1 out=delinquency_30_2;
by flag dpd_as_on;
id buckets;
run;
 
proc sql;
create table display as
select *,">30"n/("0"n+"1-30"n+">30"n) as greater_than_30  format=percent12.2 ,
"1-30"n/("0"n+"1-30"n+">30"n) as less_than_30 format=percent12.2 from delinquency_30_2
where dpd_as_on ne .;
quit;

proc sort data=display;
by dpd_as_on;
run;

proc transpose data= display(drop=_name_) out=display1;
by  dpd_as_on;
id flag;
var _numeric_;
run;

proc tabulate data=display1(where=(_name_ ne "dpd_as_on")) order=data;
class dpd_as_on _name_;
var test control;
tables _name_="",dpd_as_on=""*(TEST control)*max=""*f=percent12.2;
run;
 

data delinquency_30_days_splits;
length buckets $10.;
set delinquency2(drop=buckets);	
if dpd=0  then buckets="A.0";else 
		if 30>= dpd>0  then buckets="B.1-30";else
		if dpd>30 then buckets="C.>30";
run;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;


proc sql;
select (count(case when channel in ("SMS_ACL","SMS_NC") then account_no end)*0.13)+
(count(case when channel in ("eMessage","E_NETCORE") then account_no end)*0.225) from estatement_opt_out_new
where contactstatusid in(1,2,3);
quit;

proc sql;
select channel,count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid in(1,2,3) and channel in ("eMessage",'E_NETCORE','SMS_ACL') group by channel;
run; 

proc sql;
title "1,2,3";
select channel,count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid in(1,2,3) and channel in ("eMessage",'E_NETCORE','SMS_ACL') group by channel;
run; 

proc sql;
title "2";
select channel,count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid=2 and channel in ("eMessage",'E_NETCORE','SMS_ACL') group by channel;
select count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid=2 and channel in ("eMessage",'E_NETCORE','SMS_ACL') ;
run; 
 
proc sql;
title "1,2,3";
select count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid=2 and channel in ("eMessage",'E_NETCORE','SMS_ACL');
run; 

proc sql;
select contact_date,channel,count(distinct account_no) as cust from 
estatement_opt_out_new where 
contactstatusid=2 and channel in ("eMessage",'E_NETCORE','SMS_ACL') group by contact_date,channel;
run; 
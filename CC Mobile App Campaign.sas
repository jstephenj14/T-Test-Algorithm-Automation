
proc sql;
create table adhoc5.cc_mobile_app as
select 
	CHANNEL,
	CONTACTSTATUSID,
	CONTACTDATETIME,
	datepart(CONTACTDATETIME) as contact_date format=date9.,
	CUSTOMERID,
	put(CUSTOMERID,z9.) as cust_id 
from _axuresp.vw_contact_customer 
where campaigncode="C000001167" and 
	'01OCT2015'd <= datepart(contactdatetime) <= '31OCT2015'd;
quit;

%doitnow(adhoc5,cc_mobile_app,cust_id);

data contact_data;
set adhoc5.cc_mobile_app(where=(contactstatusid=2));
format time time.;
time=timepart(contactdatetime);
Time_of_day=ifc("6:00"t<=time<="9:00"t,"A.M.","P.M.");
/*cust_id=put(customerid,z9.);*/
run;

proc sql;
create table contact_frequency as
select 
	cust_id,
	count(case when Time_of_day="P.M." then cust_id end) as Contacts_P_M,
	count(case when Time_of_day="A.M." then cust_id end) as Contacts_A_M,
	case when calculated Contacts_P_M>0 then 1 else 0 end as P_M_Contacted,
	case when calculated Contacts_A_M>0 then 1 else 0 end as A_M_Contacted,
	count(*) as Total_no_of_contacts
from contact_data
group by 1;
quit;

proc sql;
select 
	sum(Contacts_P_M) as Contacts_P_M,
	sum(Contacts_A_M) as Contacts_A_M,
	sum(P_M_Contacted) as P_M_Contacted,
	sum(A_M_Contacted) as A_M_Contacted,
	sum(Total_no_of_contacts) as Total_no_of_contacts
from contact_frequency;
quit;

proc sql;
create table mb_conversion as 
select a.*,case when b.ONBOARDED_CUST_ID ne " " then 1 else 0 end as convert,b.CREATED_ON
from 
(
	select distinct cust_id,put(CUSTOMERID,z9.) as cust_id_unmasked,
		case when CONTACTSTATUSID=2 then "TEST" else "CONTROL" end as flag,
		contact_date 
	from  adhoc5.cc_mobile_app where CONTACTSTATUSID in (2,4) 

)
a left join adhoc5.MOBILEAPP_CAMPTEAM as b
on a.cust_id=b.ONBOARDED_CUST_ID
and "12OCT2015"d <= datepart(b.CREATED_ON) <= "31OCT2015"d
order by cust_id,convert desc;
quit;

proc sql;
create table mb_conversion1 as
select 
	cust_id,
	cust_id_unmasked,
	max(flag) as flag,
	max(convert) as convert
from mb_conversion
group by 1,2;
quit;

proc sql;
create table acc_no_mapping as
select t1.*,t2.acc_no
from mb_conversion1(where=(convert=1)) t1 left join maindata.i_grid_108 t2
on t1.cust_id=t2.cust_id;
quit;

proc sql;
create table fin_txn_mapping as
select t1.cust_id,t1.flag,t1.convert,t1.acc_no,t2.*
from acc_no_mapping	 t1 left join adhoc5.MBtrxn_post_Oct t2
on t1.acc_no=t2.FROM_ACC_NUM;
quit;

data fin_txn_mapping1;
set fin_txn_mapping;
TRANS_AMOUNT_1=TRANS_AMOUNT*1;
run;

proc sql;
create table ib_mapping as
select t1.*,case when t2.IB_registered ne 1 then 0 else 1 end as IB_registered
from mb_conversion1 t1 left join adhoc5.ib_base t2
on t1.cust_id_unmasked=t2.cust_id;
quit;

proc sql;
create table cl_mapping as
select t1.*,t2.creditlimit,t2.card_number,t2.final_balance
/*,-t2.final_balance/t2.creditlimit as utilization_rate*/
,t2.month
from ib_mapping t1 left join 
(select distinct custid,card_number,creditlimit,final_balance,month from targpord.portfolio_data_updated where aif_flag=1
and 201504<=month<=201612) t2
on t1.cust_id=t2.custid;
quit; 

proc sql;
create table mb_conversion2 as
select t1.*,t2.card_no1
from mb_conversion1 t1 left join adhoc5._new t2
on t1.cust_id=t2.cust_id1;
quit;

proc sql;
create table spends_mapping as
select t1.*,t2.billamt,t2.trxn_dt
from mb_conversion2 t1 left join targpord.crd_trxn_ods_data(where=()) t2
on t1.card_no1=t2.card_number;
quit;

proc sql;
create table cl_mapping1 as
select 
	cust_id,month,-sum(final_balance)/sum(creditlimit) as utilization_rate
from cl_mapping
where 201504<=month<=201510
group by 1,2;
quit;

data cl_mapping2;
set cl_mapping1;
utilization_rate=ifn(utilization_rate<0,0,round(utilization_rate*100,1));
run;

proc sql;
create table cl_mapping3 as
select t1.*,t2.utilization_rate
from cl_mapping(where=(201504<=month<=201510)) t1 left join cl_mapping2 t2
on t1.cust_id=t2.cust_id and t1.month=t2.month;
quit;

/*proc sql;*/
/*create table month_wise as*/
/*select*/
/*	convert,month,mean(utilization_rate) as avg_utilization_rate*/
/*from cl_mapping2*/
/*group by 1,2*/
/*order by convert;*/
/*quit;*/
/**/
/*proc transpose data= month_wise out=month_wise_tt;*/
/*by convert;*/
/*var avg_utilization_rate;*/
/*id Month;*/
/*run;*/

proc sql;
create table roll_up_util as
select cust_id,flag,convert,mean(utilization_rate) as avg_utilization_rate,
case when calculated avg_utilization_rate<1 then "A.< 1%" 
	 when  1<=calculated avg_utilization_rate<25 then "B.1%-25%" 
	 when  25<=calculated avg_utilization_rate<50 then "C.25%-50%" 
	 when  50<=calculated avg_utilization_rate<75 then "D.50%-75%" 
	 when  75<=calculated avg_utilization_rate<100 then "E.75%-100%" end as util_buckets
from (select distinct cust_id,flag,convert,utilization_rate,month from cl_mapping3)
group by 1,2,3;
quit;

proc sort data=roll_up_util;
by avg_utilization_rate;
run;

data roll_up_util1;
set roll_up_util;
retain cumulative 0;
cumulative+convert;
run;

ods graphics on;
proc freq data= roll_up_util;
table util_buckets*convert/chisq nopercent expected plots=mosaic;
run;

proc sgplot data=roll_up_util;
histogram avg_utilization_rate;
run;

ods graphics on;
proc freq data= ib_mapping;
table ib_registered*convert/chisq nocol nopercent expected plots=mosaic;
run;
libname prasad "/SAS/BIU/RO_TEAM4_3/prasad";

proc sql;
create table lift_calculation as
select 
	flag,
	util_buckets,
	count(distinct cust_id) as count_total,
	count(distinct case when convert=1 then cust_id end) as count_convert,
	count(distinct case when convert=1 then cust_id end)/count(distinct cust_id) as conversion_rate
from roll_up_util
group by 1,2
order by 2;
quit;

proc transpose data=lift_calculation out=lift_calculation1
/*(where=(_name_='conversion_rate'))*/
;
by  util_buckets;
id flag;
run;

proc sql;
create table roll_up_util2 as
select t1.*,t2.*
from roll_up_util1 t1 left join prasad.i_xh_new_103 t2
on t1.cust_id=t2.cust_id;
quit;

proc sql;
create table lift_calculation_xh as
select 
	flag,
/*	case when NO_OF_XH<=2 then "<=2"*/
/*		 when 3<=NO_OF_XH<=6 then "3-6"*/
/*		 when 7<=NO_OF_XH then ">=7" end as*/
	case when NO_OF_XH<=2 then "<=2"
		 else ">2" end as
/*	NO_OF_XH as */
	XH_buckets,
	count(distinct cust_id) as count_total,
	count(distinct case when convert=1 then cust_id end) as count_convert,
	count(distinct case when convert=1 then cust_id end)/count(distinct cust_id) as conversion_rate
from roll_up_util2
group by 1,2
order by 2;
quit;

proc transpose data=lift_calculation_xh out=lift_calculation_xh1
/*(where=(_name_='conversion_rate'))*/
;
by  XH_buckets;
id FLAG;
run;

proc sql;
create table roll_up_util3 as
select t1.*,t2.ib_registered
from roll_up_util2 t1 left join ib_mapping t2
on t1.cust_id=t2.cust_id;
quit;

proc sql;
create table roll_up_util4 as
select t1.*,t2.*
from roll_up_util3 t1 left join contact_frequency t2
on t1.cust_id=t2.cust_id;
quit;

data test_contact;
set roll_up_util4(where=(flag="TEST") keep=cust_id convert Contacts_P_M
Contacts_A_M
P_M_Contacted
A_M_Contacted
Total_no_of_contacts flag
);
run;

%macro contact_summary(var);

	title "&var.";
	proc sql;
	select &var.,sum(convert) as converts,count(*) as total,calculated converts/calculated total as conversion
	from test_contact
	group by 1;
	quit;

%mend;

%contact_summary(Contacts_P_M);
%contact_summary(Contacts_A_M);
%contact_summary(P_M_Contacted);
%contact_summary(A_M_Contacted);
%contact_summary(Total_no_of_contacts);

data adhoc5.cc_mobile_app_summary;
set roll_up_util4;
run;

proc contents data=adhoc5.cc_mobile_app_summary out=var_list;
run;

proc sql;
select count(*) into: count from var_list where index(name,"HAS_") ne 0;
select name into: var1-:var%left(&count.) from var_list where index(name,"HAS_") ne 0;
quit;

%put &var1. &var2.;

options symbolgen mprint mlogic;
%macro freq();

	%do i=1 %to &count.;

		%put &i. &&var&i.;

		title "&&var&i.";
		proc freq data=adhoc5.cc_mobile_app_summary;
		table &&var&i.*convert/nocol nopercent chisq expected;
		run;

	%end;
%mend; 

%freq();


/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*LOYALTY POINTS*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;


/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*age*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

proc sql;
create table age_mapping as
select t1.*,t2.age_106 as age,t2.Region_Cat_Name_C as region_category,t2.mob_106-3 as mob
from adhoc5.cc_mobile_app_summary t1 left join maindata.i_grid_106_c t2
on t1.cust_id=t2.cust_id;
quit;

data adhoc5.age_mapping;
set age_mapping;
run;

proc sql;
create table age_roll_up as
select 
	case when 18<=age<=30 then "A.18-30"
	 when 30<age<=40 then "B.31-40"
	 when 40<age then "C.>41"
	 end as age_bucket,
	flag,
	sum(convert) as convert,
	count(*) as total,
	calculated convert/calculated  total as conversion
from age_mapping
where age ne . and 18<=age<103
group by 1,2
order by 1;
quit;

proc transpose data=  age_roll_up out=age_roll_up_t;
by age_bucket;
/*var conversion;*/
id flag;
run;

proc sql;
create table mob_roll_up as
select 
	case when ceil(mob/12)<=2  then "A.0-2" 
		when 3<=ceil(mob/12)<=5  then "B.3-5"
		when 6<=ceil(mob/12)<=8  then "C.6-8"
		when 9<=ceil(mob/12)<=11  then "D.9-11"
		when ceil(mob/12)>11 then  "E.>11" end as years_on_book,
	flag,
	sum(convert) as convert,
	count(*) as total,
	calculated convert/calculated  total as conversion
from age_mapping
group by 1,2
order by 1;
quit;

proc sql;
create table mob_chk as
select 
	ceil(mob/12) as years,
	count(*) as total
from age_mapping
group by 1;
quit;

proc transpose data=  mob_roll_up out=mob_roll_up_t;
by years_on_book;
/*var conversion;*/
id flag;
run;

proc sort data= age_mapping(where=(age < 103 and age ne . and age>18)) out=age_mapping1;by age;run;

proc sql;
select
channel,
	count(distinct case when contactstatusid in (1,2,3) then customerid end) as target_population,
	count(distinct case when contactstatusid in (2) then customerid end) as delivered_population,
	calculated delivered_population/calculated target_population as delivered_population
from adhoc5.cc_mobile_app
group by 1;
quit;

proc sql;
   select t1.flag, 
            (count(t1.cust_id)) as count_of_cust_id, 
            (sum(t1.convert)) as sum_of_convert,
			calculated sum_of_convert/calculated count_of_cust_id as convert_percent
      from mb_conversion1 t1
      group by t1.flag;
quit;

proc sql;
create table region_roll_up as
select 
	region_category,
	flag,
	sum(convert) as convert,
	count(*) as total,
	calculated convert/calculated  total as conversion
from age_mapping
where anyalpha(region_category) ne 0
group by 1,2
order by 1;
quit;

proc transpose data=  region_roll_up out=region_roll_up_t;
by region_category;
/*var conversion;*/
id flag;
run;
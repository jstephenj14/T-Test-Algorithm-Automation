
proc sql;
create table etb_base as
select 
	t1.cust_id,
	t1.MDAB_99 as MDAB,
	"TEST" as flag,
	t2.Circle_Name_C as circle
from adhoc3.etb_dashboard_jul15 (where=(fd_broken_before_matur_jul15_t=1)) t1
left join adhoc3.etb_dashboard_april_may_jun t2
on t1.cust_id=t2.cust_id
union
select 
	t1.cust_id,
	t1.MDAB_87 as MDAB,
	"CONTROL" as flag,
	t2.Circle_Name_C as circle
from adhoc3.etb_dashboard_jul15 (where=(fd_broken_before_matur_jul15_c=1)) t1
left join adhoc3.etb_dashboard_april_may_jun t2
on t1.cust_id=t2.cust_id;
quit;

proc ttest data=etb_base plots(only)=box;
var MDAB;
class flag;
run;

data etb_base;
set etb_base;
MDAB=round(MDAB,0.01);
run;

data control;
set etb_base;
where flag="CONTROL";
run;

data test;
set etb_base;
where flag="TEST";
run;

proc sql;
	create table total like record;
run;

%macro remove();

proc sql;
select round(count(*)*0.02,1) into:count from test;
quit;

%do i=0 %to &count.;
	proc sql;
	create table record as
	select 
		&i. as obs_removed,
		max(MDAB)   as max,
		mean(MDAB)  as mean, 
		range(MDAB) as range
	from test;
	run;

	data total;
	set total record;
	run;

	proc sql;
	create table test as
	select * from test
	having MDAB ne max(MDAB);
	quit;
%end;

%mend;

%remove();

proc sql;
	create table total_c like record;
run;

%macro remove_c();

proc sql;
select round(count(*)*0.05,1) into:count from control;
quit;

%do i=0 %to &count.;
	proc sql;
	create table record_c as
	select 
		&i. as obs_removed,
		max(MDAB) as max,
		mean(MDAB) as mean, 
		range(MDAB) as range
	from control;
	run;

	data total_c;
	set total_c record_c;
	run;

	proc sql;
	create table control as
	select * from control
	having MDAB ne max(MDAB);
	quit;
%end;

%mend;

%remove_c();

data total_renamed(drop=obs_Removed);
set total;
rename max=max_t mean=mean_t range=range_t;
test_limit=lag(max);
run;

data total_c_renamed(drop=obs_Removed);
set total_c;
rename  max=max_c mean=mean_c range=range_c;
control_limit=lag(max);
run;

proc sql;
create table mean_minimizer as
select *,abs(x.mean_t-y.mean_c) as mean_diff,abs(x.range_t-y.range_c) as range_diff
/*put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit, :control_limit*/
from total_renamed x,total_c_renamed y
/*having abs(x.mean_t-y.mean_c) =min(abs(x.mean_t-y.mean_c))*/
order by mean_diff 
; 
quit;

proc sql;
select put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit, :control_limit from mean_minimizer(obs=1); 
select put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit_range, :control_limit_range 
from mean_minimizer(obs=15)
having range_diff=min(range_diff);
quit;

title "Mean";
proc ttest data=etb_base(where=((flag="TEST" and MDAB<&test_limit.) or (flag="CONTROL" and MDAB<&control_limit.)));
var MDAB;
class flag;
run;

title "Range";
proc ttest data=etb_base(where=((flag="TEST" and MDAB<&test_limit_range.) or (flag="CONTROL" and MDAB<&control_limit_range.)));
var MDAB;
class flag;
run;


%macro ttest_v2(dataset,var);

data &dataset.;
set &dataset.;
&var.=round(&var.,0.01);
array t _numeric_;
do over t;
	if t=. then t=0;
end;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_before,:control_before,:total_before
from &dataset;
quit;

%macro remove(flag,limit);

proc sql;
	select round(count(*)*&limit.,1) into:count from &flag.;
quit;

proc sql;
create table total_&flag.
	( 
		obs_removed_&flag. num,
		max_&flag. num,
		mean_&flag. num,
		range_&flag. num
	);
quit;

%do i=0 %to &count.;
	proc sql;
	create table record_&flag. as
	select 
		&i. as obs_removed_&flag.,
		max(MDAB)   as max_&flag.,
		mean(MDAB)  as mean_&flag., 
		range(MDAB) as range_&flag.
	from &flag.;
	run;

	data total_&flag.;
	set total_&flag. record_&flag.;
	run;

	proc sql;
	create table &flag. as
	select * from &flag.
	having &var. ne max(&var.);
	quit;
%end;
%mend;

%remove(test,0.20);
%remove(control,0.20);

data total_renamed(where=(test_limit ne .) drop=max_test);
set total_test;
test_limit=lag(max_test);
run;

data total_c_renamed(where=(control_limit ne .) drop=max_control);
set total_control;
control_limit=lag(max_control);
run;

proc sql;
create table mean_minimizer as
select *,abs(x.mean_test-y.mean_control) as mean_diff,abs(x.range_test-y.range_control) as range_diff
/*put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit, :control_limit*/
from total_renamed x,total_c_renamed y
/*having abs(x.mean_t-y.mean_c) =min(abs(x.mean_t-y.mean_c))*/
order by mean_diff 
; 
quit;

proc sql noprint;
select put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit, :control_limit from mean_minimizer(obs=1); 
select put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit_range, :control_limit_range 
from mean_minimizer(obs=15)
having range_diff=min(range_diff);
quit;

data &dataset._mean;
set &dataset.(where=((flag="TEST" and MDAB<&test_limit.) or (flag="CONTROL" and MDAB<&control_limit.)));
run;

data &dataset._range;
set &dataset.(where=((flag="TEST" and MDAB<&test_limit_range.) or (flag="CONTROL" and MDAB<&control_limit_range.)));
run;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_mean,:control_mean,:total_mean
from &dataset._mean;
quit;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_range,:control_range,:total_range
from &dataset._range;
quit;

data x;
y=1;
run;

proc sql;
title "MEAN LOSS";
select 
	(&test_before-&test_mean)*100/&test_before as test_loss,
	(&control_before-&control_mean)*100/&control_before as control_loss from x;
title "RANGE LOSS";
select 
	(&test_before-&test_range)*100/&test_before as test_loss,
	(&control_before-&control_range)*100/&control_before as control_loss from x;
quit; 

title "Range";
proc ttest data=&dataset._range;
var &var.;
class flag;
run;

title "Mean";
proc ttest data=&dataset._mean;
var &var.;
class flag;
run;
title;
%mend;


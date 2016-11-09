
%macro ttest_v3(dataset,var,t=0.01,c=0.10);

data x;
y=1;
run;

proc sql;
select put(time(),HHMM.) as Started_at from x;
quit;

data &dataset.;
set &dataset.;
&var.=round(&var.,0.01);
array t _numeric_;
do over t;
	if t=. then t=0;
end;
run;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_before,:control_before,:total_before
from &dataset.;
quit;

data test control;
set &dataset.;
if flag="TEST" then output TEST;
if flag="CONTROL" then output CONTROL;
run;

proc sort data=test;by descending &var.; run;
proc sort data=control;by descending &var.; run; 

proc sql noprint;
	select round(count(*)*&t.,1) into:count_test from test;
	select round(count(*)*&c.,1) into:count_control from control;
quit;

%put count_test=&count_test. count_control=&count_control.;

%macro summary(flag,var);

	proc sql noprint;
	select put(sum(&var.),best20.2), min(&var.) into :sum,:min
	from &flag.;
	quit;

	data &flag._summary(keep= mean max range rename=(mean=mean_&flag. max=max_&flag. range=range_&flag.));
		if 0 then set &flag. nobs=no;
	merge &flag. &flag. (firstobs=2 keep=&var. rename=(&var.=max)) ;
	retain cumulative 0;
	cumulative+&var.;
	mean=(&sum.-cumulative)/(no-_n_);
	range=max-&min.;
	run;

%mend;

%summary(test,&var.);
%summary(control,&var.);

proc sql;
create table mean_minimizer as
select *,abs(x.mean_test-y.mean_control) as mean_diff,abs(x.range_test-y.range_control) as range_diff
/*put(test_limit,best20.2),put(control_limit,best20.2) into:test_limit, :control_limit*/
from test_summary(obs=&count_test.) x,control_summary(obs=&count_control.) y
/*having abs(x.mean_t-y.mean_c) =min(abs(x.mean_t-y.mean_c))*/
order by mean_diff 
; 
quit;

proc sql noprint;
select put(max_test,best20.2),put(max_control,best20.2) into:test_limit, :control_limit from mean_minimizer(obs=1); 
select put(max_test,best20.2),put(max_control,best20.2) into:test_limit_range, :control_limit_range 
from mean_minimizer(obs=15)
having range_diff=min(range_diff);
quit;

data &dataset._mean;
set &dataset.(where=((flag="TEST" and &var.<=&test_limit.) or (flag="CONTROL" and &var.<=&control_limit.)));
run;

data &dataset._range;
set &dataset.(where=((flag="TEST" and &var.<=&test_limit_range.) or (flag="CONTROL" and &var.<=&control_limit_range.)));
run;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_range,:control_range,:total_range
from &dataset._range;
quit;

proc sql noprint;
select 
	count(case when flag="TEST" then &var. end),
	count(case when flag="CONTROL" then &var. end),
	count(*)
	into :test_mean,:control_mean,:total_mean
from &dataset._mean;
quit;

proc sql;
title "Range Loss";
select 
	"Test" as Group,
	&test_before as "Count before T-test"n,
	&test_range as "Count after T-test"n,
	(&test_before-&test_range) as "Loss"n,
	(&test_before-&test_range)*100/&test_before as "Loss %"n from x
union
select 
	"Control" as Group,
	&control_before as "Count before T-test"n,
	&control_range as "Count after T-test"n,
	(&control_before-&control_range) as "Loss"n,
	(&control_before-&control_range)*100/&control_before as "Loss %"n from x;

title "Mean Loss";
select 
	"Test" as Group,
	&test_before as "Count before T-test"n,
	&test_mean as "Count after T-test"n,
	(&test_before-&test_mean) as "Loss"n,
	(&test_before-&test_mean)*100/&test_before as "Loss %"n from x
union
select 
	"Control" as Group,
	&control_before as "Count before T-test"n,
	&control_mean as "Count after T-test"n,
	(&control_before-&control_mean) as "Loss"n,
	(&control_before-&control_mean)*100/&control_before as "Loss %"n from x;
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

proc sql;
select put(time(),HHMM.) as Ended_at from x;
quit;

%mend;

/*%ttest_v3(credit60_jun_total,&var.,t=0.01,c=0.20);*/

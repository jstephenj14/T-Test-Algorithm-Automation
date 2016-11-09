%macro run1();

	title "Pre-Test";
	
%let t_phrase=case when flag="TEST" then aqb end;
%let c_phrase=case when flag="CONTROL" then aqb end;

proc sql;
select 
(abs(mean(&t_phrase.) 
	- mean(&c_phrase.))) 
	/ sqrt(1/count(&t_phrase.)+1/count(&c_phrase.)) into: part1 from ens_total1;
select 
sqrt((((count(&t_phrase.)-1)*var(&t_phrase.))
+((count(&c_phrase.)-1)*var(&c_phrase.)))/
(count(&t_phrase.)+(count(&c_phrase.)-2))) into :part2 from ens_total1;
select count(&c_phrase.)+count(&t_phrase.)-2 into :degrees_of_freedom from ens_total1;
quit;
%put &part1;
%put &part2;

%let probit_val=%sysfunc(probt(%sysevalf(&part1/&part2),&degrees_of_freedom));

	%put &probit_val.;

	%do %until (&probit_val.>0.98);
	data ens_total1;
	length flag $30.;
	set  TTEST_TEST(where=(aqb<&test_max.)) TTEST_CONTROL(where=(aqb<&control_max.));
	run;

	proc sql;
	select put(mean(aqb),best12.) into: test_mean_chk from ens_total1 where flag="TEST";/*pacl_comm_aqb2*/
	select put(mean(aqb),best12.) into: control_mean_chk from ens_total1 where compress(flag)="CONTROL";/*control_ens_aqb3*/
	select put(max(aqb) ,best12.) into: test_max_chk from ens_total1 where flag="TEST";/*pacl_comm_aqb2*/
	select put(max(aqb) ,best12.) into: control_max_chk from ens_total1 where compress(flag)="CONTROL";/*control_ens_aqb3*/
	run;

	options symbolgen mprint;

	%put 
	&test_mean_chk.
	&control_mean_chk.
	&test_max_chk.
	&control_max_chk.;

	%if %sysevalf(&test_mean_chk. >= &control_mean_chk.) 
		%then 
			%do;
				%let test_max=&test_max_chk.;
			%end;
		%else
			%do;
				%let control_max=&control_max_chk.;
			%end;

	%put 
	&test_max.
	&control_max.;

	title "Post-Test";
proc sql;
select 
(abs(mean(&t_phrase.) 
	- mean(&c_phrase.))) 
	/ sqrt(1/count(&t_phrase.)+1/count(&c_phrase.)) into: part1 from ens_total1;
select 
sqrt((((count(&t_phrase.)-1)*var(&t_phrase.))
+((count(&c_phrase.)-1)*var(&c_phrase.)))/
(count(&t_phrase.)+(count(&c_phrase.)-2))) into :part2 from ens_total1;
select count(&c_phrase.)+count(&t_phrase.)-2 into :degrees_of_freedom from ens_total1;
quit;
%put &part1;
%put &part2;

%let probit_val=%sysfunc(probt(%sysevalf(&part1/&part2),&degrees_of_freedom));

	%end;
%mend;

%run1();

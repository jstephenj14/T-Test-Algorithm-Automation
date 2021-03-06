# T-test Algorithm Automation

## Background
The two sample T-test is, in the roughest terms, used to detect whether the difference in means of two samples is significantly different. If the p-value that such a T-test provides is less that 0.05, we are told to reject the null hypothesis that difference in means is non-significant.

I imagine that most T-tests are set up in such a way that their null hypotheses are meant to be disproved. However, I have seen the two sample T-test applied in an inverted manner in order to determine the similarity between two samples. In our marketing analytics team, our client was interested in ensuring that their test and control groups for campaigns was similar with respect to certain characteristics. For example, if a campaign was targeted at test and control groups of checking account customers, the client wanted to make the two groups have equal mean account balance.  

While the methods of causal inference would have been more apt to the problem, they chose to turn the usual two sample T-Test on its head to ascertain this kind of similarity. With iterative T-tests and outlier removal (done manually), once the p-value exceeded 0.95, the two groups were claimed to be similar. The flow-diagram below best describes the outline of this process:

<a href="https://drive.google.com/uc?export=view&id=1t-q-ZFLAx5d4M4NKQOHrs99UZfIPLzcA"><img src="https://drive.google.com/uc?export=view&id=1t-q-ZFLAx5d4M4NKQOHrs99UZfIPLzcA" style="width: 500px; max-width: 100%; height: auto" title="WOE Table" /></a>

During the manual iterations, adjustments are made to try and equalize the ranges of Test and Control. Under severe constraints, the equalization of the means is given priority. Also, instead of dropping the maximum outlier to reduce the mean of one group, the minimum outlier from the other group may also be dropped to raise the other group's mean. This is rarer, so I will restrict further discussion to removal of maximum outliers.

I have reservations with the statistical background of this process. I can't quite put it into words yet, but setting up the null to prove it right doesn't sit well with me. The right path, in my eyes, would be to set up the null hypothesis to be that the means were _dissimilar_, and then go about disproving it. 

## Problem Statement

The problem I discuss here is the automation of the flow chart above. When doing this process manually, it is phenomenally tedious and time-consuming. The natural solution would be to implement a simple SAS macro that incorporates the flowchart above. This, however, quickly runs into time problems. SAS' PROC TTEST consumes time and groups with many outliers would take considerable amount of time to clean up. SAS code for my initial stab at this is available [here](https://github.com/jstephenj14/T-Test-Algorithm-Automation/blob/master/T%20test%20Macro%20v1.sas).

## Solution

A good angle of attack is to understand how the removal of each outlier affects the mean and range of a given group. We can create a dataset that contains the mean and range of the group for every exclusion of data below a certain purported outlier. The algorithm may be delineated as below:

_Step 1_: Create tuple containing maximum value of Test, mean of the Test if the maximum value was excluded and respective range. 

_Step 2_: Remove maximum value of Test from Test

_Step 3_: Repeat Step 1 and append tuple to the tuple created in Step 1. Repeat 1 through 3 until exhaustion of Test or a predetermined number of rows of Test.

_Step 4_: Repeat Step 1 through 4 for Control.

In essence, we now have one descriptive data set for Test and another for Control. 

The final step of the solution would be to compare which means in both descriptive datasets are closest with the most similar ranges as well. The idea here is that if the means are nearby each other, the p-value of a resultant T-test would automatically exceed 0.95. This makes running repetitive T-tests redundant.

This comparison can be acheived by a Cartesian join between the two descriptive datasets.

## Technical Details

The implementation of the logic above also leads into obstacles of its own. The use of SAS SQL for calculation of means and ranges coupled with an iterative do loop would still pose time consumption issues. Code attempting this is available [here](https://github.com/jstephenj14/T-Test-Algorithm-Automation/blob/master/T%20test%20Macro%20v2.sas).

The most optimal solution I could come up with is to deploy a data step that takes the Test or Control data set along with the maximum value and calculates ranges and means within the dataset itself. This prevents time-consuming iteration and provides additional insights into how the entire dataset is distributed.

The final implementation of the solution is available [here](https://github.com/jstephenj14/T-Test-Algorithm-Automation/blob/master/T%20test%20Macro%20v3.sas).

## Additional Conclusions

It is worth reiterating here that I believe the statistical foundations for a process like this are suspect and ideas in causal inference seem more appropriate here. However, the problem statement is an intriguing one and its solution offers much for learning.



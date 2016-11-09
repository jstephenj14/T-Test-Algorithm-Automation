# T-Test Algorithm Automation

## Background
The two sample T-test is, in the roughest terms, used to detect whether the difference in means of two samples is significantly different. If the p-value that such a T-test provides is less that 0.05, we are told to reject the null hyptothesis that difference in means is non-significant.

I imagine that most T-tests are set up in such a way that their null hypotheses are meant to be disproved. However, I have seen the two sample T-test applied in an inverted manner in order to determine the similarity between two samples. In our marketing analytics team, our client was interested in ensuring that their test and control groups for campaigns was similar with respect to certain characteristics. While the methods of causal inference would have been more apt to the problem, they chose to turn the usual two sample T-Test on its head to ascertain similarity. With iterative T-tests and outlier removal (done manually), once the p-value exceeded 0.95, the two groups were claimed to be similar. The flow-diagram below best describes the outline of this process:

![](https://s11.postimg.org/nv187sk8j/T_Test_Flow_Chart.png)

During the manual iterations, adjustments are made to try and equalize the ranges of Tes and Control. Under severe constraints, the equalization of the means is given priority.

I have reservations with the statistical background of this process. I can't quite put it into words yet, but setting up the null to prove it right doesn't sit well with me. The right path, in my eyes, would be to set up the null hypothesis to be that the means were _dissimilar_, and then go about disproving it. 

## Problem Statement

The problem I discuss here is the automation of the flow chart above. When doing this process manually, it is phenomenally tedious and time-consuming. The natural solution would be to implement a simple SAS macro that incorporates the flowchart above. This, however, quickly runs into time problems. SAS' PROC TTEST consumes time and groups with many outliers would take considerable amount of time to clean up.



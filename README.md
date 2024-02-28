# rewards-ulster-cashback-lambda
Reference JIRA: https://capillarytech.atlassian.net/browse/REW-2754
 
We wanted to send a communication to members (list will be provided manually on s3 ). 
This lambda will pick file from s3 and hit the intermediate DB for fetching Fname and package ufirst or ufirst Gold. Once these information is fetched , lambda can send communication api the below data in merge fields.
1) Fname
2) PackageName
3) Number of cashback
4) Cashback Reff Numbers

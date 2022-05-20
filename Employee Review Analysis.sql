USE shubham;
CREATE EXTERNAL TABLE IF NOT EXISTS EMPLOYEE2(
    E_INDEX INT,    
    COMPANY STRING,
    E_LOCATION STRING,
    DATES string,
    JOBTITLE STRING,
    SUMMARY STRING,
    PROS STRING,
    CONS STRING,
    overall_ratings INT,
    work_balance_stars INT,
    culture_values_stars INT,
    carrer_opportunities_stars INT,
    comp_benefit_stars INT,
    senior_mangemnet_stars INT)
row format delimited fields terminated by ","
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/anabig114215/employee_review_data.csv' INTO TABLE EMPLOYEE2;

SELECT * FROM EMPLOYEE2 LIMIT 5;

CREATE TABLE EMP3 AS 
SELECT E_INDEX,
    COMPANY ,
    (CASE 
    WHEN E_LOCATION LIKE "%@%" THEN replace(replace(regexP_extract(E_LOCATION,'@.*',0),")",""),"@","")
    ELSE "USA"
    End) as E_COUNTRY,
    (TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_DATE,
    QUARTER(E_DATE) AS E_QUARTER,
    JOBTITLE,
    SUMMARY ,
    PROS,
    CONS ,
    overall_ratings ,
    work_balance_stars ,
    culture_values_stars ,
    carrer_opportunities_stars,
    comp_benefit_stars,
    senior_mangemnet_stars 
    FROM employee2
  
    
    
SELECT * --(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_DATE--REPLACE(REPLACE(DATES,";","")," ","-")--,SUBSTRING(DATES,9,12)---_,replace(replace(regexP_extract(E_LOCATION,'@.*',0),")",""),"@","") as Country
FROM EMP1
 WHERE EMP_COUNTRY NOT LIKE "USA"
 LIMIT 10


CREATE TABLE EMP4 AS 
SELECT E_INDEX,
    COMPANY ,
    (CASE 
    WHEN E_LOCATION LIKE "%@%" THEN replace(replace(regexP_extract(E_LOCATION,'@.*',0),")",""),"@","")
    ELSE "USA"
    End) as E_COUNTRY,
    (TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_DATE,
    QUARTER(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) as E_QUARTER,
    YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_YEAR,
    JOBTITLE,
    SUMMARY ,
    PROS,
    CONS ,
    overall_ratings,
    work_balance_stars ,
    culture_values_stars ,
    carrer_opportunities_stars,
    comp_benefit_stars,
    senior_mangemnet_stars 
    FROM employee2

SELECT * FROM EMP4;
---CREATE TABLE EMP5 AS 
SELECT 
    E_INDEX,
    COMPANY ,
    (CASE 
    WHEN E_LOCATION LIKE "%@%" THEN replace(replace(regexP_extract(E_LOCATION,'@.*',0),")",""),"@","")
    ELSE "USA"
    End) as E_COUNTRY,
    (TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_DATE,
    QUARTER(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) as E_QUARTER,
    YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_YEAR,
    JOBTITLE,
    SUMMARY,
    PROS,
    CONS,
    (CASE 
    WHEN overall_ratings=="NULL" THEN ABS(AVG(overall_ratings))
    end) as overall_ratingsS
---   FROM employee2

SELECT ceiling(AVG( work_balance_stars)) from EMP4 where  work_balance_stars is not NULL---4
SELECT ceiling(AVG( culture_values_stars)) from EMP4 where  culture_values_stars is not NULL---4
SELECT ceiling(AVG(carrer_opportunities_stars )) from EMP4 where carrer_opportunities_stars  is not NULL---4
SELECT ceiling(AVG(comp_benefit_stars)) from EMP4 where comp_benefit_stars is not NULL---4
SELECT ceiling(AVG(senior_mangemnet_stars)) from EMP4 where senior_mangemnet_stars is not NULL---4



SELECT E_COUNTRY,work_balance_stars  FROM EMP4
--EXPORTING THE CSV FILE AND STORE IT IN HDFS AND NOW IMPORT IT AND DO PARTIONED AND BUCKETING

CREATE TABLE EMP5 AS 
SELECT E_INDEX,
    COMPANY ,
    (CASE 
   WHEN E_LOCATION LIKE "%@%" THEN replace(replace(regexP_extract(E_LOCATION,'@.*',0),")",""),"@","")
    ELSE "USA"
   End) as E_COUNTRY,
    (TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_DATE,
   QUARTER(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) as E_QUARTER,
   YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(REPLACE(REPLACE(REGEXP_EXTRACT(DATES,' (.*)',0),";","")," ","-"),2,12),"MMM-DD-YYYY")))) AS E_YEAR,
    JOBTITLE,
   SUMMARY ,
   PROS,
    CONS,
    overall_ratings as overallratings,
    (case 
    when work_balance_stars is null then '4'
    WHEN work_balance_stars is not null then work_balance_stars end) as workbalancestars ,
    (case 
    when culture_values_stars  is null then "4"
    WHEN culture_values_stars is not null then overall_ratings end) as culturevaluesstars,
    (case 
    when carrer_opportunities_stars  is null then "4"
    WHEN carrer_opportunities_stars is not null then overall_ratings end) as carreropportunitiesstars,
    (case 
    when comp_benefit_stars is null then "4"
    WHEN comp_benefit_stars is not null then comp_benefit_stars end) as compbenefitstar, 
    (case 
    when senior_mangemnet_stars  is null then "4"
    WHEN senior_mangemnet_stars  is not null then senior_mangemnet_stars  end) as seniormangemnetstars 
    FROM employee2
 
 select COUNT(DISTINCT E_YEAR) from EMP5 
  select(case 
    when culture_values_stars  is null then "4"
    WHEN culture_values_stars is not null then overall_ratings end) as culturevaluesstars from employee2 limit 5
    

CREATE EXTERNAL TABLE IF NOT EXISTS COMEMPLOYEE_FINAL(
    E_INDEX INT,    
    E_COMPANY STRING,
    E_COUNTRY STRING,
    E_DATE STRING,
    E_QUARTER STRING,
    E_YEAR STRING,
    JOBTITLE STRING,
    SUMMARY STRING,
    PROS STRING,
    CONS STRING,
    overall_ratings INT,
    work_balance_stars INT,
    culture_values_stars INT,
    carrer_opportunities_stars INT,
    comp_benefit_stars INT,
    senior_mangemnet_stars INT)
--PARTITIONED BY (E_COUNTRY STRING)
--CLUSTERED by (E_YEAR) INTO 10 buckets
row format delimited fields terminated by ","
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/anabig114215/query-hive-73383.csv' INTO TABLE COMEMPLOYEE_FINAL;


create table COMPANY_EMPLOYEE1(
    E_INDEX INT,    
    E_COMPANY STRING,
    E_DATE STRING,
    E_QUARTER STRING,
    E_YEAR STRING,
    JOBTITLE STRING,
    SUMMARY STRING,
    PROS STRING,
    CONS STRING,
    overall_ratings INT,
    work_balance_stars INT,
    culture_values_stars INT,
    carrer_opportunities_stars INT,
    comp_benefit_stars INT,
    senior_mangemnet_stars INT)
--PARTITIONED BY (E_COUNTRY STRING)
CLUSTERED by (E_YEAR) INTO 10 buckets
row format delimited fields terminated by ","
stored as textfile;
set hive.exec.dynamic.partition.mode=nonstrict
insert overwrite table COMPANY_EMPLOYEE1 SELECT * FROM COMEMPLOYEE1

create table COMPANY_EMPLOYEE_FINAL1(
    E_INDEX INT,    
    E_COMPANY STRING,
    E_DATE STRING,
    E_QUARTER STRING,
    E_YEAR STRING,
    JOBTITLE STRING,
    SUMMARY STRING,
    PROS STRING,
    CONS STRING,
    overall_ratings INT,
    work_balance_stars INT,
    culture_values_stars INT,
    carrer_opportunities_stars INT,
    comp_benefit_stars INT,
    senior_mangemnet_stars INT)
PARTITIONED BY (E_COUNTRY STRING)
CLUSTERED by (E_YEAR) INTO 10 buckets
row format delimited fields terminated by ","
stored as textfile;

insert overwrite table COMPANY_EMPLOYEE_FINAL1 partition(E_COUNTRY) SELECT * FROM COMEMPLOYEE_FINAL

select * from COMEMPLOYEE_FINAL

--------------------------------
--Q1 Globally by company identify trends

select E_COMPANY,AVG(overall_ratings) AS ratings
FROM COMEMPLOYEE_FINAL
GROUP BY E_COMPANY
ORDER BY ratings DESC

--Q2. Globally by company per year
select E_COMPANY,E_year,AVG(overall_ratings) AS ratings
FROM COMEMPLOYEE_FINAL
GROUP BY E_COMPANY,E_YEAR
ORDER BY ratings DESC

--Q3.By company by country (Identify trends for each company by country
select E_COUNTRY,COUNT(E_COUNTRY) AS NO_OF_EMPLOYEES 
FROM COMEMPLOYEE_FINAL
GROUP BY E_COUNTRY
ORDER BY NO_OF_EMPLOYEES DESC

--Q4.Display the impact of employee status on rating a company using the overall-ratings field by the company by year.
select (case when jobtitle like "former%" then "Former" else "current" end) as title,
e_company,e_year
round(avg(overall_ratings),2) as rating
FROM COMEMPLOYEE_FINAL
GROUP BY title,e_company,e_year

--q5.Display the impact of job role on rating a company using the overall-ratings field by the company by year.
select replace(regexp_extract(jobtitle,'(. *) (. *)( .*)',0),"e -","") as jobrole,e_company,e_year, avg(overall_ratings)
from comemployee_final 
group by jobtitle,e_company,e_year 

--q6.Display the relationship between the overall rating score vs. the rest of the rating field scores by company. Also, document your findings.
select overall_ratings ,round(avg( work_balance_stars),2) as work_balance_rating,round(avg(culture_values_stars),2) as culture_rating,
round(avg(carrer_opportunities_stars),2) as carreer_oppur_rating,
    round(avg(comp_benefit_stars),2) as comp_benefit_rating,
    round(avg(senior_mangemnet_stars),2) as senior_manage_rating from comemployee_final
    group by overall_ratings
-----carrer rating, benefit rating and culture rating are highly correlated with overall rating as compared to others
--Q7 see graphs of q6

--Q8.Document your findings for the following:
--a) Which corporation is worth working for
create table corp as select e_company, round(avg(overall_ratings),2) as rating
from comemployee_final 
group by e_company
select e_company,rating from corp 
where rating > 4
--facebook and google

--b) Classification of satisfied or unsatisfied employee
create table tap1 as select e_company,jobtitle, case when overall_ratings >4 then "satisfied" else "unsatisfied" end as comment
from comemployee_final 
select (select count(jobtitle) from tap1 where comment = "satisfied") as satisfied_emp,
    ( select count(jobtitle) from tap1 where comment = "unsatisfied") as unsatisfied_emp
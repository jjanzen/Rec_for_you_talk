-- Collaborative Filtering

-- step 1: create table from web form
CREATE EXTERNAL TABLE IF NOT EXISTS ml_demo ( 
user string, 
activity string,
units int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://xxxxxxxxxxxx/';

-- step 2: create exact table, again from web form
CREATE EXTERNAL TABLE IF NOT EXISTS ml_demo_two ( 
user string, 
activity string,
units int) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://xxxxxxxxxxxx/';

-- step 3: co-occur all activities with another activity for a give user
CREATE TABLE act_cooccur row format delimited fields terminated by ',' AS
SELECT a.activity as driver_activity, b.activity as also_activity,
count(distinct a.user) as act_count
from (
select user, activity
from ml_demo) a 
join (
select user, activity
from ml_demo_two) b 
on a.user = b.user
where a.activity != b.activity
group by a.activity, b.activity
having act_count >=0;

-- step 4: add rank to activities in relation to other activities and limit result to 10 
CREATE TABLE act_results row format delimited fields terminated by ',' AS
SELECT a.driver_activity, a.also_activity, a.act_count, a.rank
from (select driver_activity, also_activity, act_count, 
row_number() over(partition by driver_activity order by act_count desc) as rank
FROM act_cooccur) a 
WHERE a.rank <= 10; 

-- step 5: create output table on S3
CREATE EXTERNAL TABLE tmp_results (driver_activity string, also_activity string, act_count string, rank string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://xxxxxxxxxxxx';

-- step 6: load output into S3 table
INSERT INTO TABLE tmp_results select * from act_results; 

-- Naive Bayers
-- use steps 1-3 from above

-- step 1: get count of users for each activity
CREATE TABLE act_n_user row format delimited fields terminated by ',' AS
SELECT activity, count(user) as count_user
FROM ml_demo
GROUP BY activity; 

-- step 2: get count of activities 
CREATE TABLE prob_by_activity row format delimited fields terminated by ',' AS
SELECT activity, sum(units) as count_activity
FROM ml_demo
GROUP BY activity; 

-- step 3:  create probabilites to activities occur together, also_activity = predictor 
CREATE TABLE act_prob row format delimited fields terminated by ',' AS
SELECT a.driver_activity, a.also_activity, a.act_count as both
,round(e.count_activity/c.count_n_user,2) as class_prior_p
,round(d.count_activity/c.count_n_user,2) as predict_prior_p
,round(a.act_count/b.count_user,2) as p_both
,round(((((d.count_activity/c.count_n_user) * (a.act_count/b.count_user)) / (e.count_activity/c.count_n_user))),2) as p_adjust
FROM act_cooccur a 
LEFT JOIN act_n_user b 
on a.also_activity = b.activity
LEFT JOIN prob_by_activity d 
on a.also_activity = d.activity
LEFT JOIN prob_by_activity e
on a.driver_activity = e.activity
CROSS JOIN (
SELECT COUNT(DISTINCT user) as count_n_user
FROM ml_demo) c 
GROUP BY a.driver_activity, a.also_activity, a.act_count, b.count_user, c.count_n_user, e.count_activity, d.count_activity; 

-- step 4: add rank to activities in relation to other activities and limit result to 10 
CREATE TABLE act_results_nb row format delimited fields terminated by ',' AS
SELECT a.driver_activity, a.also_activity, a.p_adjust, a.rank
from (select driver_activity, also_activity, p_adjust, 
row_number() over(partition by driver_activity order by p_adjust desc) as rank
FROM act_prob) a 
WHERE a.rank <= 10; 

-- step 5: create output table on S3
CREATE EXTERNAL TABLE tmp_results_nb (driver_activity string, also_activity string, p_adjust string, rank string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 'xxxxxxxxxxxx';

-- step 6: load output into S3 table
INSERT INTO TABLE tmp_results_nb select * from act_results_nb;


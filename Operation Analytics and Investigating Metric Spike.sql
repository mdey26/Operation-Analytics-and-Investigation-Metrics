CREATE DATABASE PROJECT_3;

USE PROJECT_3;


#TABLE JOB DATA 

create table job_data
(ds DATE, job_id INT NOT NULL, actor_id INT NOT NULL, event VARCHAR(50) NOT NULL, language VARCHAR(50) NOT NULL, time_spent INT NOT NULL,org CHAR(20));

INSERT INTO job_data(ds, job_id, actor_id, event, language, time_spent, org)
VALUES('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'), 
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'), 
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'), 
('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'), 
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'), 
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'), 
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'), 
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C'); 

select * from job_data

-- 1 JOBS REVIEWED OVER TIME:

SELECT ds AS Dates, ROUND((count(job_id)/SUM(time_spent))*3600) AS "Jobs Reviewed per hour per day"
FROM job_data
WHERE ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds;

-- 2 THOUGHPUT ANALYSIS

SELECT round(count(event)/sum(time_spent), 4) AS "Weekly Throughput"   -- 7 days throughput
FROM job_data;

SELECT ds AS Dates, round(count(event)/sum(time_spent), 4) AS "Daily Throughput"   -- Daily Throughput 
FROM job_data
GROUP BY ds
ORDER BY ds;

-- 3 LANGUAGE SHARE ANALYSIS

SELECT language AS Languages, round(100*count(*)/total, 3) AS Percentage_Share, sub.total
FROM job_data
CROSS JOIN (select count(*) as total from job_data) AS sub
GROUP BY language, sub.total;

-- 4 DUPLICATE ROWS SELECTION

SELECT actor_id, count(*) AS Duplicates
FROM job_data
GROUP BY actor_id
HAVING count(*)>1;


#CASE STUDY 2: INVESTIGATING METRIC SPIKE

-- TABLE-1 users_

create table users_
( user_id int, created_at varchar(80), company_id int, language varchar(80), activated_at varchar(80), state varchar(80));

show variables like 'secure_file_priv';

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-1 users_.csv'
into table users_ 
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

alter table users_ add column temp_created_at datetime;
UPDATE users_ SET temp_created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');
ALTER TABLE users_ DROP COLUMN created_at;
ALTER TABLE users_ CHANGE COLUMN temp_created_at created_at DATETIME;

select * from users_;

-- TABLE-2 event

create table event_
(user_id int NULL, occurred_at varchar(80), event_type varchar(80), event_name varchar(50), location varchar(80), device varchar(80), user_type int NULL);

show variables like 'secure_file_priv'; 

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-2 events.csv'
into table event_
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from event_;

alter table event_ add column temp_occurred_at datetime;
UPDATE event_ SET temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');
ALTER TABLE event_ DROP COLUMN occurred_at;
ALTER TABLE event_ CHANGE COLUMN temp_occurred_at occurred_at DATETIME;


-- TABLE-3 email_events

create table email_events
(user_id int NULL, occurred_at varchar(80), action varchar(80), user_type int NULL); 

show variables like 'secure_file_priv';       

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-3 email_events.csv'
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from email_events

ALTER TABLE email_events add column temp_occurred_at datetime;
UPDATE email_events SET temp_occurred_at = str_to_date(occurred_at, '%d-%m-%Y %H:%i');
ALTER TABLE email_events DROP COLUMN occurred_at;
ALTER TABLE email_events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;


-- 1. WEEKLY USER ENGAGEMENT

SELECT extract(week from occurred_at) AS Week_number, count(distinct user_id) AS active_user
FROM event_
WHERE event_type= 'engagement'
GROUP BY week_number
ORDER BY week_number


-- 2. USER GROWTH ANALYSIS

SELECT Months, Users, ROUND(((Users / LAG(Users, 1) OVER(ORDER BY Months) - 1) * 100), 4) AS 'Growth %'
FROM (SELECT EXTRACT(MONTH FROM created_at) AS Months, COUNT(created_at) AS Users
FROM users_
WHERE created_at IS NOT NULL
GROUP BY 1
ORDER BY 1) sub;


-- 3. WEEKLY RETENTION ANALYSIS

WITH cte1 AS 
(SELECT DISTINCT user_id, EXTRACT(week FROM occurred_at) AS SignUp_Week
FROM event_
WHERE event_type = 'signup_flow' AND event_name = 'complete_signup' 
AND EXTRACT(week FROM occurred_at) = 18),
cte2 AS 
(SELECT DISTINCT user_id, EXTRACT(week FROM occurred_at) AS Engagement_Week
FROM event_
WHERE event_type = 'engagement')
SELECT COUNT(sub.user_id) AS Total_engaged_users,
SUM(CASE WHEN sub.Retention_Week > 8 THEN 1 ELSE 0 END) AS Retained_Users
FROM 
(SELECT a.user_id, a.SignUp_Week, 
b.Engagement_Week, 
b.Engagement_Week - a.SignUp_Week AS Retention_Week
FROM cte1 a
LEFT JOIN cte2 b ON a.user_id = b.user_id) sub
ORDER BY sub.user_id;


-- 4 WEEKLY ENGAGEMENT PER DEVICE
WITH cte AS 
(SELECT EXTRACT(YEAR FROM occurred_at) || '-' || EXTRACT(WEEK FROM occurred_at) AS Week_number, device, 
COUNT(DISTINCT user_id) AS user_count FROM event_ WHERE event_type = 'engagement' GROUP BY Week_number, device)
SELECT Week_number, device, user_count FROM cte
ORDER BY Week_number;



-- 5 EMAIL ENGAGEMENT ANALYSIS 

SELECT 100 * SUM(CASE WHEN EMAIL_CATEGORY = 'email_open' then 1 else 0 END)/
SUM(CASE WHEN EMAIL_CATEGORY = 'email_sent' then 1 else 0 END) AS Email_Open_Rate,
100 * SUM(CASE WHEN EMAIL_CATEGORY = 'email_clicked' then 1 else 0 END)/
SUM(CASE WHEN EMAIL_CATEGORY = 'email_sent' then 1 else 0 END) AS Email_Click_Rate
FROM (SELECT CASE WHEN ACTION IN ('sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
WHEN ACTION IN ('email_open') THEN 'email_open'
WHEN ACTION IN ('email_clickthrough') THEN 'email_clicked'
END AS Email_Category 
FROM Email_Events) AS sub;






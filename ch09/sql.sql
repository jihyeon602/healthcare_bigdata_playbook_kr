


create table temp2_10min

 SELECT 
 deviceid,
 UNIX_TIMESTAMP(first_eventTime) DIV 600 AS activity_key,
 first_eventtime, lastStatus
 FROM StatusDetailT_2209
 WHERE deviceId IN (SELECT deviceId FROM deviceInfoT WHERE facility_id = 'daegu_bohun')
 AND ( first_eventTime >= '2022-09-01 00:00:00' AND first_eventTime < '2022-10-01 00:00:00')
 GROUP BY activity_key, lastStatus; 





-- 
 select 'deviceId','activity_key','avg_presence','avg_people_cnt','avg_activity','max_activity','cnt', 
 'first_eventtime','lastStatus'
 

  union all

select A.*, ifnull(B.first_eventtime,'') as first_eventtime, ifnull(B.lastStatus,'') as lastStatus
from temp_10min as A left join temp2_10min as B
on A.deviceId = B.deviceid and  A.activity_key = B.activity_key
order by deviceId, activity_key


into outfile 'C:/works/healthcare_bigdata/stackPerDevice.csv'
character set utf8
fields terminated by ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'





-- fall 집계
select 'deviceId','mon','label','cnt'
union all
(
SELECT 
deviceId, 
DATE_FORMAT( first_eventTime, '%y/%m') as date1,
	case 
		when (lastStatus = 'fall_detected') then 'caution' 
		when (lastStatus = 'fall_exit') then 'caution' 
		when (lastStatus = 'fall_confirmed') then 'caution' 
		when (lastStatus = 'calling') then 'fall' 
		when (lastStatus = 'finished') then 'fall' 
	else 'unknown'
	END as label,	
count(*) as cnt

 FROM StatusDetailT_2209
 WHERE 
  ( first_eventTime >= '2022-07-01 00:00:00' AND first_eventTime < '2022-10-01 00:00:00')
 GROUP BY deviceId, date1, label
)

into outfile 'C:/works/healthcare_bigdata/ch10/fall_summary_all.csv'
character set utf8
fields terminated by ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'



-- select 'activity_key','avg_presence','avg_people_cnt','avg_activity','max_activity','cnt',  'first_eventtime','lastStatus'
-- -- 열 이름 정렬이 잘 안 된다.
-- union all
-- (
select A.*, ifnull(B.first_eventtime,'') as first_eventtime,
ifnull(B.lastStatus, '') as lastStatus
from mda_temp_10min as A left join mda_temp2_10min as B
on A.activity_key = B.activity_key
-- )

order by activity_key

into outfile 'C:/works/healthcare_bigdata/MDA_act_10min2.csv'
character set utf8
fields terminated by ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\n'






-- create table temp_10min


select
deviceId,
activity_key, 
sum(tbl.avg_presence) as avg_presence,
sum(tbl.avg_people_cnt) as avg_people_cnt,
sum(tbl.avg_activity) as avg_activity,
sum(tbl.max_activity) as max_activity,
sum(tbl.cnt) as cnt


FROM (
SELECT 
	deviceId,
   presenceTime as activity_key,
   avg(presenceRatio) as avg_presence,
	avg(people_cnt) as avg_people_cnt,
	0 as avg_activity,
	0 as max_activity,
	0 as cnt

 FROM TpresenceRatio10min
where deviceId = 'id_MzQ6QUI6OTU6NzE6QTQ6Mjg'
 AND presenceTime >= UNIX_TIMESTAMP('2022-09-01 00:00:00') DIV 600 
 AND presenceTime < UNIX_TIMESTAMP('2022-10-01 00:00:00') DIV 600
 GROUP BY deviceId, activity_key

  union all

 SELECT
	deviceId,
 UNIX_TIMESTAMP(eventtime) DIV 600 AS activity_key,
 0 as avg_presence,
 0 as avg_people_cnt,
 AVG(activity) AS avg_activity,
 MAX(activity) AS max_activity,
 COUNT(activity) as cnt

 FROM activities_2209 use index (deviceId_eventTime)
where deviceId = 'id_MzQ6QUI6OTU6NzE6QTQ6Mjg'
     AND (eventTime >= '2022-09-01 00:00:00' AND eventTime < '2022-10-01 00:00:00')
 GROUP BY deviceId, activity_key
 
) as tbl
group by deviceId, activity_key




create table mda_temp_10min


select
deviceId,
activity_key, 
sum(tbl.avg_presence) as avg_presence,
sum(tbl.avg_people_cnt) as avg_people_cnt,
sum(tbl.avg_activity) as avg_activity,
sum(tbl.max_activity) as max_activity,
sum(tbl.cnt) as cnt


FROM (
SELECT 
	deviceId,
   presenceTime as activity_key,
   avg(presenceRatio) as avg_presence,
	avg(people_cnt) as avg_people_cnt,
	0 as avg_activity,
	0 as max_activity,
	0 as cnt

 FROM TpresenceRatio10min
 WHERE deviceId = 'id_MzQ6QUI6OTU6NzE6QTE6MDA'
-- where deviceId IN (SELECT deviceId FROM deviceInfoT WHERE facility_id = 'daegu_bohun')
 AND presenceTime >= UNIX_TIMESTAMP('2022-09-01 00:00:00') DIV 600 
 AND presenceTime < UNIX_TIMESTAMP('2022-10-01 00:00:00') DIV 600
 GROUP BY activity_key

  union all

 SELECT
	deviceId,
 UNIX_TIMESTAMP(eventtime) DIV 600 AS activity_key,
 0 as avg_presence,
 0 as avg_people_cnt,
 AVG(activity) AS avg_activity,
 MAX(activity) AS max_activity,
 COUNT(activity) as cnt

 FROM activities_2209 use index (deviceId_eventTime)
 WHERE deviceId = 'id_MzQ6QUI6OTU6NzE6QTE6MDA'
-- where deviceId IN (SELECT deviceId FROM deviceInfoT WHERE facility_id = 'daegu_bohun')
     AND (eventTime >= '2022-09-01 00:00:00' AND eventTime < '2022-10-01 00:00:00')
 GROUP BY activity_key
 
) as tbl
group by activity_key
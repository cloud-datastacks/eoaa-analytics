select year(received_date) as year, count(*) as number_of_applications
from eoaa_db.eoaa_data.building_application_status
group by year(received_date)
;

select count(*)
from eoaa_db.eoaa_data.building_application_status
;

select s.application_type,
       t.description_gr,
       s.application_description,
       s.status,
       s.sub_status,
       s.received_date,
       s.completion_date,
       s.completion_date - s.received_date as duration_in_days
from eoaa_db.eoaa_data.building_application_status s
inner join eoaa_db.eoaa_data.application_types t
    on s.application_type = t.type
;

select distinct status, sub_status
from eoaa_db.eoaa_data.building_application_status
;

select s.application_type,
       s.status,
       s.sub_status,
       count(*) as number_of_applications,
       round(avg(s.completion_date - s.received_date), 1) as avg_duration_in_days,
       round(min(s.completion_date - s.received_date), 1) as min_duration_in_days,
       round(max(s.completion_date - s.received_date), 1) as max_duration_in_days
from eoaa_db.eoaa_data.building_application_status s
where s.completion_date is not null
group by s.application_type, s.status, s.sub_status
order by 1, 2, 3;


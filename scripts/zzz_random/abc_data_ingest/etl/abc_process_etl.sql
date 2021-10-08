/*
drop table if exists #stage;
drop table #run_params;
drop table #change_records;
select 1 as ingest_file_id into #run_params;
*/
alter table #run_params	
	add rows_processed int 
	, error_records int
	, new_records int
	, change_records int
	, fof_records int
;
update #run_params
	set rows_processed = 0
		, error_records = 0
		, new_records = 0
		, change_records = 0
		, fof_records = 0
;
select distinct
	cast(trim(field_0) as varchar(100))		as member_id		--varchar(100)
	, cast(trim(field_1) as varchar(100))	as first_name			--varchar(100)
	, cast(trim(field_2) as varchar(100))	as middle_name			--varchar(100)
	, cast(trim(field_3) as varchar(100))	as last_name			--varchar(100)		
	, try_convert(date,field_4)				as date_of_birth	--date
	, cast(trim(field_5) as varchar(100))	as sex					--varchar(100)
	, cast(trim(field_6) as varchar(100))	as favorite_color		--varchar(100)
	, cast(trim(field_7) as varchar(100))	as attributed_q1		--varchar(100)
	, cast(trim(field_8) as varchar(100))	as attributed_q2		--varchar(100)
	, try_convert(decimal(20,10),field_9)	as risk_q1			--decimal(20,10)
	, try_convert(decimal(20,10),field_10)	as risk_q2			--decimal(20,10)
	, cast(trim(field_11) as varchar(100))	as risk_increased_flag	--varchar(100)
	, cast(checksum(field_0,field_1,field_2,field_3,field_4,field_5,field_6,field_7,field_8,field_9,field_10,field_11) as varchar(100)) as rec_hash
	, record_ingest_id
	, s.ingest_file_date
	, s.ingest_file_id
into #stage
from abc_stage.dbo.abc_ingest_raw r
	inner join abc_stage.dbo.abc_ingest_status s on r.ingest_file_id = s.ingest_file_id
where r.ingest_file_id = (select ingest_file_id from #run_params)
;
update #run_params
	set rows_processed = @@ROWCOUNT
;
--Error Handling
	--Missing/Invalid DOB
	insert into abc_stage.dbo.abc_process_error_records
		select ingest_file_id 
			, record_ingest_id
			, 'DOB Error' as error
		from #stage
		where date_of_birth is null
	;
	update #run_params
		set error_records = error_records+@@ROWCOUNT
	;
	delete from #stage 
	where date_of_birth is null;
	insert into abc_stage.dbo.abc_process_error_records
		select ingest_file_id 
			, record_ingest_id
			, 'DOB Out of Range' as error
		from #stage
		where date_of_birth > getdate()
			or year(date_of_birth) < 1850
	;
	update #run_params
		set error_records = error_records+@@ROWCOUNT
	;
	delete from #stage 
	where date_of_birth > getdate()
		or year(date_of_birth) < 1850
	;
	--Missing Risk Score
	insert into abc_stage.dbo.abc_process_error_records
		select ingest_file_id 
			, record_ingest_id
			, 'Missing Risk Scores' as error
		from #stage
		where risk_q1 is null
			or risk_q2 is null
	;
	update #run_params
		set error_records = error_records+@@ROWCOUNT
	;
	delete from #stage 
	where risk_q1 is null
		or risk_q2 is null
	;
--process records
--new records
with new_recs as
	(
	select distinct
		member_id
		, first_name
		, middle_name
		, last_name
		, date_of_birth
		, sex
		, favorite_color
		, attributed_q1
		, attributed_q2
		, risk_q1
		, risk_q2
		, risk_increased_flag
		, rec_hash
		, ingest_file_id
		, record_ingest_id
		, ingest_file_date as eff_date
		, cast('31Dec2999' as date) as end_date
	from #stage
	where member_id not in (select member_id from abc_prod.dbo.abc_family_medicine where end_date > getdate())
	)
insert into abc_prod.dbo.abc_family_medicine
	select *
	from new_recs
;
update #run_params
	set new_records = new_records+@@ROWCOUNT
;
--change records
select distinct
	member_id
	, first_name
	, middle_name
	, last_name
	, date_of_birth
	, sex
	, favorite_color
	, attributed_q1
	, attributed_q2
	, risk_q1
	, risk_q2
	, risk_increased_flag
	, rec_hash
	, ingest_file_id
	, record_ingest_id
	, ingest_file_date as eff_date
	, cast('31Dec2999' as date) as end_date
into #change_records
from #stage
where rec_hash not in (select rec_hash from abc_prod.dbo.abc_family_medicine where end_date > getdate())
;
update abc_prod.dbo.abc_family_medicine
	set end_date = cr.eff_date
	from abc_prod.dbo.abc_family_medicine m
		inner join (select member_id, eff_date from #change_records) cr on m.member_id = cr.member_id
	where m.end_date > getdate()
;
insert into abc_prod.dbo.abc_family_medicine
	select *
	from #change_records
;
update #run_params
	set change_records = change_records+@@ROWCOUNT
;
--fof records
with fof_records as
	(
	select rec_hash
	from abc_prod.dbo.abc_family_medicine b
	where member_id not in (select member_id from #stage)
		and end_date > getdate()
	)
	, rec_date as
	(
	select ingest_file_date
	from abc_stage.dbo.abc_ingest_status
	where ingest_file_id = (select ingest_file_id from #run_params)
	)
update abc_prod.dbo.abc_family_medicine
	set end_date = cr.end_date
	from abc_prod.dbo.abc_family_medicine m
		inner join 
			(
			select rec_hash, rd.ingest_file_date as end_date
			from fof_records 
				cross join rec_date rd
			) cr on m.rec_hash = cr.rec_hash
	where m.end_date > getdate()
;	
update #run_params
	set fof_records = fof_records+@@ROWCOUNT
;
update abc_stage.dbo.abc_ingest_status	
	set rows_processed = r.rows_processed
		, new_records = r.new_records
		, change_records = r.change_records
		, fof_records = r.fof_records
		, error_records = r.error_records
	from #run_params r
		inner join abc_stage.dbo.abc_ingest_status s on r.ingest_file_id = s.ingest_file_id
;

--select * from abc_stage.dbo.abc_ingest_status	

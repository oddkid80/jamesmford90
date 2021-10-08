insert into abc_stage.dbo.abc_ingest_meta 
	(
	ingest_name
	, file_prefix	
	, file_suffix
	, file_delimiter
	, file_header
	, file_frequency
	, file_script
	, file_next_check_datetime
	, file_last_run_datetime
	, created_date
	, updated_date
	, created_by
	, updated_by
	, isactive
	)
	values 
	(
	'ABC Family Medicine Files'
	, 'abc_family_medicine'
	, '.txt'
	, '|'
	, 'Y'
	, 'monthly-15'
	, '\etl\abc_process_etl.sql'
	, cast(getdate() as date)
	, cast(null as datetime)
	, getdate()
	, getdate()
	, 'ford.james'
	, 'ford.james'
	, 1
	)
;
select * from abc_stage.dbo.abc_ingest_meta
--truncate table abc_stage.dbo.abc_ingest_meta ;
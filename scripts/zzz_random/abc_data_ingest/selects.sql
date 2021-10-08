begin tran;
truncate table abc_stage.dbo.abc_ingest_raw;
truncate table abc_stage.dbo.abc_ingest_status;
truncate table abc_stage.dbo.abc_process_error_records;
truncate table abc_prod.dbo.abc_family_medicine;
truncate table abc_stage.dbo.abc_sql_executor;
truncate table abc_stage.dbo.abc_logging;
update abc_stage.dbo.abc_ingest_meta
	set file_last_run_datetime = null
		, file_next_check_datetime = cast(getdate() as date)
;
commit tran;


select * from abc_stage.dbo.abc_ingest_meta

select * from abc_stage.dbo.abc_ingest_status

select * from abc_stage.dbo.abc_ingest_raw

select * from abc_stage.dbo.abc_sql_executor

select * from abc_prod.dbo.abc_family_medicine

select * from abc_stage.dbo.abc_process_error_records

select * from abc_stage.dbo.abc_logging
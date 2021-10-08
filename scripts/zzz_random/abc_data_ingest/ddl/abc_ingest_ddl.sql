--ddl
create table abc_stage.dbo.abc_ingest_meta
	(
	ingest_id int identity(1,1)
	, ingest_name varchar(500)
	, file_prefix varchar(500)	
	, file_suffix varchar(500)
	, file_delimiter varchar(500)
	, file_header varchar(500)
	, file_frequency varchar(500)
	, file_next_check_datetime datetime
	, file_last_run_datetime datetime
	, file_script varchar(500)
	, created_date datetime
	, updated_date datetime
	, created_by varchar(500)
	, updated_by varchar(500)
	, isactive int
	)
;
create table abc_stage.dbo.abc_ingest_status
	(
	ingest_id int
	, ingest_file_id int identity(1,1)
	, ingest_file_name varchar(500)
	, ingest_file_date date
	, ingest_file_size_in_bytes int
	, ingest_file_status varchar(500)
	, ingest_file_status_datetime datetime
	, load_start_datetime datetime
	, load_end_datetime datetime
	, raw_rows_inserted int
	, process_status varchar(500)
	, process_start_datetime datetime
	, process_end_datetime datetime
	, rows_processed int
	, new_records int
	, change_records int
	, fof_records int
	, error_records int
	)
;
create table abc_stage.dbo.abc_ingest_raw
	(	
	field_0 varchar(500)
	, field_1 varchar(500)
	, field_2 varchar(500)
	, field_3 varchar(500)
	, field_4 varchar(500)
	, field_5 varchar(500)
	, field_6 varchar(500)
	, field_7 varchar(500)
	, field_8 varchar(500)
	, field_9 varchar(500)
	, field_10 varchar(500)
	, field_11 varchar(500)
	, field_12 varchar(500)
	, field_13 varchar(500)
	, field_14 varchar(500)
	, field_15 varchar(500)
	, field_16 varchar(500)
	, field_17 varchar(500)
	, field_18 varchar(500)
	, field_19 varchar(500)
	, field_20 varchar(500)
	, ingest_id int
	, ingest_file_id int
	, ingest_datetime datetime
	, record_ingest_id int IDENTITY(1,1)
	)
;
create table abc_stage.dbo.abc_sql_executor
	(
	file_script varchar(500)
	, sql_statement_order int
	, sql_statement varchar(max)
	, updated_date datetime
	)
;
create table abc_stage.dbo.abc_process_error_records
	(
	ingest_file_id int
	, record_ingest_id int 	
	, error varchar(max)
	)
;
create table abc_stage.dbo.abc_sql_executor
	(
	file_script varchar(500)
	, sql_statement_order int
	, sql_statement varchar(max)
	, updated_date datetime
	)
;
create table abc_prod.dbo.abc_family_medicine
	(
	record_id int identity(1,1)
	, member_id varchar(100)
	, first_name varchar(100)
	, middle_name varchar(100)
	, last_name varchar(100)
	, date_of_birth date
	, sex varchar(100)
	, favorite_color varchar(100)
	, attributed_q1 varchar(100)
	, attributed_q2 varchar(100)
	, risk_q1 decimal(20,10)
	, risk_q2 decimal(20,10)
	, risk_increased_flag varchar(100)
	, rec_hash varchar(100)
	, ingest_file_id int
	, record_ingest_id int
	, eff_date date
	, end_date date
	)
;
create table abc_stage.dbo.abc_logging
	(
	log_datetime datetime
	, log_category varchar(200)
	, log_message varchar(max)
	)
;
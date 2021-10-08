#potential improvements
    #split commonalities into functions for testing/deployment
    #build full rollback process in case of error - delete ingest, processed records, etc.
    #notification if there are no files on a given date/stop the process
    #notficiation if there are errors (i.e., send error log records)


import os, shutil, pyodbc, datetime, logging, traceback, re

#defining sql server connection information
conn_info = 'Driver={SQL Server};Server=DESKTOP-JN17IQ1\\SQLEXPRESS;Trusted_Connection=yes;'

#defining log message
def log_message(log_id,process_name,message,**log_args):
    error_message = log_args.get('error_message','')
    if error_message == '':
        return [f'Log_ID: {log_id}',f'Process_name: {process_name}',f'Message: {message}']
    else:
        return [f'Log_ID: {log_id}',f'Process_name: {process_name}',f'Message: {message}',f'ERROR: {error_message}']

#splits sql queries into individual commits/statements
def query_splitter(file_name):
    query_to_split = open(file_name,'r')
    individual_commits = query_to_split.read()
    individual_commits = re.sub(r"/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/",'',individual_commits)
    individual_commits = re.sub(r'--.+?\n', '\n',individual_commits)
    individual_commits = individual_commits.replace('\n',' ').replace('  ',' ').replace('\t',' ')
    individual_commits = re.split(''';(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''', individual_commits)

    query_list = []
    statement_no = 0
    for commits in individual_commits:
        query_insert = []
        query_insert.insert(0,[statement_no,commits+';'])
        query_list.append(query_insert)
        statement_no += 1   
    return query_list

#executes sql statements for given ingest/file (i.e., etl file)
def query_executor(connection_info,log_id,ingest_id,ingest_file_id,file_script):
    with pyodbc.connect(connection_info) as con:
        cursor = con.cursor()
        cursor.execute(f"select * from abc_stage.dbo.abc_sql_executor where file_script = '{file_script}'")
        queries_to_execute = list(cursor)

        file_info = f'[Ingest_id: {ingest_id}, Ingest_file_id: {ingest_file_id}, SQL_Script: {file_script}]'
        logging.info(log_message(log_id,'ABC Ingest','Beginning SQL ETL Process. '+str(file_info)))

        cursor.execute(f'select {ingest_file_id} as ingest_file_id into #run_params;')
        cursor.execute('begin tran process;')
        for statement in queries_to_execute:
            error = 0
            try:
                cursor.execute(statement[2])   
            except:
                report = traceback.format_exc()
                logging.error(log_message(log_id,'ABC Ingest',f'SQL ETL Process Failed. Failed on SQL Statement {statement[1]} '+str(file_info),error_message=str(report)))
                cursor.execute("ROLLBACK;")
                error = 1
                return 1                
        if error == 0:
            cursor.execute('commit tran process;')
            logging.info(log_message(log_id,'ABC Ingest',f'SQL ETL Process Successfully Executed. '+str(file_info)))
            return 0

#gets files with a given prefix/suffix (name and file type) from a particular folder
def get_files(input_path,file_prefix,suffix):
    file_list = os.listdir(input_path)
    
    files_to_process = []
    for f in file_list:
        if f[:len(file_prefix)] == file_prefix:
            if f.endswith(suffix):
                date_suffix = f.split('.')[0][-len('MMDDYYYY'):]
                file_info = [
                        input_path+'\\'+f
                        ,os.stat(input_path+'\\'+f).st_size
                        ,datetime.datetime.strptime(date_suffix,'%m%d%Y')
                        ]
                files_to_process.append(file_info)
    
    files_to_process.sort(key=lambda x: x[2])

    return files_to_process

#loads raw files, creating stage table (with # of fields in the given file, based on a particular delimiter and first row)
def load_raw_files(connection_info,log_id,ingest_id,ingest_file,ingest_file_id,delimiter,first_row):
    with open(ingest_file,'r') as qr:
        cols = str(qr.readline()).count(delimiter)+1
    for i in range(cols):
        if i == 0:
            load_table_sql_statement = "create table #bulk (field_0 varchar(500)"
            insert_table_sql_statement = "insert into abc_stage.dbo.abc_ingest_raw (field_0"
            select_table_sql_statement = "select field_0"
        else:
            load_table_sql_statement = load_table_sql_statement + ', field_'+str(i)+' varchar(500)'
            insert_table_sql_statement = insert_table_sql_statement + ', field_'+str(i)
            select_table_sql_statement = select_table_sql_statement + ', field_'+str(i)
            if i == cols-1:
                load_table_sql_statement = load_table_sql_statement + ');'
                insert_table_sql_statement = insert_table_sql_statement + ', ingest_id, ingest_file_id, ingest_datetime) '
                select_table_sql_statement = select_table_sql_statement + f", {ingest_id}, '{ingest_file_id}', getdate() from #bulk;"

                insert_table_sql_statement = insert_table_sql_statement + select_table_sql_statement

    with pyodbc.connect(connection_info) as con:
        cursor = con.cursor()
        file_info = f'[Ingest_id: {ingest_id}, Ingest_file_id: {ingest_file_id}, Ingest_file: {ingest_file}]'
        logging.info(log_message(log_id,'ABC Ingest','Beginning load. '+str(file_info)))
        try:
            cursor.execute('begin tran ingest;')            

            cursor.execute(load_table_sql_statement)        
            cursor.execute(f"bulk insert #bulk from '{ingest_file}' with (fieldterminator = '{delimiter}',rowterminator='\n',firstrow = {first_row});")
            cursor.execute(insert_table_sql_statement)
    
            cursor.execute('commit tran ingest;')
            logging.info(log_message(log_id,'ABC Ingest','File Loaded. '+str(file_info)))
            return 0
        except:
            report = traceback.format_exc()
            logging.error(log_message(log_id,'ABC Ingest','File Load Failed. '+str(file_info),error_message=str(report)))
            cursor.execute('ROLLBACK;')            
            return 1

#loads log files into abc_stage.dbo.abc_logging
def load_logs(connection_info,directory):
    files = os.listdir(os.getcwd())
    for i in files:
        with pyodbc.connect(connection_info) as con:
            cursor = con.cursor()
            if i.endswith('.log'):
                log_to_import = os.getcwd()+'\\'+i
                cursor.execute(f"bulk insert abc_stage.dbo.abc_logging from '{log_to_import}' with (fieldterminator = '|',rowterminator='\n',firstrow = 1);")   
                os.remove(log_to_import) 


if __name__ == '__main__': 
    logfile = os.getcwd()+'\\abc_ingest_log_'+str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))+'.log'
    logging.basicConfig(filename=logfile,level=logging.DEBUG,
        format='%(asctime)s|%(levelname)s|L%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    log_id = str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    #fetching processes to execute
    with pyodbc.connect(conn_info) as con:
        cursor = con.cursor()
        cursor.execute('select * from abc_stage.dbo.abc_ingest_meta where isactive = 1 and (file_next_check_datetime < getdate() or file_last_run_datetime is null)')
        ingest_to_check = list(cursor)
    
    logging.info(log_message(log_id,'ABC Ingest','Process Starting.'))

    #executing through processes to check
    for process in ingest_to_check:

        error = 0

        #uploading/updating sql script for process                   
        query = query_splitter(os.getcwd()+process[9])
        query_bulk_upload = []
        for line in query:
            line_to_insert = [
                process[9]
                ,line[0][0]
                ,line[0][1]
                ,datetime.datetime.now()
                ]
            query_bulk_upload.append(line_to_insert)
        with pyodbc.connect(conn_info) as con:
            cursor = con.cursor()
            cursor.execute(f"delete from abc_stage.dbo.abc_sql_executor where file_script = '{process[9]}'")
            cursor.executemany("""
                insert into abc_stage.dbo.abc_sql_executor
                    (
                    file_script
                    , sql_statement_order
                    , sql_statement
                    , updated_date   
                    )
                values (?,?,?,?)""",query_bulk_upload)
        
        #determining starting row of files for current process
        if process[5]=='Y':
            first_row = 2
        else:
            first_row = 1        
        #determining delimiter
        delimiter = process[4]

        #gathering files to be loaded
        files = get_files(os.getcwd()+'\\input',process[2],process[3])  
 
        #looping through files in input folder
        for f in files:
            #creating initial record for file
            with pyodbc.connect(conn_info) as con:
                cursor = con.cursor()
                cursor.execute(f"""insert into abc_stage.dbo.abc_ingest_status 
                                    (ingest_id
                                    , ingest_file_name
                                    , ingest_file_size_in_bytes
                                    , ingest_file_date
                                    , ingest_file_status
                                    , ingest_file_status_datetime) values ({process[0]},'{f[0]}',{f[1]}, cast('{f[2]}' as datetime),'Initializing',getdate());                                    
                                    """)
                cursor.execute('select cast(scope_identity() as int);')
                ingest_file_id = list(cursor)[0][0]

            #executes load_raw_files function, if error will return a 1 and break the process
            with pyodbc.connect(conn_info) as con:
                cursor = con.cursor()
                cursor.execute(f"update abc_stage.dbo.abc_ingest_status set load_start_datetime = getdate(), ingest_file_status = 'Loading' where ingest_file_id = {ingest_file_id};")            
            
            error = load_raw_files(conn_info,log_id,process[0],f[0],ingest_file_id,delimiter,first_row)
            
            with pyodbc.connect(conn_info) as con:
                cursor = con.cursor()
                if error == 1:
                    cursor.execute(f"update abc_stage.dbo.abc_ingest_status set ingest_file_status = 'Failed' where ingest_file_id = {ingest_file_id};")
                else:
                    cursor.execute(f"""
                        update abc_stage.dbo.abc_ingest_status set load_end_datetime = getdate(), ingest_file_status = 'Completed', raw_rows_inserted = cj.ct from abc_stage.dbo.abc_ingest_status s 
	                        cross join (select count(*) ct from abc_stage.dbo.abc_ingest_raw where ingest_file_id = {ingest_file_id}) cj where s.ingest_file_id = {ingest_file_id};
                        """)
            if error == 1:              
                break
            
            #executes query executor for particular ingest_id, if error will return a 1 and break the process
            with pyodbc.connect(conn_info) as con:
                cursor = con.cursor()
                cursor.execute(f"update abc_stage.dbo.abc_ingest_status set process_start_datetime = getdate(), process_status = 'Processing' where ingest_file_id = {ingest_file_id};")
            
            error = query_executor(conn_info,log_id,process[0],ingest_file_id,process[9])
            with pyodbc.connect(conn_info) as con:
                cursor = con.cursor()
                if error == 1:
                    cursor.execute(f"update abc_stage.dbo.abc_ingest_status set process_status = 'Failed' where ingest_file_id = {ingest_file_id};")
                else:
                    cursor.execute(f"update abc_stage.dbo.abc_ingest_status set process_end_datetime = getdate(), process_status = 'Completed' where ingest_file_id = {ingest_file_id};")	

            if error == 1:
                break

            #moving file to processed folder
            shutil.move(f[0],os.getcwd()+'\\processed\\'+os.path.basename(f[0]))
        
        if error == 1:
            break
        #updating next process rundate based on frequency
        with pyodbc.connect(conn_info) as con:
            cursor = con.cursor()
            cursor.execute(f"""
                update abc_stage.dbo.abc_ingest_meta
                    set file_last_run_datetime = getdate()
                        , file_next_check_datetime = 
                            case
                                when file_frequency = 'daily' then dateadd(day,1,cast(getdate() as date))
                                when file_frequency like 'weekly%' then	
                                    dateadd(day,(datepart(dw,getdate())+substring(file_frequency,charindex('-',file_frequency)+1,len(file_frequency)-1))-datepart(dw,getdate()),cast(getdate() as date))
                                when file_frequency like 'monthly%' then 
                                    cast(concat(year(dateadd(month,1,getdate())),'-',month(dateadd(month,1,getdate())),'-',substring(file_frequency,charindex('-',file_frequency)+1,len(file_frequency)-1)) as date)
                            end
                    where ingest_id = {process[0]}
                ;
                """)

    logging.info(log_message(log_id,'ABC Ingest','Process Finished.'))

    logging.shutdown()
    load_logs(conn_info,os.getcwd())
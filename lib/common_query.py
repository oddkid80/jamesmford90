import re

class common_query():

    #legacy query splitter
    """
    def split_query(filename):
        try:
            query_to_split = open(filename,'r')
        except Exception as ex:
            raise ex

        individual_commits = query_to_split.read()
        individual_commits = re.sub(r"/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/",'',individual_commits)
        individual_commits = re.sub(r'--.+?\n', '\n',individual_commits)
        individual_commits = individual_commits.replace('\n',' ').replace('  ',' ').replace('\t',' ')
        individual_commits = re.split(''';(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''', individual_commits)

        query_list = []
        statement_no = 0
        for commits in individual_commits:
            if commits.strip() != '':
                query_insert = []
                query_insert.insert(0,[statement_no,commits+';'])
                query_list.append(query_insert[0])
                statement_no += 1
        return query_list
    """
    #legacy query splitter
    """
    def split_query(filename):
        try:
            query_to_split = open(filename,'r')
        except Exception as ex:
            raise ex

        query_set_with_lines = ''
        for line_no, line in enumerate(query_to_split):
            query_set_with_lines+=f'<line:{line_no+1}>'+line

        individual_commits = query_set_with_lines
        individual_commits = re.sub(r"/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/",'',individual_commits)
        individual_commits = re.sub(r'--.+?\n', '\n',individual_commits)
        individual_commits = individual_commits.replace('\n',' ').replace('  ',' ').replace('\t',' ')
        individual_commits = re.split(''';(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''',individual_commits)

        query_list = []
        statement_no = 0
        for commit in individual_commits:
            #line = re.split(r'(<line:(|))>',commit.strip(),1)
            line = re.split(r'<line:(.*?)>',commit.strip(),1)[1]
            sql_statement = re.sub(r'<line:.*?>','',commit.strip())

            if sql_statement.strip() != '':
                query_list.append([statement_no,sql_statement+';',line])
                statement_no +=1

        return query_list
    """

    def split_query(filename):
        """
        Splits query file into individual commits/transactions - splits on ';' (semicolon)

        Generates a list containing individual lists with each commit/transaction; the list will include
            the statement_no for ordering the sql statement, the sql statement itself, and the original
            line position of the statement in the raw file (for tracking errors)
        """
        try:
            query_to_split = open(filename,'r')
        except Exception as ex:
            raise ex

        query_set_with_lines = ''
        for line_no, line in enumerate(query_to_split):
            query_set_with_lines+=f'<line:{line_no+1}>'+line

        individual_commits = query_set_with_lines
        individual_commits = re.sub(r"/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/",'',individual_commits)
        individual_commits = re.sub(r'--.+?\n', '\n',individual_commits)
        individual_commits = individual_commits.replace('\n',' ').replace('  ',' ').replace('\t',' ')
        individual_commits = re.split(''';(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''',individual_commits)

        query_list = []
        statement_no = 0
        for commit in individual_commits:
            if commit.strip() != '':
                line = re.split(r'<line:(.*?)>',commit.strip(),1)[1]
                sql_statement = re.sub(r'<line:.*?>','',commit.strip())

                if sql_statement.strip() != '':
                    query_list.append([statement_no,sql_statement+';',line])
                    statement_no +=1
        
        return query_list

    def param_query(query,parameters,param_identifier=':'):
        """
        Parameterizes query based on identifier. Useful for SQL languages without inbuilt/dynamic variables.
        
        Pass dictionary with variables to be identified/replaced, will scan for the identifier (default is ':'), then
            will replace with the corresponding value in the dictionary.
            
        Ex:
            query = select * from schema.table where value = ':replace'
            
            dict = {'replace':'new value'}
            
            param_query(query,dict,param_identifier=':')
            
            results in 'select * from schema.table where value = 'new value''
            
        If you need a value between quotes, you must handle that either in the dictionary or the query itself prior to
            paramaterizing the query.
        """

        query_to_modify = query[:]

        for key, param in parameters.items():
            key = str(key)
            value = f"{param}"
            match_str = u"{1}{0}\\b".format(key,param_identifier)
            query_to_modify = re.sub(match_str, lambda _: value, query_to_modify, flags=re.U)

        return query_to_modify

#copied from https://github.com/NachiMK/python/blob/a31c75cbe919ad1ee3540cbe65801d32c2eef011/FinTech/prospects-api/chalicelib/bulk_load/s3_bulk_copy.py
import os
import boto3
import re
import logging
import sys

from chalicelib.env_setter import EnvSetter
from chalicelib.database.postgres_metadata import PostgresMetaDataHelper
from chalicelib.database.data_helper import DataHelper
from chalicelib.bulk_load.csv_file_format import CSVFileFormat
from chalicelib.s3_helper.file_helper import S3FileHelper

EnvSetter.run()
aws_region = os.getenv('AWS_REGION')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
logger = logging.getLogger("s3_bulk_copy")
logger.setLevel(LOG_LEVEL)
logger.addHandler(logging.StreamHandler(sys.stdout))
s3_bulk_logger = logging.getLogger("s3_bulk_copy")

class InvalidParamError(Exception):
    pass

class InvalidTableError(Exception):
    pass

class InvalidCSVOptionError(Exception):
    pass

class UploadCSVError(Exception):
    pass

class S3BulkCopyResult(object):
    def __init__(self):
        self.upload_status = 'Not Started'
        self.row_count = -1
        self.upload_error = None
        self.upload_key = -1
    
    def __str__(self):
        return f'Upload Status: {self.upload_status}, Row Count: {self.row_count}, Upload Error: {str(self.upload_error)}, Key: {self.upload_key}'

class S3BulkCopy(object):
    def __init__(self,*
                   ,s3_bucket=None
                   ,s3_key=None
                   ,sql_table_name=None
                   ,aurora_connection_str=""
                   ,csv_file_format=CSVFileFormat()):
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_table_name = sql_table_name
        self.aurora_connection_str = aurora_connection_str
        self.csv_file_format = csv_file_format

    def load_csv(self, *, column_list='', **kargs):
        copy_result = S3BulkCopyResult()
        self.is_load_csv_param_valid(**kargs)
        s3_bulk_logger.info(f'Param for load csv is valid. Continuing with load')
        # get copy options
        copy_options = self.csv_file_format.postgres_with_options()
        column_list = '' if column_list == None else column_list
        copy_result.upload_status = 'Started'
        # get other params
        # Truncate stage table. If not provided it. Default is to truncate the Table!!!
        truncate_table = False if 'truncate_table' not in kargs else kargs.get("truncate_table")
        # truncate tables
        if truncate_table == True:
            DataHelper.execute_command(f'DELETE FROM public."{self.sql_table_name}";'
                                        ,connection_str = self.aurora_connection_str
                                        ,auto_commit=True)
        # get script to load file.
        sql_cmd_to_load = f'''
                SELECT 
                aws_s3.table_import_from_s3 (
                                            '"{self.sql_table_name}"', 
                                            '{column_list}', 
                                            '{copy_options}', 
                                            aws_commons.create_s3_uri('{self.s3_bucket}', '{self.s3_key}', '{aws_region}')
                                            ) 
        '''
        s3_bulk_logger.info(f'sql_cmd_to_load: {sql_cmd_to_load}')
        # call script
        sql_result = DataHelper.execute_scalar(sql_cmd_to_load, auto_commit=True)
        # prepare results and return
        s3_bulk_logger.info(f'Copy command executed. sql_results: {sql_result}')
        if sql_result.query_status:
            copy_result.row_count = self.get_row_count(sql_result.scalar_value)
            copy_result.upload_status = 'Success'
        else:
            raise UploadCSVError(f'Error uploading CSV File {self.s3_key} to Table {self.sql_table_name}. DB Error:{sql_result.sql_error}')
        return copy_result

    def is_load_csv_param_valid(self, **kargs):
        if (self.s3_bucket is None) or (self.s3_key is None) or (self.sql_table_name is None):
            raise InvalidParamError('S3 Bucket, Key and SQL Table names are required.')
        if (len(self.s3_bucket) == 0 or (len(self.s3_key) == 0) or (len(self.sql_table_name) == 0)):
            raise InvalidParamError('S3 Bucket, Key and SQL Table names are required.')
        # check s3 file exists
        S3FileHelper.check_file_exists(s3_bucket=self.s3_bucket, s3_key=self.s3_key)
        # check if sql table exists
        table_exists = PostgresMetaDataHelper.check_table_exists(self.sql_table_name, connection_str=self.aurora_connection_str)
        if not table_exists:
            raise InvalidTableError(f'Table name provided {self.sql_table_name} does not exists in database')
        # check CSV format options
        if not self.csv_file_format.is_format_valid():
            raise InvalidCSVOptionError(f'CSV File Format Option {self.csv_file_format} is not valid')
        return True

    def get_row_count(self, scalar_value):
        #scalar_value = result_rows[0][0]
        rows_search = re.search('\\d*( rows)', scalar_value, re.IGNORECASE) if scalar_value != None else ''
        search_result = rows_search.group(0) if rows_search else ''
        if rows_search and (len(search_result) > 0) and (search_result.replace(' rows', '').isdigit()):
            return int(search_result.replace(' rows', ''))
        return 0

    def get_bytes(self, scalar_value):
        #scalar_value = result_rows[0][0]
        rows_search = re.search('\\d*( bytes)', scalar_value, re.IGNORECASE) if scalar_value != None else ''
        search_result = rows_search.group(0) if rows_search else ''
        if rows_search and (len(search_result) > 0) and (search_result.replace(' bytes', '').isdigit()):
            return int(search_result.replace(' bytes', ''))
        return 0

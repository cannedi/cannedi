import pandas as pd
import math
import os
import time
import io

import boto3
import paramiko
import getpass
from datetime import datetime, timedelta
import stat

#S3_BUCKET_NAME = 'sureco-dev-snowflake-import'
S3_BUCKET_NAME = 'sureco-dev-data-eng'
S3_FILE_PATH = 'snowflake/import/healthaxis'

FTP_HOST = 'sftp.healthaxis.com'
FTP_PORT = 22
FTP_USERNAME = ''
FTP_PASSWORD = ''
FTP_FILE_PATH = ''

CHUNK_SIZE = 6291456

def open_ftp_connection(ftp_host, ftp_port, ftp_username, ftp_password):
    '''
    Opens ftp connection and returns connection object

    '''
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    try:
        transport = paramiko.Transport(ftp_host, ftp_port)
    except Exception as e:
        return 'conn_error'
    try:
        transport.connect(username=ftp_username, password=ftp_password)
    except Exception as identifier:
        return 'auth_error'
    ftp_connection = paramiko.SFTPClient.from_transport(transport)
    return ftp_connection

def transfer_chunk_from_ftp_to_s3(
        ftp_file,
        s3_connection,
        multipart_upload,
        bucket_name,
        ftp_file_path,
        s3_file_path,
        part_number,
        chunk_size
    ):
    start_time = time.time()
    chunk = ftp_file.read(int(chunk_size))
    part = s3_connection.upload_part(
        Bucket = bucket_name,
        Key = s3_file_path,
        PartNumber = part_number,
        UploadId = multipart_upload['UploadId'],
        Body = chunk,
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    print('speed is {} kb/s total seconds taken {}'.format(math.ceil((int(chunk_size) /1024) / total_seconds), total_seconds))
    part_output = {
        'PartNumber': part_number,
        'ETag': part['ETag']
    }
    return part_output

def transfer_file_from_ftp_to_s3(bucket_name, ftp_file_path_and_name, ftp_file_name, s3_file_path, ftp_username, ftp_password, chunk_size):
    ftp_connection = open_ftp_connection(FTP_HOST, int(FTP_PORT), ftp_username, ftp_password)
    ftp_file = ftp_connection.file(ftp_file_path_and_name, 'r')
    s3_connection = boto3.client('s3')
    
    ftp_file_size = ftp_file._get_size()    
    
    # print('Listing Files in FTP Server............')
    # print(ftp_file.listdir_attr(path='.'))
    # print()    
    
    utime = ftp_file.stat().st_mtime   #.st_mtime
    print(utime)
    file_last_modified = str(datetime.fromtimestamp(utime))
    #print('====file_last_modified====', file_last_modified)
    #convert this date format "2021-02-25 10:00:21" to 20210225100021    
    file_last_modified_fmtd = file_last_modified[0:4] \
                + file_last_modified[5:7]  \
                + file_last_modified[8:10] \
                + file_last_modified[11:13] \
                + file_last_modified[14:16] \
                + file_last_modified[17:19] 
    print('FTP Server File Modified Date: ', file_last_modified , ' formated date: ', file_last_modified_fmtd)
    #s3_file_path_and_name = s3_file_path + '/' + str(file_last_modified_fmtd) + '_' \
    #               + ftp_file_name.replace('.zip','.txt.gzip')
    s3_file_path_and_name = s3_file_path + '/' + str(file_last_modified_fmtd) + '_' \
                   + ftp_file_name
    #s3_file_path_and_name = s3_file_path_and_name.replace('.zip','.txt.gzip')
    print()
    print('FTP Server File name: ', ftp_file_path_and_name)   
    print('S3 File name        : ', S3_BUCKET_NAME + '/' + s3_file_path_and_name)   
    print()
    #######
    try:
        s3_file = s3_connection.head_object(Bucket = bucket_name, Key = s3_file_path_and_name)
        if s3_file['ContentLength'] == ftp_file_size:
            print('File ' + s3_file_path_and_name + ' already exists in S3 bucket')
            ftp_file.close()
            return
    except Exception as e:
        pass
    if ftp_file_size <= int(chunk_size):
        #upload file in one go
        print('Starting transfer from FTP server to S3......')
        ftp_file_data = ftp_file.read()
        #print(ftp_file_data)
        #s3_connection.upload_fileobj(ftp_file_data, bucket_name, s3_file_path_and_name)
        data_obj= io.BytesIO(ftp_file_data)
        s3_connection.upload_fileobj(data_obj, bucket_name, s3_file_path_and_name)
        print('Successfully Transferred file from FTP to S3!')
        ftp_file.close()

    else:
        print('Starting transfer from FTP server to S3......')
        #upload file in chunks
        chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
        multipart_upload = s3_connection.create_multipart_upload(Bucket = bucket_name, Key = s3_file_path_and_name)
        parts = []
        for  i in range(chunk_count):
            print('Transferring chunk {}...'.format(i + 1))
            part = transfer_chunk_from_ftp_to_s3(
                ftp_file,
                s3_connection,
                multipart_upload,
                bucket_name,
                ftp_file_path_and_name,
                s3_file_path_and_name,
                i + 1,
                chunk_size
            )
            parts.append(part)
            print('Chunk {} Transferred Successfully!'.format(i + 1))

        part_info = {
            'Parts': parts
        }
        s3_connection.complete_multipart_upload(
        Bucket = bucket_name,
        Key = s3_file_path_and_name,
        UploadId = multipart_upload['UploadId'],
        MultipartUpload = part_info
        )
        print('All chunks Transferred to S3 bucket! File Transfer successful!')
        ftp_file.close()


if __name__ == '__main__':
    ftp_username = FTP_USERNAME
    ftp_password = FTP_PASSWORD
    ftp_file_path = FTP_FILE_PATH
    #'/Healthrive/Provider/997669768_021921_mn_updt_pd1_demo_004265.zip'
    s3_file_path = S3_FILE_PATH
    #'import/healthaxis/997669768_021921_mn_updt_pd1_demo_004265.txt.gzip'
    #ftp_file_path = str(input('Enter file path located on FTP server: '))
    print()
    print('File will copied from SFTP server('+str(FTP_HOST)+str(FTP_FILE_PATH)+')' \
          +' to AWS S3('+str(S3_BUCKET_NAME)+'/'+str(s3_file_path)+')')
    print()
    ftp_file_name = str(input('Enter file name to be copied to S3: '))
    print()
    #s3_file_path = str(input('Enter file path to upload to s3: '))
    ftp_file_path_and_name = ftp_file_path + '/' + str(ftp_file_name)
    ftp_connection = open_ftp_connection(FTP_HOST, int(FTP_PORT), ftp_username, ftp_password)
    if ftp_connection == 'conn_error':
        print('Failed to connect FTP Server!')
    elif ftp_connection == 'auth_error':
        print('Incorrect username or password!')
    else:
        try:
            ftp_file = ftp_connection.file(ftp_file_path_and_name, 'r')
        except Exception as e:
            print('File does not exists on FTP Server!')
        transfer_file_from_ftp_to_s3(S3_BUCKET_NAME, ftp_file_path_and_name, ftp_file_name, s3_file_path, ftp_username, ftp_password, CHUNK_SIZE)

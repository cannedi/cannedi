
import prefect
import io
import requests
from requests.structures import CaseInsensitiveDict
from prefect import Flow, task
from requests.auth import HTTPBasicAuth
#from azure.storage.blob import BlobServiceClient, BlobClient, \
#    ContainerClient
import pandas as pd
from time import sleep
from time import time, sleep
from datetime import *
from pytz import timezone, utc
import prefect
from prefect import Flow, task
import requests
import datetime
from prefect.storage.local import Local
from prefect.agent.local import LocalAgent
from prefect.schedules import CronSchedule
from prefect.schedules import Schedule, filters
from prefect.schedules.clocks import IntervalClock
import pendulum
import snowflake
import snowflake.connector as snowflake
from snowflake.connector import DictCursor
from configparser import ConfigParser
import os
import pickle
import json
#from pandas import json_normalize
import botocore
import boto3
from multiprocessing import Process, Lock, Value
from ssm_parameter_store import EC2ParameterStore
import getpass
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import logging
from logging import *


def get_logger():
    logger = prefect.context.get('logger')
    logger.addHandler(FileHandler(LOG_PATH))
    return logger

def get_pst_time():
    date_format = '%Y-%m-%d %H:%M:%S.000'
    date = datetime.now(tz=utc)
    date = date.astimezone(timezone('US/Pacific'))
    pstDateTime = date.strftime(date_format)
    return pstDateTime

def send_email(
        sns,
        sns_topic_arn,
        subject,
        message,
):
    topic = sns.Topic(sns_topic_arn)
    topic.publish(Subject=subject, Message=message)
    
task()
def readconfigini():
    # read config.ini file
    global runenvironment
    global flow_project_name
    global aws_region_name
    global aws_access_key_id
    global aws_secret_access_key
    global sns_topic_arn
    global sfcompute_wh
    global sfdatabase1
    global sfschema1
    global sfdatabase2
    global sfschema2
    global LOG_PATH
    global runenvironmentlower
    global ssm
    global sns_topic_arn
    global subject
    global errorsubject
    global err
    # global report_file_path_and_file
    try:
        config = ConfigParser()
        config.read('chrono_patient_delete_config.ini')
        # logging parms
        log_path_locn = config['logging_parms']['LOG_PATH_LOCN']
        log_file_name = config['logging_parms']['LOG_FILE_NAME']
        #print("Path ", log_path_locn)
        log_path_and_file = str(log_path_locn) + str(log_file_name)
        print("log_path_and_file=====", log_path_and_file)
        
        LOG_PATH = os.path.expanduser(log_path_and_file)
        print('LOG_PATH:', LOG_PATH)
        
        runenvironment = config['environment']['RUNENVIRONMENT']
        # prefect_flow_parms
        flow_project_name = config['prefect_flow_parms'
        ]['FLOW_PROJECT_NAME']
        # aws parms
        aws_region_name = config['aws_ec2_details']['AWS_REGION_NAME']
        aws_access_key_id = config['aws_ec2_details'
        ]['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = config['aws_ec2_details'
        ]['AWS_SECRET_ACCESS_KEY']
        sns_topic_arn = config['aws_ec2_details']['TOPIC_ARN']
        sns1 = boto3.session.Session().resource('sns',
                                                region_name=aws_region_name)
        runenvironmentlower = runenvironment.lower()
        if runenvironmentlower == 'localrun':
            prefix = '/dev/'
            sns1 = boto3.session.Session().resource('sns',
                                                    region_name=aws_region_name,
                                                    aws_access_key_id=aws_access_key_id,
                                                    aws_secret_access_key=aws_secret_access_key)
            
        subject = runenvironmentlower + ' : ' + 'chrono_patient_delete.py'
        errorsubject = runenvironmentlower + ' : Error in ' \
                       + 'chrono_patient_delete.py'
        # Snowflake parms
        
        sfcompute_wh = config['snowflake_db_parms']['SFCOMPUTE_WH']
        
        sfdatabase1 = config['snowflake_db_parms']['sfdatabase1']
        sfschema1 = config['snowflake_db_parms']['sfschema1']
        
        #sfdatabase2 = config['snowflake_db_parms']['sfdatabase2']
        #sfschema2 = config['snowflake_db_parms']['sfschema2']
        
    except Exception as e:
        logger = get_logger()
        err = ''
        err = str(get_pst_time()) \
              + '==========ERROR IN readconfigini() RELATED TO config.INI FILE VARIABLES: ' \
              + str(e)
        logger.error(err)
        send_email(sns1, sns_topic_arn, errorsubject, err)
        raise FAIL(str(e))
        
@task()
def read_aws_ssm_parameters(a):
    global connstring1
    global credentials
    global sns
    global ssm
    global errorsubject
    try:
        sns = boto3.session.Session().resource('sns',
                                               region_name=aws_region_name)
        ssm = boto3.session.Session().client('ssm',
                                             region_name=aws_region_name)
        runenvironmentlower = runenvironment.lower()
        if runenvironmentlower == 'localrun':
            prefix = '/dev/'
            sns = boto3.session.Session().resource('sns',
                                                   region_name=aws_region_name,
                                                   aws_access_key_id=aws_access_key_id,
                                                   aws_secret_access_key=aws_secret_access_key)
            ssm = boto3.session.Session().client('ssm',
                                                 region_name=aws_region_name,
                                                 aws_access_key_id=aws_access_key_id,
                                                 aws_secret_access_key=aws_secret_access_key)
        if runenvironmentlower == 'localrun':
            prefix = '/dev/'
        elif runenvironmentlower == 'dev':
            prefix = '/dev/'
        elif runenvironmentlower == 'uat':
            prefix = '/uat/'
        elif runenvironmentlower == 'qa':
            prefix = '/qa/'
        elif runenvironmentlower == 'prod':
            prefix = '/prod/'
        #api_locn = prefix + 'db/azure' + '/'
        db_locn = prefix + 'db/snowflake' + '/'
        
        credentials = {         
            'dbaccount': ssm.get_parameter(Name=db_locn + 'account',
                                           WithDecryption=True)['Parameter']['Value'],
            'dbuser': ssm.get_parameter(Name=db_locn + 'user',
                                        WithDecryption=True)['Parameter']['Value'],
            'dbpwd': ssm.get_parameter(Name=db_locn + 'password',
                                       WithDecryption=True)['Parameter']['Value'],
        }
        
    except Exception as e:
        logger = get_logger()
        err = ''
        err = str(get_pst_time()) \
              + '==========ERROR IN readssmparms() WHILE READING/SETTING AWS PARAMETER STORE VALUES: ' \
              + str(e)
        logger.error(err)
        send_email(sns, sns_topic_arn, errorsubject, err)
        raise FAIL()
        
def setup():    
    config = botocore.config.Config(read_timeout=900,
                                    connect_timeout=900,
                                    retries={'max_attempts': 0})
    func = boto3.client('lambda', region_name=aws_region_name,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        config=config)
    return func

@task
def query():    
    try:
        
        connstring1 = snowflake.connect(
            user=credentials['dbuser'],
            password=credentials['dbpwd'],
            account=credentials['dbaccount'],
            warehouse=sfcompute_wh,
            database=sfdatabase1,
            schema=sfschema1,
        )
        cursor = connstring1.cursor()
        Payload = \
            'select CHART_ID from vw_chrono_patients_to_delete limit 2'
        cursor.execute(Payload)
        row = cursor.fetchall()
        x = row
        #print('Read snowflake======:', x)
        connstring1.close()
        return x
    
    except Exception as e:
        logger = get_logger()
        err = ''
        err = str(get_pst_time()) + '==========ERROR IN query(): ' \
              + str(e)
        logger.error(err)
        send_email(sns, sns_topic_arn, errorsubject, err)
        raise FAIL()
        
@task
def extract(d):
    
    api_call_count = 0;
    api_call_count_total = 0;
    try:
        df = pd.DataFrame(columns=['CHART_ID','DETAILS_JSON'])
        # print(df)
        for x in d:
            while True:
                # print 'Fetching agent ' + x[0]
                #step 1: use patient list search by chart id to get dr chrono internal id:

                #curl --location --request GET 'https://drchrono.com/api/patients?chart_id=PESA000001&verbose=false' \
                #--header 'Authorization: Bearer EcWzMahfzfjTb94doI19PaERz0Hy5Z' \
                #--header 'Cookie: csrftoken=072rfGnvwP0lqHXa4Nap4VAIvXHX7cBqLbup7jxOeKDxNbFyTnbx2aD4IXELUp8L'
                
                #response = \
                    #requests.get('https://api.hixme.com/person/identity?personPublicKey=' + x[0])
                    
                
                    
                #response = requests.get('https://api.github.com/user', headers={'Authorization': 'Token Gsp_etrqRurrqSre9473289rv65BXdhafsdDB'}) print(r.status_code)
                #response = requests.get('https://drchrono.com/api/patients?chart_id=WEAA000005&verbose=false', headers={'Authorization': 'Token Gsp_etrqRurrqSre9473289rv65BXdhafsdDB'}) print(r.status_code)                
                #url = "https://reqbin.com/"
                
                url = "https://drchrono.com/api/patients?chart_id=" + x[0] + "&verbose=false"
                
                #url = "https://drchrono.com/api/patients?chart_id=WEAA000005&verbose=false"
                
                headers = CaseInsensitiveDict()
                headers["Accept"] = "application/json"
                headers["Authorization"] = "Bearer "
                
                
                resp = requests.get(url, headers=headers)
                
                #print(resp.status_code)
                
                #print('resp===', resp.text)
                
                if resp.status_code == 429:
                    print('TooManyRequests...sleeping...')
                    sleep(60)
                    continue
                
                df = df.append({'CHART_ID': x[0],'DETAILS_JSON': resp.text},
                               ignore_index=True)
                
                #print('DETAILS_JSON:' , response.text)                
                                 
                #print ('response.status_code======', response.status_code)
                '''
                api_call_count = api_call_count + 1
                if api_call_count == 100:
                    api_call_count_total = api_call_count_total + api_call_count
                    print('api_call_count_total:', api_call_count_total)
                    api_call_count = 0
                    
                if response.status_code == 429:
                    print('TooManyRequests...sleeping...')
                    sleep(60)
                    continue
                
                df = df.append({'PERSON_PUBLIC_KEY': x[0],'DETAILS_JSON': response.text},
                               ignore_index=True)
                
                #print('DETAILS_JSON:' , response.text)
                '''
                break            
        print('====df=====')
        print(df)
        
        return df

    except Exception as e:
        logger = get_logger()
        err = ''
        err = str(get_pst_time()) + '==========ERROR IN extract(): ' \
              + str(e)
        logger.error(err)
        send_email(sns, sns_topic_arn, errorsubject, err)
        raise FAIL()

       
flow_schedule = CronSchedule('10 05,06,07,08,09,10,11,12,13,14,15,16,17,18 * * *',
                             start_date=pendulum.now(tz='America/Los_Angeles'
                                                     ))

#with Flow("enrollme_person_registration_status", schedule=flow_schedule) as flow:
with Flow("chrono_patient_delete") as flow:
    readconfigini = readconfigini()
    readssm = read_aws_ssm_parameters(readconfigini)
    query = query()
    frame = extract(query)
flow.run()
#flow.register(project_name='prod_sync_etls')

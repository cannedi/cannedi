import boto3
import pandas as pd
import io
from sqlalchemy import create_engine

#pip install boto3
#pip install pandas
#pip install sqlalchemy
#pip install psycopg2-binary

#by defeault the loads to the public schema, make sure you are not overwriting any existing table by mistake
engine = \
    create_engine('postgresql://<<userid>>:<<password>>@pg-db.qa.surecolabs.com:5432/<<database>>'
                  )
    
REGION = 'us-west-2'
ACCESS_KEY_ID = ''       # credenyial here
SECRET_ACCESS_KEY = ''   #crendential here
BUCKET_NAME = 'docday-bucket-dev'
#KEY = 'postgres/Sample DocDay Providers.csv' # file path in S3 s3c = boto3.client
KEY = 'postgres/DocDay Providers - Only Names.csv' # file path in S3 s3c = boto3.client

s3c = boto3.client(
        's3', 
        region_name = REGION,
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = SECRET_ACCESS_KEY
    )

obj = s3c.get_object(Bucket= BUCKET_NAME , Key = KEY)

#df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

cnt = 0
print("Load started.....")

for df in \
    pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8'
                ,sep=',', engine='python', chunksize=1000):
                
  # print(df.head(100))

    df.to_sql('<<tablename>>', engine, index=False,
              if_exists='append')  # if the table already exists, append this data
    
    cnt = cnt + 1 
    
    print("Completed Chunk #: " + str(cnt)) 

print("Load completed.....")


import pandas as pd
from sqlalchemy import create_engine

# https://medium.com/@apoor/quickly-load-csvs-into-postgresql-using-python-and-pandas-9101c274a92f
# Instantiate sqlachemy.create_engine object

#engine = \
#    create_engine('postgresql://sathish:DBPassword4SK!@pg-db.dev.surecolabs.com:5432/docday'
#                  )

#by defeault the loads to the public schema, make sure you are not overwriting any existing table by mistake
engine = \
    create_engine('postgresql://<<userid>>:<<password>>@pg-db.qa.surecolabs.com:5432/<<database>>'
                  )


# columns = [
#     'SPECIALITY_CODE_ID',
#     'CODE',
#     'CODE_TYPE',
#     'SPECIALITY',
#     'DELETED',
#     'CREATED_ON',
#     ]

# Create an iterable that will read "chunksize=1000" rows
# at a time from the CSV file

#pd.read_csv(r"/Users/sathishkannaian/Documents/work_local/projects/docday/Specialities.csv"
                
for df in \
    pd.read_csv(r"/Users/sathishkannaian/Documents/work_local/projects/docday/Sample DocDay Providers.csv"
                ,sep=',', engine='python', chunksize=1000):
                #,header=None, sep=',', names=columns, chunksize=1000):

  # print(df.head(100))

    df.to_sql('<<tablename>>', engine, index=False,
              if_exists='append')  # if the table already exists, append this data

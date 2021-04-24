import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
#from dotenv import load_dotenv 

# define all static variables
input_snowflake_sql = "cytracom_stg_view_gen.sql"
client_models_path = "/cytracom_models/"
models_to_iterate = 200
  
# load latest environment variable list
#load_dotenv()

# Setup the snowflake connection
user = os.getenv('snowflake_user')
password = os.getenv('snowflake_password')
account = os.getenv('snowflake_account')

# sqlalchemy object for snowflake connection
engine = create_engine(URL(
    account=account,
    user=user,
    password=password,
    role='transformer_admin',
    warehouse='transforming'
))

#get current working directory
cwd = os.getcwd()
#build path to store generated models
model_path = cwd + client_models_path


# open client file containting view/ddl for creating dbt stg models
f = open(input_snowflake_sql)
#read file contect into variable
base_sql = f.read()

# snowflake connect and execute
try:
    conn = engine.connect()
    models =  conn.execute(base_sql).fetchmany(models_to_iterate)

    # iterate through all available models and generate files
    for model in models: 
        # build stage views
        sql_filepath = model_path + model['target_name'] 
        with open(sql_filepath, 'w+') as sql_file:
            sql_file.write(model['stage_ddl'])
            sql_file.close()
        # build yml files
        yml_filepath = sql_filepath.replace(".sql",".yml")
        with open(yml_filepath, 'w+') as yml_file:
            yml_file.write(model['yml_data'])
            yml_file.close()


# close connection once work is done
finally:
    conn.close()
    engine.dispose()



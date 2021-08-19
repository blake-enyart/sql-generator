import os
from os.path import join, dirname
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv

# define all static variables
INPUT_SNOWFLAKE_SQL = "sql_scripts/medstreaming_stg_view_gen.sql"
MODELS_TO_ITERATE = 200

# get current working directory
cwd = os.getcwd()
# load latest environment variable list
# TODO: fix environment variable loading
dotenv_path = join(cwd, '.env')
load_dotenv(dotenv_path)

# Setup the snowflake connection. Remember to update .env file if needed
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
CLIENT_MODELS_PATH = os.getenv("DBT_PROJECT_PATH")
SNOWFLAKE_DATABASE = 'EL_FIVETRAN_WORKFLOW'

# sqlalchemy object for snowflake connection
engine = create_engine(
    URL(
        account=account,
        user=user,
        password=password,
        role="engineer_admin",
        warehouse="engineering_adhoc",
        database=SNOWFLAKE_DATABASE
    )
)

# build path to store generated models
model_path = CLIENT_MODELS_PATH


# open client file containting view/ddl for creating dbt stg models
f = open(INPUT_SNOWFLAKE_SQL)
# read file content into a variable
base_sql = f.read()

# snowflake connect and execute
try:
    conn = engine.connect()
    models = conn.execute(base_sql).fetchmany(MODELS_TO_ITERATE)

    # iterate through all available models and generate files
    for model in models:
        # build stage views
        sql_filepath = model_path + model["target_name"]
        with open(sql_filepath, "w+") as sql_file:
            # print(model["stage_ddl"])
            sql_file.write(model["stage_ddl"])
            sql_file.close()
        # build yml files
        yml_filepath = sql_filepath.replace(".sql", ".yml")
        with open(yml_filepath, "w+") as yml_file:
            yml_file.write(model["yml_data"])
            yml_file.close()


# close connection once work is done
finally:
    conn.close()
    engine.dispose()

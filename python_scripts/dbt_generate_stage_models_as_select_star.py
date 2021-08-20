import os
from os.path import join
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv

# define all static variables
INPUT_SNOWFLAKE_SQL = join("sql_scripts", "medstreaming_stg_view_gen.sql")
MODELS_TO_ITERATE = 200
CLIENT_MODELS_PATH = '/Users/blakeenyart/programming/projects/medstreaming/medstreaming_workflow_dbt/models/staging/mimit/medstreamingemrdb/'
SNOWFLAKE_DATABASE = 'EL_FIVETRAN_WORKFLOW'

# load latest environment variable list
load_dotenv()

# Setup the snowflake connection. Remember to update .env file if needed
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# sqlalchemy object for snowflake connection
engine = create_engine(
    URL(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        role="engineer_admin",
        warehouse="engineering_adhoc",
        database=SNOWFLAKE_DATABASE
    )
)

# get current working directory
cwd = os.getcwd()
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

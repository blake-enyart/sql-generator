import os
from os.path import join
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader


# define all static variables
TEMPLATE_NAME = "medstreaming_stg_view_gen.sql.jinja"
MODELS_TO_ITERATE = 200
DATASOURCE = "medstreaming"
CLIENT_MODELS_PATH = f"/Users/blakeenyart/programming/projects/medstreaming/medstreaming_workflow_dbt/models/staging/mimit/{DATASOURCE}/"
SNOWFLAKE_DATABASE = "EL_FIVETRAN_WORKFLOW"

# Creates the Jinja environment
cwd = os.getcwd()
templates_path = join(cwd, "templates")
env = Environment(
    loader=FileSystemLoader(templates_path),
    autoescape=select_autoescape(["html", "xml"]),
)

# Indicates which file to get first
template = env.get_template(TEMPLATE_NAME)

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
        database=SNOWFLAKE_DATABASE,
    )
)

keys_list = ["datasource"]

values_list = [DATASOURCE]

# Create dictionary from list of keys and values
line_dict = dict(zip(keys_list, values_list))
base_sql = template.render(line_dict)

# snowflake connect and execute
try:
    conn = engine.connect()
    models = conn.execute(base_sql).fetchmany(MODELS_TO_ITERATE)

    # iterate through all available models and generate files
    for model in models:
        # build stage views
        sql_filepath = CLIENT_MODELS_PATH + model["target_name"]
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

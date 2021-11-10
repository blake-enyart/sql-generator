import os
from os.path import join
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader


# define all static variables
TEMPLATE_NAME = "snowflake_src_view_gen.sql.jinja"
MODELS_TO_ITERATE = 1000
CLIENT_MODELS_PATH = "/home/blake-enyart/data_warehouse_dbt/models/raw/"
SNOWFLAKE_DATABASE = "raw_airbyte"
GENERATOR_CONFIG = {
    "database": SNOWFLAKE_DATABASE.lower(),
    # Must generate staging models for a single schema at a time
    "schema_filter": "orthofi__dbo",
    # Use to exclude a set pattern for tables such as "_airbyte_"
    "table_filter_fuzzy_exclude": "_airbyte_",
    "dbt_source_name": "orthofi__dbo",
    "loaded_at_field": "_airbyte_normalized_at"
}

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
ROLE = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# sqlalchemy object for snowflake connection
engine = create_engine(
    URL(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )
)

# Generate Jinja SQL template
base_sql = template.render(GENERATOR_CONFIG)

# snowflake connect and execute
try:
    conn = engine.connect()
    models = conn.execute(base_sql).fetchmany(MODELS_TO_ITERATE)

    # iterate through all available models and generate files
    for model in models:
        # build directories
        dir_path = join(
            CLIENT_MODELS_PATH,  
        )
        os.makedirs(dir_path, exist_ok=True)
        yml_filepath = join(dir_path, f"src_{GENERATOR_CONFIG['dbt_source_name']}.yml")
        # build yml files
        with open(yml_filepath, "w+") as yml_file:
            yml_file.write(model["yml_data"])
            yml_file.close()

# close connection once work is done
finally:
    conn.close()
    engine.dispose()

from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

TEMPLATE_NAME = "snowflake_raw_metrics.sql.jinja"

load_dotenv()

# Setup the snowflake connection. Remember to update .env file if needed
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")

# sqlalchemy object for snowflake connection
engine = create_engine(
    URL(
        account=account,
        user=user,
        password=password,
        role="transformer_admin",
        warehouse="transforming",
    )
)

# Query table names
try:
    connection = engine.connect()
    table_data = connection.execute(
        """
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME 
        FROM EL_MELTANO_RAW.INFORMATION_SCHEMA.TABLES 
        WHERE ROW_COUNT != 0
    """
    ).fetchmany(30)

finally:
    connection.close()
    engine.dispose()

# Creates the Jinja environment
cwd = os.getcwd()
templates_path = os.path.join(cwd, "templates")
env = Environment(
    loader=FileSystemLoader(templates_path),
    autoescape=select_autoescape(["html", "xml"]),
)

template = env.get_template(TEMPLATE_NAME)

keys_list = ["raw_database", "raw_schema", "raw_table"]


with open("sql_script_output.sql", "a") as file_output:
    file_output.seek(0)
    file_output.truncate()
    number_of_tables = len(table_data)
    for idx, table in enumerate(table_data):
        # Converts str to list
        # Create dictionary from list of keys and values
        line_dict = dict(zip(keys_list, table))
        file_output.write(template.render(line_dict))
        if idx < (number_of_tables - 1):
            file_output.write("\nUNION ALL\n")
        else:
            file_output.write(";")

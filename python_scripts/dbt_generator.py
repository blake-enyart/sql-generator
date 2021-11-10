import os
from os.path import join
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader
import re
from os.path import exists
import sys


class DbtSqlGenerator:
    # load latest environment variable list
    load_dotenv()
    # Setup the snowflake connection. Remember to update .env file if needed
    USER = os.getenv("SNOWFLAKE_USER")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    ROLE = os.getenv("SNOWFLAKE_ROLE")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    database_msg = "Which Snowflake database would you like to use? (i.e raw_airbyte): "
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE") or input(database_msg)

    def __init__(self) -> None:
        try:
            # sqlalchemy object for snowflake connection
            self.engine = create_engine(
                URL(
                    account=self.ACCOUNT,
                    user=self.USER,
                    password=self.PASSWORD,
                    role=self.ROLE,
                    warehouse=self.WAREHOUSE,
                    database=self.SNOWFLAKE_DATABASE,
                )
            )
        except Exception as e:
            env_msg = """
            Remember to configure your .env file prior to running this script
            """
            env_msg = re.sub(" +", " ", env_msg)
            env_msg = re.sub("\n +", "\n", env_msg)
            print(env_msg)
            sys.exit()

        dbt_project_path_msg = """What is the full file path to the dbt project? 
        (i.e C:\\\\Users\\blake-enyart\\<dbt_project_root>")

        Full file path: """
        dbt_project_path_msg = re.sub(" +", " ", dbt_project_path_msg)
        dbt_project_path_msg = re.sub("\n +", "\n", dbt_project_path_msg)
        self.DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH") or input(
            dbt_project_path_msg
        )

        schema_msg = (
            "Which Snowflake schema would you like to use? (i.e orthofi__dbo): "
        )
        self.SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") or input(schema_msg)

        table_exclude_prompt = "Is there a table name pattern you would like to exclude (i.e. _airbyte_)? (y/N): "
        text = (
            os.getenv("TABLE_FILTER_FUZZY_EXCLUDE")
            or input(table_exclude_prompt)
            or "n"
        )
        text = text.lower()
        if text == "y":
            table_exclude_msg = "What is the table pattern you would like to exclude? (i.e. _airbyte_): "
            self.TABLE_FILTER_FUZZY_EXCLUDE = input(table_exclude_msg).lower()
        elif text == "n":
            self.TABLE_FILTER_FUZZY_EXCLUDE = None
        else:
            self.TABLE_FILTER_FUZZY_EXCLUDE = text

        source_prompt = f'Do you want the source schema ("{self.SNOWFLAKE_SCHEMA}") to be the name of the dbt source? (Y/n): '
        text = os.getenv("DBT_SOURCE_NAME") or input(source_prompt) or "y"
        text = text.lower()
        if text == "y":
            self.DBT_SOURCE_NAME = self.SNOWFLAKE_SCHEMA
        elif text == "n":
            dbt_source_msg = (
                "What should the name of the source be? (i.e orthofi__dbo): "
            )
            self.DBT_SOURCE_NAME = input(dbt_source_msg)
        else:
            self.DBT_SOURCE_NAME = text

        src_stg_prompt = (
            "Would you like to configure a dbt source(src) or stage(stg)? (src/stg): "
        )
        text = input(src_stg_prompt)
        text = text.lower()
        if text == "src":
            DbtSourceGenerator(self)
        elif text == "stg":
            DbtStageGenerator(self)


class DbtSourceGenerator:
    # define all static variables
    TEMPLATE_NAME = "snowflake_src_view_gen.sql.jinja"
    MODELS_TO_ITERATE = 1000

    def __init__(self, config: DbtSqlGenerator) -> None:
        # TODO: Build out prompt
        self.MODEL_OUTPUT_DIR_PATH = join(config.DBT_PROJECT_PATH, "models", "raw")
        self.SNOWFLAKE_SCHEMA = config.SNOWFLAKE_SCHEMA

        loaded_at_msg = "What should the default loaded_at_field be? (i.e. _airbyte_normalized_at): "
        self.LOADED_AT_FIELD = os.getenv("LOADED_AT_FIELD") or input(loaded_at_msg)
        while self.LOADED_AT_FIELD == "":
            loaded_at_msg = (
                "Please input a loaded_at_field (i.e. _airbyte_normalized_at): "
            )
            self.LOADED_AT_FIELD = os.getenv("LOADED_AT_FIELD") or input(loaded_at_msg)

        self.GENERATOR_CONFIG = {
            "database": config.SNOWFLAKE_DATABASE.lower(),
            # Must generate staging models for a single schema at a time
            "schema_filter": self.SNOWFLAKE_SCHEMA.lower(),
            # Use to exclude a set pattern for tables such as "_airbyte_"
            "table_filter_fuzzy_exclude": config.TABLE_FILTER_FUZZY_EXCLUDE,
            "dbt_source_name": config.DBT_SOURCE_NAME.lower(),
            "loaded_at_field": self.LOADED_AT_FIELD.lower(),
        }

        print("\n")
        print("Using the following parameters to generate scripts:")
        [print(f"{k.upper()}: {v}") for k,v in self.GENERATOR_CONFIG.items()]
        print("----------------------")

        self.engine = config.engine

        self.source_generator()

    def source_generator(self):
        dir_path = self.MODEL_OUTPUT_DIR_PATH

        yml_filepath = join(
            dir_path, f"src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml"
        )
        if exists(yml_filepath):
            prompt_msg = f"""
                Would you like to overwrite the source file: "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml"?
                
                WARNING: Proceeding will erase any modifications and restore source file to defaults.
                Respond (N/y): """
        else:
            prompt_msg = f"""
                Would you like to create the source file: "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml"?
                Respond (N/y): """
        prompt_msg = re.sub(" +", " ", prompt_msg)
        prompt_msg = re.sub("\n +", "\n", prompt_msg)
        text = ""
        while text.lower() not in ["n", "y"]:
            text = input(prompt_msg) or "n"
            if text.lower() == "n":
                print(
                    f"""Cancelling generation of "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml" """
                )
                sys.exit()
            elif text.lower() == "y":
                self.produce_src_file(dir_path=dir_path, yml_filepath=yml_filepath)
            else:
                prompt_msg = "Invalid response. Please respond with (N/y): "
                text = input(prompt_msg)
                if text.lower() == "n":
                    print(
                        f"""Cancelling generation of "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml" """
                    )
                    sys.exit()
                elif text.lower() == "y":
                    self.produce_src_file(dir_path=dir_path, yml_filepath=yml_filepath)

    def produce_src_file(self, dir_path: str, yml_filepath: str):
        # Creates the Jinja environment
        cwd = os.getcwd()
        templates_path = join(cwd, "templates")
        env = Environment(
            loader=FileSystemLoader(templates_path),
            autoescape=select_autoescape(["html", "xml"]),
        )

        # Indicates which file to get first
        template = env.get_template(self.TEMPLATE_NAME)

        # Generate Jinja SQL template
        base_sql = template.render(self.GENERATOR_CONFIG)

        # snowflake connect and execute
        try:
            print("Gathering tables and generating source file...")
            print("----------------------")
            conn = self.engine.connect()
            models = conn.execute(base_sql).fetchmany(self.MODELS_TO_ITERATE)

            # iterate through all available models and generate files
            for model in models:
                # build directories structure
                os.makedirs(dir_path, exist_ok=True)
                if exists(yml_filepath):
                    success_msg = f"""
                    Successfully overwrote "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml"

                    Full file path: {yml_filepath}
                    """
                else:
                    success_msg = f"""
                    Successfully created "src_{self.GENERATOR_CONFIG['dbt_source_name']}.yml"

                    Full file path: {yml_filepath}
                    """
                # build yml files
                with open(yml_filepath, "w+") as yml_file:
                    yml_file.write(model["yml_data"])
                    yml_file.close()

                success_msg = re.sub(" +", " ", success_msg)
                success_msg = re.sub("\n +", "\n", success_msg)
                print(success_msg)
        # close connection once work is done
        finally:
            conn.close()
            self.engine.dispose()


class DbtStageGenerator:
    # define all static variables
    TEMPLATE_NAME = "snowflake_stg_view_gen.sql.jinja"
    MODELS_TO_ITERATE = 10000

    def __init__(self, config: DbtSqlGenerator) -> None:
        self.SNOWFLAKE_SCHEMA = config.SNOWFLAKE_SCHEMA
        self.DBT_SOURCE_NAME = config.DBT_SOURCE_NAME
        self.MODEL_OUTPUT_DIR_PATH = join(config.DBT_PROJECT_PATH, "models", "staging")

        table_filter_prompt = "Would you like to generate a single set of files (.sql/.yml) for one source table? (Y/n): "
        text = os.getenv("TABLE_FILTER") or input(table_filter_prompt) or "y"
        text = text.lower()
        if text == "y":
            table_filter_msg = (
                "Which table would you like to work on? (i.e. contract): "
            )
            self.TABLE_FILTER = input(table_filter_msg).lower()
        elif text == "n":
            self.TABLE_FILTER = None
        else:
            self.TABLE_FILTER = text

        self.GENERATOR_CONFIG = {
            "database": config.SNOWFLAKE_DATABASE.lower(),
            # Must generate staging models for a single schema at a time
            "schema_filter": self.SNOWFLAKE_SCHEMA.lower(),
            # Use to generate a single staging model set to None for only schema filters
            "table_filter": self.TABLE_FILTER,
            # Use to exclude a set pattern for tables such as "_airbyte_"
            "table_filter_fuzzy_exclude": config.TABLE_FILTER_FUZZY_EXCLUDE,
            "dbt_source_name": self.DBT_SOURCE_NAME.lower(),
        }

        print("\n")
        print("Using the following parameters to generate scripts:")
        [print(f"{k.upper()}: {v}") for k,v in self.GENERATOR_CONFIG.items()]
        print("----------------------")
        print("\n")
        self.engine = config.engine

        self.source_generator()

    def source_generator(self):
        # Creates the Jinja environment
        cwd = os.getcwd()
        templates_path = join(cwd, "templates")
        env = Environment(
            loader=FileSystemLoader(templates_path),
            autoescape=select_autoescape(["html", "xml"]),
        )

        # Indicates which file to get first
        template = env.get_template(self.TEMPLATE_NAME)

        # Generate Jinja SQL template
        base_sql = template.render(self.GENERATOR_CONFIG)

        print("Gathering tables and generating stage files (.sql/.yml)...")
        print("----------------------")
        conn = self.engine.connect()
        models = conn.execute(base_sql).fetchmany(self.MODELS_TO_ITERATE)

        for model in models:
            # build directories
            dir_path = join(
                self.MODEL_OUTPUT_DIR_PATH,
                self.GENERATOR_CONFIG["schema_filter"],
            )
            os.makedirs(dir_path, exist_ok=True)
            sql_filepath = join(dir_path, model["target_name"])

            base_name = model["target_name"].replace(".sql", "")
            if exists(sql_filepath):
                prompt_msg = f"""
                    Would you like to overwrite the stage files (.sql/.yml) for: "{base_name}"?
                    
                    WARNING: Proceeding will erase any modifications and restore stage files to defaults.
                    Respond (N/y): """
            else:
                prompt_msg = f"""
                    Would you like to create the stage files (.sql/.yml) for: "{base_name}"?
                    Respond (N/y): """
            prompt_msg = re.sub(" +", " ", prompt_msg)
            prompt_msg = re.sub("\n +", "\n", prompt_msg)
            text = ""
            while text.lower() not in ["n", "y", "all"]:
                text = input(prompt_msg) or "n"
                if text.lower() == "n":
                    print(
                        f"""Cancelling generation of stage files (.sql/.yml) for "{base_name}" """
                    )
                    sys.exit()
                elif text.lower() == "y":
                    self.produce_src_files(
                        dir_path=dir_path, model=model, base_name=base_name
                    )
                else:
                    prompt_msg = "Invalid response. Please respond with (N/y): "
                    text = input(prompt_msg)
                    if text.lower() == "n":
                        print(
                            f"""Cancelling generation of stage files (.sql/.yml) for "{base_name}" """
                        )
                        sys.exit()
                    elif text.lower() == "y":
                        self.produce_src_files(
                            dir_path=dir_path, model=model, base_name=base_name
                        )

    def produce_src_files(self, dir_path: str, model, base_name: str):
        sql_filepath = join(dir_path, model["target_name"])

        if exists(sql_filepath):
            success_msg = f"""
            Successfully overwrote "{base_name}.sql" and "{base_name}.yml"

            Full model path: {sql_filepath}
            Full yml path: {sql_filepath.replace(".sql", ".yml")}
            """
        else:
            success_msg = f"""
            Successfully overwrote "{base_name}.sql" and "{base_name}.yml"

            Full model path: {sql_filepath}
            Full yml path: {sql_filepath.replace(".sql", ".yml")}
            """

        # build stage views
        with open(sql_filepath, "w+") as sql_file:
            sql_file.write(model["stage_ddl"])
            sql_file.close()
        # build yml files
        yml_filepath = sql_filepath.replace(".sql", ".yml")
        with open(yml_filepath, "w+") as yml_file:
            yml_file.write(model["yml_data"])
            yml_file.close()

        success_msg = re.sub(" +", " ", success_msg)
        success_msg = re.sub("\n +", "\n", success_msg)
        print(success_msg)


if __name__ == "__main__":
    DbtSqlGenerator()

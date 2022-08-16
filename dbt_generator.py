import os
from os.path import join
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError

# from sqlalchemy.schema import MetaData
from sqlalchemy import inspect

from dotenv import load_dotenv
from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader
import re
from os.path import exists
import sys
import pandas as pd


class DbtSqlGenerator:
    # load latest environment variable list
    load_dotenv()
    # Setup the snowflake connection. Remember to update .env file if needed
    USER = os.getenv("SNOWFLAKE_USER")
    PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    ROLE = os.getenv("SNOWFLAKE_ROLE")
    WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_config = [USER, PASSWORD, ACCOUNT, ROLE, WAREHOUSE]
    snowflake_config = [x != "" for x in snowflake_config]
    if not all(snowflake_config):
        print("Please configure Snowflake credentials in .env")
        sys.exit()
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
            self.conn = self.engine.connect()
        except DatabaseError as e:
            print("Have you turned on the VPN to access the Snowflake instance?")
            print(f"{type(e).__name__}:", e.orig)
            sys.exit()
        except Exception as e:
            env_msg = """
            Remember to correctly configure your .env file prior to running this script
            """
            env_msg = re.sub(" +", " ", env_msg)
            env_msg = re.sub("\n +", "\n", env_msg)
            print(env_msg)
            sys.exit()

        dbt_project_path_msg = """What is the full file path to the dbt project? 
        (i.e C:\\Users\\blake-enyart\\<dbt_project_root>)
        Do not add the final slash.

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

        print("----------------------")
        print("Using the following parameters to generate scripts:")
        [print(f"{k.upper()}: {v}") for k, v in self.GENERATOR_CONFIG.items()]
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
        self.STAGE_DATE_FILTER = os.getenv("STAGE_DATE_FILTER").lower()

        table_filter_prompt = """
        Would you like to generate a single set of stage files (.sql/.yml) for one source table?
        If you want to create sets of stage files for multiple tables respond with "n".

        Respond (Y/n): """
        table_filter_prompt = re.sub(" +", " ", table_filter_prompt)
        table_filter_prompt = re.sub("\n +", "\n", table_filter_prompt)
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

        print("----------------------")
        print("Using the following parameters to generate scripts:")
        [print(f"{k.upper()}: {v}") for k, v in self.GENERATOR_CONFIG.items()]
        print("----------------------")

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

        template = env.get_template("snowflake_columns_metadata.sql.jinja")
        columns_sql = template.render(self.GENERATOR_CONFIG)

        print("Gathering tables and generating stage files (.sql/.yml)...")
        print("----------------------")
        conn = self.engine.connect()
        columns_df = pd.read_sql(columns_sql, conn)
        table_columns = (
            columns_df.groupby("table_name")["column_name"].apply(list).to_dict()
        )
        for table, columns in table_columns.items():
            has_date_filter = any(
                [x.lower() == self.STAGE_DATE_FILTER for x in columns]
            )
            table_columns[table] = has_date_filter
            table_columns = {k.lower().strip(): v for k, v in table_columns.items()}

        # Indicates which file to get first
        template = env.get_template(self.TEMPLATE_NAME)
        # Generate Jinja SQL template
        base_sql = template.render(self.GENERATOR_CONFIG)
        models = conn.execute(base_sql).fetchmany(self.MODELS_TO_ITERATE)

        exists_cnt = 0
        create_cnt = 0
        if len(models) == 1:
            self.produce_single_src_file(models=models, date_filters=table_columns)
            sys.exit()
        for model in models:
            dir_path = join(
                self.MODEL_OUTPUT_DIR_PATH,
                self.GENERATOR_CONFIG["schema_filter"],
            )
            sql_filepath = join(dir_path, model["target_name"])
            if exists(sql_filepath):
                exists_cnt += 1
            else:
                create_cnt += 1
        prompt_msg = f"""
            With these parameters, the stage files (.sql/.yml) would behave as:
            - {exists_cnt} set(s) of stage files would be overwritten 
            - {create_cnt} set(s) of stage files would be created 
            
            How do you want to proceed?
            1 - Overwrite all old files and create all new files
            2 - Only create new files
            3 - Only overwrite existing files
            4 - Cancel
            Respond with a numeric option (1-4): """
        prompt_msg = re.sub(" +", " ", prompt_msg)
        prompt_msg = re.sub("\n +", "\n", prompt_msg)

        cancel_msg = "Cancelling generation of a set of stage files (.sql/.yml)"
        confirm_msg = "Are you sure you want to overwrite existing files? (y/N): "

        text = ""
        while text not in ["1", "2", "3", "4"]:
            text = input(prompt_msg) or "4"
            if text == "4":
                print(cancel_msg)
                sys.exit()
            elif text == "1":
                text = ""
                while text not in ["y", "n"]:
                    text = input(confirm_msg) or "n"
                    text = text.lower()
                    if text == "y":
                        self.produce_all_src_files(
                            models=models,
                            date_filters=table_columns,
                            overwrite=True,
                            create=True,
                        )
                    elif text == "n":
                        print(cancel_msg)
                        sys.exit()
                    else:
                        text = input(confirm_msg) or "n"
                        text = text.lower()
                print(cancel_msg) if text == "n" else None
                sys.exit()
            elif text == "2":
                self.produce_all_src_files(
                    models=models,
                    date_filters=table_columns,
                    overwrite=False,
                    create=True,
                )
            elif text == "3":
                text = ""
                while text not in ["y", "n"]:
                    text = input(confirm_msg) or "n"
                    text = text.lower()
                    if text == "y":
                        self.produce_all_src_files(
                            models=models,
                            date_filters=table_columns,
                            overwrite=True,
                            create=False,
                        )
                    elif text == "n":
                        print(cancel_msg)
                        sys.exit()
                    else:
                        text = input(confirm_msg) or "n"
                        text = text.lower()
                print(cancel_msg) if text == "n" else None
                sys.exit()

    def produce_all_src_files(
        self, models, date_filters: dict, overwrite: bool = False, create: bool = True
    ):
        overwrite_cnt = 0
        created_cnt = 0

        for model in models:
            # build directories
            dir_path = join(
                self.MODEL_OUTPUT_DIR_PATH,
                self.GENERATOR_CONFIG["schema_filter"],
            )
            os.makedirs(dir_path, exist_ok=True)
            sql_filepath = join(dir_path, model["target_name"])

            base_name = model["target_name"].replace(".sql", "")
            # file exists and overwrite is true
            if exists(sql_filepath) and overwrite:
                self.produce_src_files(
                    dir_path=dir_path,
                    model=model,
                    date_filters=date_filters,
                    base_name=base_name,
                    silent=True,
                )
                overwrite_cnt += 1
            # file does not exist and create is true
            elif not exists(sql_filepath) and create:
                self.produce_src_files(
                    dir_path=dir_path,
                    model=model,
                    date_filters=date_filters,
                    base_name=base_name,
                    silent=True,
                )
                created_cnt += 1
        success_msg = f"Created {created_cnt} set(s) of stage files and overwrote {overwrite_cnt} set(s) of stage files."
        print(success_msg)

    def produce_single_src_file(self, models, date_filters: str):
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
                    Would you like to overwrite the set of stage files (.sql/.yml) for: "{base_name}"?
                    
                    WARNING: Proceeding will erase any modifications and restore stage files to defaults.
                    Respond (N/y): """
            else:
                prompt_msg = f"""
                    Would you like to create the set of stage files (.sql/.yml) for: "{base_name}"?
                    Respond (N/y): """
            prompt_msg = re.sub(" +", " ", prompt_msg)
            prompt_msg = re.sub("\n +", "\n", prompt_msg)
            text = ""
            while text.lower() not in ["n", "y"]:
                text = input(prompt_msg) or "n"
                if text.lower() == "n":
                    print(
                        f"""Cancelling generation of a set of stage files (.sql/.yml) for "{base_name}" """
                    )
                    sys.exit()
                elif text.lower() == "y":
                    self.produce_src_files(
                        dir_path=dir_path,
                        model=model,
                        date_filters=date_filters,
                        base_name=base_name,
                    )
                else:
                    prompt_msg = "Invalid response. Please respond with (N/y): "
                    text = input(prompt_msg)
                    if text.lower() == "n":
                        print(
                            f"""Cancelling generation of a set of stage files (.sql/.yml) for "{base_name}" """
                        )
                        sys.exit()
                    elif text.lower() == "y":
                        self.produce_src_files(
                            dir_path=dir_path,
                            model=model,
                            date_filters=date_filters,
                            base_name=base_name,
                        )

    def produce_src_files(
        self,
        dir_path: str,
        date_filters: dict,
        model,
        base_name: str,
        silent: bool = False,
    ):
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
        table_name = (
            model["target_name"]
            .replace(f"stg_{self.SNOWFLAKE_SCHEMA.lower()}__", "")
            .replace(".sql", "")
        )
        if date_filters[table_name]:
            f = open("date_filter_jinja.txt", "r")
            date_filter_jinja = f.read()
            date_filter_jinja = date_filter_jinja.format(
                table_name=table_name, stage_date_filter=self.STAGE_DATE_FILTER
            )

            stage_ddl = model["stage_ddl"].replace(")\n\nselect", date_filter_jinja)
            stage_ddl = stage_ddl.replace(
                f"from {table_name} \n", "from update_filter\n"
            )
        else:
            stage_ddl = model["stage_ddl"]

        # build stage views
        with open(sql_filepath, "w+") as sql_file:
            sql_file.write(stage_ddl)
            sql_file.close()
        # build yml files
        yml_filepath = sql_filepath.replace(".sql", ".yml")
        with open(yml_filepath, "w+") as yml_file:
            yml_file.write(model["yml_data"])
            yml_file.close()

        success_msg = re.sub(" +", " ", success_msg)
        success_msg = re.sub("\n +", "\n", success_msg)

        if not silent:
            print(success_msg)


if __name__ == "__main__":
    sql_gen = DbtSqlGenerator()

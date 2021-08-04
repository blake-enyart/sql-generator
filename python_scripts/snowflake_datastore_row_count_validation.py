from jinja2 import Environment, select_autoescape
from jinja2.loaders import FileSystemLoader
import os 

FILENAME = 'results.csv'
TEMPLATE_NAME = 'snowflake_datastore_row_count_validation.sql.jinja'

# Creates the Jinja environment
cwd = os.getcwd()
templates_path = os.path.join(cwd, 'templates')
env = Environment(
    loader=FileSystemLoader(templates_path),
    autoescape=select_autoescape(['html', 'xml'])
)

# Indicates which file to get first
template = env.get_template(TEMPLATE_NAME)

file_input = open(FILENAME, 'r')

keys_list = [
    'full_table_name'
]

# Creates the script
with open('sql_script_output.sql', 'a') as file_output:
    file_output.seek(0)
    file_output.truncate()
    file_input_load = file_input.readlines()
    number_of_lines = len(file_input_load)
    for idx, line in enumerate(file_input_load):
        # Converts str to list
        line_list = line.split(',')

        # Remove newline characters
        line_list = [x.rstrip("\n") for x in line_list]
        # Create dictionary from list of keys and values
        line_dict = dict(zip(keys_list, line_list))
        file_output.write(
            template.render(line_dict)
        )
        if idx < (number_of_lines - 1):
            file_output.write(
                "\nUNION ALL\n"
            )
        else:
            file_output.write(
                ";"
            )

# sql-generator
Generates SQL scripts built based in Python, Jinja templates, and SQL. This is a prototype for data testing and generating dbt source/staging files.

## Getting Started
Start by installing `python` and then `pip install poetry`. After poetry is installed, run:
- `poetry config virtualenvs.in-project true`
- `poetry install`
- `poetry shell`

The most developed script in the repo is the [dbt_generator.py](dbt_generator.py). There are two paths to using this tool to generate dbt source/staging models.

### Option 1
- Duplicate the [.env.sample](.env.sample) and rename to `.env`
- Fill out all of the variables in the new `.env` file 
- Run `python dbt_generator.py` and follow the prompts to complete the generation script

### Option 2
- Duplicate the [.env.sample](.env.sample) and rename to `.env`
- Fill out only the mandatory variables in the new `.env` file 
- Run `python dbt_generator.py` and follow the prompts to complete the generation script

### Option 3

There are several other rudimentary scripts in here at the moment that generate a batch of data quality checks that consist of row counts, column counts, number of loads to the DATASTORE, and what are the oldest and newest load dates for a specific table in the DATASTORE.

This script is a brittle and rudimentary SQL generator for now, but will hopefully serve as a launch point for later development.

I manually create the Souce-to-Target mapping from RAW --> STAGE --> DATASTORE in the first sheet within the worksheet while referencing the Miro diagram. From here, I copy the results as plain txt into the `CSV Export` sheet where I remove the mapping columns and ensure all RAW tables are linked to an associated STAGE and DATASTORE table. 

After this, I export the sheet as a CSV and copy that file into the root of this project. The CSV should have 9 columns in the pattern of DATABASE,SCHEMA,TABLE for each layer on a single row. From here, I navigate to the `snowflake_raw_to_datastore_validation_generator.py` script and ensure that the `FILENAME` and `TEMPLATE_NAME` are correct.

At this point, all that needs to be done is run:

```bash
python snowflake_raw_to_datastore_validation_generator.py
```

That command will output the resulting SQL script to a file called `sql_script_output.sql` which can then be copy/pasted into the Snowflake UI or SQL IDE to quickly profile the RAW to DATASTORE layers.

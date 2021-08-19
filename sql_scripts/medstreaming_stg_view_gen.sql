with join_filter as (
  
  select
    lower(all_columns.table_catalog) as source_db,
    lower(all_columns.table_schema) as source_schema,
    lower(all_columns.table_name) as source_table,
    'stg_' || case
      source_schema
      when 'mimit_test_2016_medstreaming_dbo' then 'mimit_medstreaming'
      else source_schema
    end || '__' || lower(source_table) || '.sql' as target_name,
    upper(all_columns.column_name) as column_name,
    all_columns.ordinal_position,
    lower(all_columns.data_type) as data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    datetime_precision
  from
    information_schema.columns all_columns
  where
    lower(table_schema) in ('mimit_test_2016_medstreaming_dbo')
),

src as (
  select
    source_db,
    source_schema,
    source_table,
    target_name,
    column_name,
    -9 ordinal_position,
    'with ' || lower(source_table) || ' as (\n
select *
from  \n-- Necessary to switch between Development and Production data sources\n'||
    '{% if target.name == ''dev'' %}\n' ||
    '        {{ source(''' || case source_schema
      when 'mimit_test_2016_medstreaming_dbo' then 'mimit_test_medstreaming'
      else source_schema
      end || ''', ''' || lower(source_table) || ''') }}\n' || '    {% else %}\n' ||
    '        {{ source(''' || case source_schema
      when 'mimit_prod_2016_medstreaming_dbo' then 'mimit_prod_medstreaming'
      else 'source_tbd'
      end || ''', ''' || lower(source_table) || ''') }}\n' || '{% endif %}\n' ||'\n)\n' 
  
  as sql_text
  from
    join_filter
  where
    ordinal_position = 1
  union
  all
  select
    source_db,
    source_schema,
    source_table,
    target_name,
    column_name,
    ordinal_position,
    case
      when ordinal_position = 1 then 'select\n    '
      else '  ,'
    end 
    ||
    case
      when data_type in ('text', 'varchar', 'varying') then 'trim(' || '"' || column_name || '"' || ') as ' || '"' || column_name || '"'
      else '"' || column_name || '"'
    end 
    as sql_text
  from
    join_filter
  union
  all
  select
    source_db,
    source_schema,
    source_table,
    target_name,
    column_name,
    9999 ordinal_position,
    'from ' || lower(source_table) || ' ' as sql_text
  from
    join_filter
  where
    ordinal_position = 1
),

stg_view_gen as (
  select
    src.source_db,
    src.source_schema,
    src.source_table,
    src.target_name,
    listagg(sql_text, '\n ') within group (
      order by
        ordinal_position
    ) stage_ddl
  from
    src
  group by
    source_db,
    source_schema,
    source_table,
    target_name
  order by
    2 desc,
    3
),

columns as (
  select
    *
  from
    information_schema.columns
),

yml_gen as (
  select
    table_schema,
    table_name,
    ordinal_position,
    '    - name: ' || lower(column_name) || '\n      description: tbd' yml
  from
    columns
  where
    lower(table_schema) in ('mimit_test_2016_medstreaming_dbo')
),

cte_yml as (
  select
    lower(table_schema) as source_schema,
    lower(table_name) as source_table,
    listagg(yml, '\n') within group (
      order by
        ordinal_position
    ) as yml_data
  from
    yml_gen
  group by
    1,
    2
)
select
  target_name,
  stage_ddl,
  'version: 2\n\nmodels:\n  - name: ' || split_part(stg.target_name,'.',0) || '\n    columns:\n' || yml_data as yml_data
from
  stg_view_gen stg
  join cte_yml yml on stg.source_schema = yml.source_schema
  and stg.source_table = yml.source_table
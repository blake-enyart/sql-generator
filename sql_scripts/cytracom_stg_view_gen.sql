with join_filter as (
    select
        ordinal_position,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        lower(
            table_catalog
        ) as source_db,
        lower(
            table_schema
        ) as source_schema,
        lower(
            table_name
        ) as source_table,
        'stg_' || case
        source_schema
            when 'cytracom_core_core' then 'core'
            else source_schema
        end || '__' || lower(source_table) || '.sql' as target_name,
        upper(
            column_name
        ) as column_name,
        lower(data_type) as data_type
    from
        el_fivetran.information_schema.columns
    where
        table_schema in ('cytracom_core_core', 'zendesk')
),

src as (
    select
        source_db,
        source_schema,
        source_table,
        target_name,
        column_name,
        - 9 as ordinal_position,
        'with ' || lower(
            source_table
        ) || ' as (
    select *
    from  {{ source(' as 'fivetran_'   || case
      source_schema
      when 'cytracom_core_core' then 'core'
      else source_schema
    end || ''', ''' || lower(source_table) || ''') }}
)' as sql_text
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
            when ordinal_position = 1 then 'select '
            else ','
        end || case
            when
                data_type in (
                     'text', 'varchar', 'varying'
                ) then 'trim(' || '"' || column_name || '"' || ') as ' || '"' || column_name || '"'
            else '"' || column_name || '"'
        end as sql_text
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
        9999 as ordinal_position,
        ' from ' || lower(source_table) || ' ' as sql_text
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
        listagg(sql_text, '\n ') as within group (
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
        el_fivetran.information_schema.columns
),

yml_gen as (
    select
        table_schema,
        table_name,
        ordinal_position,
        '    - name: ' || column_name || '\n      description: tbd' as yml
    from
        columns
    where
        lower(table_schema) in ('zendesk', 'cytracom_core_core')
),

cte_yml as (
    select
        lower(table_schema) as source_schema,
        lower(table_name) as source_table,
        listagg(yml, '\n') as within group (
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
    'version: 2\n\nmodels:\n  - name: stg_core__customers\n    columns:\n' || yml_data as yml_data
from
    stg_view_gen
join cte_yml on stg_view_gen.source_schema = cte_yml.source_schema
        and stg_view_gen.source_table = cte_yml.source_table

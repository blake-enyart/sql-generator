with cte_yml as (
    select lower(table_schema) AS source_schema, 
            lower(table_name) AS source_table, 
            listagg(yml, '\n') WITHIN GROUP (ORDER BY ordinal_position) AS yml_data
    from prod_analytics.utils.yml_gen 
    GROUP BY 1,2
)
select target_name, stage_ddl, 
        'version: 2\n\nmodels:\n  - name: stg_core__customers\n    columns:\n' || yml_data as yml_data
from prod_analytics.utils.stg_view_gen stg
join cte_yml yml 
    on stg.source_schema = yml. source_schema
    and stg.source_table = yml.source_table
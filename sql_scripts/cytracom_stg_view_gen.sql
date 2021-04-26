with  join_filter as (
select 
   lower(all_columns.table_catalog) as source_db
  ,lower(all_columns.table_schema) as source_schema 
  ,lower(all_columns.table_name) as source_table
  ,'stg_' || CASE source_schema 
  		WHEN 'cytracom_core_core' THEN 'core' 
		ELSE source_schema 
  	end  ||'__'|| lower(source_table) || '.sql' as target_name
  ,UPPER(all_columns.column_name ) AS column_name
  ,all_columns.ordinal_position
  ,lower(all_columns.data_type) as data_type
  ,character_maximum_length
  ,numeric_precision
  ,numeric_scale
  ,datetime_precision
from EL_FIVETRAN.information_schema.columns   all_columns
 where table_schema IN ('CYTRACOM_CORE_CORE', 'ZENDESK')
),
src  as (
select source_db, source_schema, source_table, target_name, column_name, -9 ordinal_position,  
		'with ' || lower(source_table) || ' as (
    select *
    from  {{ source(''fivetran_' || CASE source_schema WHEN 'cytracom_core_core' THEN 'core' ELSE source_schema end  || ''', ''' || lower(source_table) || ''') }}
)'  as sql_text 
from join_filter where ordinal_position = 1
union all 
select 
 source_db
,source_schema 
,source_table
,target_name
,column_name
,ordinal_position
,case when ordinal_position =1 then 'SELECT ' else ',' end || 
 case when data_type in ('text','varchar','varying') then 'trim(' || '"' || column_name || '"'   || ') as ' || '"' || column_name || '"'  
 	ELSE '"' || column_name || '"'  
 END    AS sql_text
from join_filter 
union all 
select source_db, source_schema, source_table, target_name, column_name, 9999 ordinal_position, ' from ' || lower(source_table) ||  ' ' as sql_text 
from join_filter where ordinal_position = 1
)
, stg_view_gen as (
select src.source_db, src.source_schema, src.source_table, src.target_name,  
listagg(sql_text, '\n ') within group (order by ordinal_position) stage_ddl
from src 
group by source_db, source_schema, source_table, target_name
ORDER BY 2 DESC, 3 
)
, columns as (
    select * from EL_FIVETRAN.information_schema.columns
),
yml_gen as (
select table_schema, table_name, ordinal_position, 
'    - name: ' || column_name || '\n      description: tbd' yml 
from columns
where lower(table_schema) in ('zendesk','cytracom_core_core')
)
,cte_yml as (
    select lower(table_schema) AS source_schema, 
            lower(table_name) AS source_table, 
            listagg(yml, '\n') WITHIN GROUP (ORDER BY ordinal_position) AS yml_data
    from  yml_gen 
    GROUP BY 1,2
)
select target_name, stage_ddl, 
        'version: 2\n\nmodels:\n  - name: stg_core__customers\n    columns:\n' || yml_data as yml_data
from stg_view_gen stg
join cte_yml yml 
    on stg.source_schema = yml.source_schema
    and stg.source_table = yml.source_table
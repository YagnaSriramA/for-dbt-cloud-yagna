{% macro caribou_to_ods_layer_macro(src_schema,table_name) %}
{% set column_lst %}
        SELECT UPPER (COLUMN_NAME) AS COLUMN_NAME
        FROM {{source('info_schema','COLUMNS')}}
        WHERE TABLE_NAME = '{{table_name}}' AND TABLE_SCHEMA = '{{src_schema}}'
        AND COLUMN_NAME NOT LIKE 'ETL_%'
        ORDER BY ORDINAL_POSITION
{% endset %}
​
{% set result_sql = run_query(column_lst) %}
{% if execute %}
{# Return the first column #}
{% set results_list_col = result_sql.columns[0].values() %}
{% else %} {% set results_list_col = [] %}
{% endif %}
​
{% set all_column_lst %}
        SELECT UPPER (COLUMN_NAME) AS COLUMN_NAME
        FROM {{source('info_schema','COLUMNS')}}
        WHERE TABLE_NAME = '{{table_name}}' AND TABLE_SCHEMA = '{{src_schema}}'
        AND COLUMN_NAME NOT IN ('ETL_DELTAHASHKEY')
        ORDER BY ORDINAL_POSITION
{% endset %}
​
{% set result_sql_2 = run_query(all_column_lst) %}
{% if execute %}
{# Return the first column #}
{% set all_results_list_col = result_sql_2.columns[0].values() %}
{% else %} {% set all_results_list_col = [] %}
{% endif %}
{% set all_results_list_col_string = '"' + '", "'.join(all_results_list_col) + '"' %}
​
{% set hash_col %}
    {%- for sql_fld_col in results_list_col -%}
        {%- if loop.first -%} md5_binary(  {%- endif %} 
        coalesce(UPPER("{{sql_fld_col}}"), '#')
        {%- if not loop.last -%} ||  {%- endif %} 
        {%- if loop.last -%} ) {%- endif %} 
    {%- endfor %}
{% endset %}
​
​
{% set final_stmt %}
        select 
        {{hash_col}} ETL_DELTAHASHKEY , {{all_results_list_col_string}}
        from {{ source('caribou_to_ods_source', table_name ) }}  
{% endset %}
{{ return( final_stmt ) }}
{% endmacro %}
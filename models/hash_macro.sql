{% macro hash_macro(src_schema, table_name, hashing_name) %}
-- src_schema, table_name, hashing_name
{% set column_lst %}
        SELECT UPPER ({{hashing_name}}) AS COLUMN_NAMES
        FROM {{src_schema}}.{{table_name}}
{% endset %}

{%set result_sql = run_query(column_lst) %}
{% if execute %}
{# Return the first column #}
{% set results_list_col = result_sql.columns[0].values() %}
{% set result_cols = results_list_col[0].split(',')%}
{% else %} {% set results_list_col = [] %}
{% endif %}

{% set hash_col %}
    {%- for sql_fld_col in results_cols -%}
        {%- if loop.first -%} md5_binary(  {%- endif %} 
        coalesce(UPPER("{{sql_fld_col}}"), '#')
        {%- if not loop.last -%} ||  {%- endif %} 
        {%- if loop.last -%} ) {%- endif %} 
    {%- endfor %}
{% endset %}

{% set final_stmt %}
        select 
        {{hash_col}} {hashing_name} 
        from {{ source('caribou_to_ods_source', table_name ) }}  
{% endset %}
{{ return( final_stmt ) }}

{% endmacro %}
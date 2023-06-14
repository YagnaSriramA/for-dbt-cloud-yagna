{% macro fix_etl_id()%}
{%set date_arr%}
SELECT SPLIT(current_date, '-') 
{%endset%}

{{return (date_arr)}}

{%endmacro%}
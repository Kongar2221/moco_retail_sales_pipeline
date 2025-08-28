{% test row_count_at_least(model, arguments) %}
select 1
where (select count(*) 
from {{ model }}) < {{ arguments.min_rows }}
{% endtest %}

{% test non_negative(model, arguments) %}
select {{ arguments.column }} as value
from {{ model }}
where {{ arguments.column }} < 0
{% endtest %}

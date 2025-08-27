{% test unique_combo(model, combo_cols) %}
select
    {{ combo_cols | join(', ') }},
    count(*) as count
from {{ model }}
group by {{ combo_cols | join(', ') }}
having count(*) > 1
{% endtest %}
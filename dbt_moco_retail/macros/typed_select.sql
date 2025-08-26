{% macro moco_typed_select_from_stg() %}
  {% set cols = adapter.get_columns_in_relation(ref('stg_moco_retail')) %}

  select
  {%- for col in cols %}
    {%- set name = col.name %}
    {%- set lname = name | lower %}

    {%- if lname in ['year'] -%}
      try_cast({{ adapter.quote(name) }} as integer) as year
    {%- elif lname in ['month'] -%}
      try_cast({{ adapter.quote(name) }} as integer) as month
    {%- elif lname in ['units_sold','inventory_on_hand','qty','quantity'] -%}
      try_cast({{ adapter.quote(name) }} as bigint) as {{ lname }}
    {%- elif lname in ['sales_dollars','sales','amount','net_sales'] -%}
      try_cast({{ adapter.quote(name) }} as double) as {{ lname }}
    {%- else -%}
      {{ adapter.quote(name) }} as {{ lname }}
    {%- endif -%}
    {%- if not loop.last %}, {% endif %}
  {%- endfor %}
  from {{ ref('stg_moco_retail') }}
{% endmacro %}

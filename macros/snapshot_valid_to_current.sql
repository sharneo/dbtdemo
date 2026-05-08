{% macro snapshot_valid_to_current() %}
  {{ var('snapshot_max_date') }}
{% endmacro %}
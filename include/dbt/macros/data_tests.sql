{% macro matches_regex(value, regex) %}
    regexp_like(cast({{ value }} as varchar), '{{ regex }}')
{% endmacro %}


{% test regex_match(model, column_name, regex, row_condition=None) %}
select *
from {{ model }}
where
{% if row_condition %}
    ({{ row_condition }}) and
{% endif %}
    not {{ matches_regex(column_name, regex) }}
{% endtest %}

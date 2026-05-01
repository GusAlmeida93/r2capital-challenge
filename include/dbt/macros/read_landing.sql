{# Compile-time helpers that gracefully degrade when the landing folder is empty.
   Without this, DuckDB's read_csv() throws "No files found" — the production DAG
   short-circuits via check_landing, but ad-hoc `dbt build` calls still benefit
   from a typed empty result that lets every downstream model run cleanly. #}

{% macro landing_has_files(pattern) %}
    {%- if execute -%}
        {% set q %}
            select count(*) > 0 as has_files
            from glob('{{ var("landing_path") }}/{{ pattern }}')
        {% endset %}
        {% set result = run_query(q) %}
        {% if result and result.columns | length > 0 -%}
            {{ return(result.columns[0].values()[0]) }}
        {%- else -%}
            {{ return(false) }}
        {%- endif %}
    {%- else -%}
        {{ return(true) }}
    {%- endif %}
{% endmacro %}


{% macro read_landing_csv(pattern, columns, exclude_header_predicate=none) %}
    {% set has_files = landing_has_files(pattern) %}
    {% if has_files %}
        select
        {%- for col in columns.keys() %}
            cast({{ col }} as {{ columns[col] }}) as {{ col }}{{ "," if not loop.last else "" }}
        {%- endfor %},
            filename
        from read_csv(
            '{{ var("landing_path") }}/{{ pattern }}',
            columns={
        {%- for col, ctype in columns.items() %}
                '{{ col }}':'{{ ctype }}'{{ "," if not loop.last else "" }}
        {%- endfor %}
            },
            header=false,
            filename=true,
            union_by_name=true,
            ignore_errors=true
        )
        {% if exclude_header_predicate %}where not ({{ exclude_header_predicate }}){% endif %}
    {% else %}
        select
        {%- for col, ctype in columns.items() %}
            cast(null as {{ ctype }}) as {{ col }}{{ "," if not loop.last else "" }}
        {%- endfor %},
            cast(null as varchar) as filename
        where false
    {% endif %}
{% endmacro %}

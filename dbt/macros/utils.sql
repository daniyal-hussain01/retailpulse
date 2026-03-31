{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then null
        else {{ numerator }} / cast({{ denominator }} as double)
    end
{% endmacro %}


{% macro cents_to_dollars(cents_col) %}
    round({{ cents_col }} / 100.0, 2)
{% endmacro %}


{% macro assert_not_negative(col) %}
    case
        when {{ col }} < 0
        then null  -- coerce to null rather than fail; data quality suite catches it separately
        else {{ col }}
    end
{% endmacro %}

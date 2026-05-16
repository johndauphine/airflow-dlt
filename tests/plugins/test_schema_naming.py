import pytest

from airflow_dlt.schema_naming import derive_dataset_name


def test_basic_pattern():
    assert derive_dataset_name("dev", "StackOverflow2010", "dbo") == "dev_stackoverflow2010_dbo"


def test_lowercase_and_sanitize():
    # Single underscore separator; sanitized parts may themselves contain
    # underscores. The result matches what dlt's postgres destination
    # actually creates as the schema name.
    assert derive_dataset_name("Prod-East", "My.DB", "Sales-Reports") == (
        "prod_east_my_db_sales_reports"
    )


def test_underscores_preserved_in_parts():
    """Underscores inside parts survive — only the join separator changed."""
    assert derive_dataset_name("a_b", "c_d", "e_f") == "a_b_c_d_e_f"


def test_empty_part_rejected():
    with pytest.raises(ValueError):
        derive_dataset_name("", "db", "schema")
    with pytest.raises(ValueError):
        derive_dataset_name("alias", "---", "schema")


def test_no_schema_drops_segment():
    """Sources without a schema concept (MySQL, SQLite) → 2-segment name."""
    assert derive_dataset_name("dev", "shop", None) == "dev_shop"


def test_no_schema_still_validates_other_parts():
    with pytest.raises(ValueError):
        derive_dataset_name("", "db", None)
    with pytest.raises(ValueError):
        derive_dataset_name("alias", "---", None)

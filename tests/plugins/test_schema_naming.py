import pytest

from airflow_dlt.schema_naming import derive_dataset_name


def test_basic_pattern():
    assert derive_dataset_name("dev", "StackOverflow2010", "dbo") == "dev__stackoverflow2010__dbo"


def test_lowercase_and_sanitize():
    assert derive_dataset_name("Prod-East", "My.DB", "Sales-Reports") == (
        "prod_east__my_db__sales_reports"
    )


def test_underscores_preserved():
    assert derive_dataset_name("a_b", "c_d", "e_f") == "a_b__c_d__e_f"


def test_empty_part_rejected():
    with pytest.raises(ValueError):
        derive_dataset_name("", "db", "schema")
    with pytest.raises(ValueError):
        derive_dataset_name("alias", "---", "schema")

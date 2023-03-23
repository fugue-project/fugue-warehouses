from fugue_trino.collections import TableName
from pytest import raises


def test_table_name():
    r = TableName.parse(" a . b . c")
    assert str(r) == "a.b.c"
    r = TableName.parse(" b . c", default_catalog="a")
    assert str(r) == "a.b.c"
    r = TableName.parse(" a . b . c")
    assert str(r) == "a.b.c"
    r = TableName.parse("  c", default_catalog="a", default_schema="b")
    assert str(r) == "a.b.c"
    r = TableName.parse(
        " ", default_catalog="a", default_schema="b", table_func=lambda: "c"
    )
    assert str(r) == "a.b.c"
    r = TableName.parse(
        None, default_catalog="a", default_schema="b", table_func=lambda: "c"
    )
    assert str(r) == "a.b.c"
    r = TableName.parse(r)
    assert str(r) == "a.b.c"
    r = TableName.parse(("a", "b", "c"))
    assert str(r) == "a.b.c"
    r = TableName.parse(["a", "b", "c"])
    assert str(r) == "a.b.c"

    raises(ValueError, lambda: TableName.parse(123))
    raises(ValueError, lambda: TableName.parse(" b . c"))
    raises(ValueError, lambda: TableName.parse(" c", default_catalog="a"))
    raises(
        ValueError,
        lambda: TableName.parse(" ", default_catalog="a", default_schema="b"),
    )
    raises(
        ValueError,
        lambda: TableName.parse(("a", "b", "c", "d")),
    )

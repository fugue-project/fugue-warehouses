from fugue_bigquery._utils import is_select_query

def test_is_select_query():
    assert is_select_query("""
    
    SELECT
    a
    """)
    assert is_select_query("""
    
    WITH
    a
    """)
    assert not is_select_query("a.b.c")
    assert not is_select_query("select.b.c")
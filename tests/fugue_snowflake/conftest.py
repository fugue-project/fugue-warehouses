import pytest


@pytest.fixture(scope="module")
def snowflake_session():
    from fugue_snowflake.tester import SnowflakeTestBackend

    with SnowflakeTestBackend.generate_session_fixture() as session:
        yield session

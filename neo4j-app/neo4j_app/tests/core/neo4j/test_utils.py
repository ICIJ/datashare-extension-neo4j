from typing import Optional

import neo4j
import pytest

from neo4j_app.core.neo4j.utils import check_neo4j_database


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database,expected_error_msg",
    [("neo4j", None), ("not existing", 'Invalid neo4j database "not existing"')],
)
async def test_neo4j_database_exists(
    neo4j_test_driver_session: neo4j.AsyncNeo4jDriver,
    database: str,
    expected_error_msg: Optional[str],
):
    # Given
    driver = neo4j_test_driver_session

    # When/Then
    if expected_error_msg is not None:
        with pytest.raises(RuntimeError, match=expected_error_msg):
            await check_neo4j_database(driver, database)
    else:
        await check_neo4j_database(driver, database)

import neo4j

_SHOW_DBS = "SHOW DATABASE $db"


async def check_neo4j_database(driver: neo4j.AsyncDriver, database: str):
    res = await driver.execute_query(_SHOW_DBS, {"db": database})
    if not res.records:
        raise RuntimeError(f'Invalid neo4j database "{database}"')

from typing import Any, Mapping, Optional, Union

from elastic_transport import ObjectApiResponse

from neo4j_app.core.elasticsearch import ESClient

DEFAULT_SIZE = int(1e5)


async def search_documents(
    client: ESClient,
    query: Optional[Mapping[str, Any]] = None,
    *,
    scroll: str = DEFAULT_SCROLL_DURATION,
    size: Optional[int] = None,
    track_total_hits: Optional[Union[bool, int]] = None,
) -> ObjectApiResponse:
    return await client.search(
        index=client.index,
        scroll=scroll,
        query=query,
        track_total_hits=track_total_hits,
        size=size,
    )

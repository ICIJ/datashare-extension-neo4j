import logging
from typing import Any, AsyncGenerator, Dict, Mapping, Optional

from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_scan

logger = logging.getLogger(__name__)


class ESClient(AsyncElasticsearch):
    def __init__(self, project_index: str, **kwargs):
        self._project_index = project_index
        super().__init__(**kwargs)

    @property
    def project_index(self) -> str:
        return self._project_index

    async def async_scan(
        self,
        query: Optional[Mapping[str, Any]],
        *,
        scroll: str,
        scroll_size: int,
        **kwargs
    ) -> AsyncGenerator[Dict, None]:
        async for res in async_scan(
            self, query=query, scroll=scroll, size=scroll_size, **kwargs
        ):
            yield res

from typing import Dict, Any, Optional, List, AsyncGenerator
from sifthub.utils import httputil
from sifthub.configs.http_configs import (
    ANALYTICS_SERVICE_HOST, ANSWER_LIST_ENDPOINT, ANSWER_STATS_ENDPOINT,
    AUTOFILL_LIST_ENDPOINT, AUTOFILL_STATS_ENDPOINT, TEAMMATE_LIST_ENDPOINT, TEAMMATE_STATS_ENDPOINT
)
from sifthub.reporting.models.export_models import (
    FilterConditions, AnswerListResponse, AnswerStatsResponse,
    AutofillListResponse, AutofillStatsResponse, AITeammateListResponse, AITeammateStatsResponse
)
from sifthub.configs.constants import BATCH_SIZE
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class UsageLogsAnalyticsClient:
    """Client for calling usage logs analytics service APIs with complete pagination support"""
    
    def __init__(self):
        self.service_host = ANALYTICS_SERVICE_HOST

    # Answer APIs
    async def get_answer_logs(self, filter_conditions: Optional[FilterConditions] = None,
                             page_filter: Optional[FilterConditions] = None,
                             page: int = 1, page_size: int = BATCH_SIZE) -> Optional[AnswerListResponse]:
        """Fetch answer logs with pagination support"""
        try:
            endpoint = ANSWER_LIST_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching answer logs (page {page}, size {page_size})")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AnswerListResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch answer logs: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching answer logs: {e}", exc_info=True)
            return None

    async def get_answer_stats(self, filter_conditions: Optional[FilterConditions] = None,
                              page_filter: Optional[FilterConditions] = None) -> Optional[AnswerStatsResponse]:
        """Fetch answer stats (summary data)"""
        try:
            endpoint = ANSWER_STATS_ENDPOINT
            payload = {}
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching answer stats")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AnswerStatsResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch answer stats: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching answer stats: {e}", exc_info=True)
            return None

    async def get_answer_logs_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                     page_filter: Optional[FilterConditions] = None,
                                     batch_size: int = BATCH_SIZE) -> AsyncGenerator[AnswerListResponse, None]:
        """Generator for answer logs batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching answer logs batch {page}")
                
                response = await self.get_answer_logs(filter_conditions, page_filter, page, batch_size)
                if not response or not response.data:
                    logger.info(f"No more answer logs available. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.data) < batch_size:
                    logger.info(f"Last answer logs batch processed. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in answer logs batch generator: {e}", exc_info=True)
            return

    # Autofill APIs
    async def get_autofill_logs(self, filter_conditions: Optional[FilterConditions] = None,
                               page_filter: Optional[FilterConditions] = None,
                               page: int = 1, page_size: int = BATCH_SIZE) -> Optional[AutofillListResponse]:
        """Fetch autofill logs with pagination support"""
        try:
            endpoint = AUTOFILL_LIST_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching autofill logs (page {page}, size {page_size})")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AutofillListResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch autofill logs: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching autofill logs: {e}", exc_info=True)
            return None

    async def get_autofill_stats(self, filter_conditions: Optional[FilterConditions] = None,
                                page_filter: Optional[FilterConditions] = None) -> Optional[AutofillStatsResponse]:
        """Fetch autofill stats (summary data)"""
        try:
            endpoint = AUTOFILL_STATS_ENDPOINT
            payload = {}
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching autofill stats")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AutofillStatsResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch autofill stats: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching autofill stats: {e}", exc_info=True)
            return None

    async def get_autofill_logs_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                       page_filter: Optional[FilterConditions] = None,
                                       batch_size: int = BATCH_SIZE) -> AsyncGenerator[AutofillListResponse, None]:
        """Generator for autofill logs batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching autofill logs batch {page}")
                
                response = await self.get_autofill_logs(filter_conditions, page_filter, page, batch_size)
                if not response or not response.data:
                    logger.info(f"No more autofill logs available. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.data) < batch_size:
                    logger.info(f"Last autofill logs batch processed. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in autofill logs batch generator: {e}", exc_info=True)
            return

    # AITeammate APIs
    async def get_teammate_logs(self, filter_conditions: Optional[FilterConditions] = None,
                               page_filter: Optional[FilterConditions] = None,
                               page: int = 1, page_size: int = BATCH_SIZE) -> Optional[AITeammateListResponse]:
        """Fetch AI teammate logs with pagination support"""
        try:
            endpoint = TEAMMATE_LIST_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching AI teammate logs (page {page}, size {page_size})")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AITeammateListResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch AI teammate logs: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching AI teammate logs: {e}", exc_info=True)
            return None

    async def get_teammate_stats(self, filter_conditions: Optional[FilterConditions] = None,
                                page_filter: Optional[FilterConditions] = None) -> Optional[AITeammateStatsResponse]:
        """Fetch AI teammate stats (summary data)"""
        try:
            endpoint = TEAMMATE_STATS_ENDPOINT
            payload = {}
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching AI teammate stats")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return AITeammateStatsResponse(data=response["data"])
            
            logger.warning(f"Failed to fetch AI teammate stats: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching AI teammate stats: {e}", exc_info=True)
            return None

    async def get_teammate_logs_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                       page_filter: Optional[FilterConditions] = None,
                                       batch_size: int = BATCH_SIZE) -> AsyncGenerator[AITeammateListResponse, None]:
        """Generator for AI teammate logs batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching AI teammate logs batch {page}")
                
                response = await self.get_teammate_logs(filter_conditions, page_filter, page, batch_size)
                if not response or not response.data:
                    logger.info(f"No more AI teammate logs available. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.data) < batch_size:
                    logger.info(f"Last AI teammate logs batch processed. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in AI teammate logs batch generator: {e}", exc_info=True)
            return 
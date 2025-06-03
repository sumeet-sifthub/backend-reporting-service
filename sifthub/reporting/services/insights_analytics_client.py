from typing import Dict, Any, Optional, List
from sifthub.utils import httputil
from sifthub.configs.http_configs import (ANALYTICS_SERVICE_HOST, ANALYTICS_CONFIG_GENERATE_INFO_CARD_ENDPOINT,
    ANALYTICS_CONFIG_GENERATE_CATEGORY_DISTRIBUTION_ENDPOINT, ANALYTICS_CONFIG_GENERATE_SUB_CATEGORY_DISTRIBUTION_ENDPOINT,
    ANALYTICS_CONFIG_GENERATE_TOP_QUESTION_ENDPOINT)
from sifthub.reporting.models.export_models import (FilterConditions, InfoCardsData, CategoryDistributionResponse,
    SubCategoryDistributionResponse, TopQuestionsResponse
)
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class InsightsAnalyticsClient:
    """Client for calling insights analytics service APIs"""
    
    def __init__(self):
        self.service_host = ANALYTICS_SERVICE_HOST

    async def get_info_cards(self, page_filter: Optional[FilterConditions] = None) -> Optional[InfoCardsData]:
        """
        API 1: Fetch info cards data from insights service
        """
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_INFO_CARD_ENDPOINT
            payload = {}
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Calling info cards API with payload: {payload}")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return InfoCardsData(**response["data"])
            
            logger.warning(f"Failed to fetch info cards: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching info cards: {e}", exc_info=True)
            return None

    async def get_category_distribution(self, filter_conditions: Optional[FilterConditions] = None, 
                                      page_filter: Optional[FilterConditions] = None) -> Optional[CategoryDistributionResponse]:
        """
        API 2: Fetch category distribution data from insights service
        """
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_CATEGORY_DISTRIBUTION_ENDPOINT
            payload = {}
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Calling category distribution API with payload: {payload}")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return CategoryDistributionResponse(**response["data"])
            
            logger.warning(f"Failed to fetch category distribution: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching category distribution: {e}", exc_info=True)
            return None

    async def get_subcategory_distribution(self, category_id: str, 
                                         filter_conditions: Optional[FilterConditions] = None,
                                         page_filter: Optional[FilterConditions] = None) -> Optional[SubCategoryDistributionResponse]:
        """
        API 4: Fetch subcategory distribution data for a specific category
        """
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_SUB_CATEGORY_DISTRIBUTION_ENDPOINT.format(category_id=category_id)
            payload = {}
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Calling subcategory distribution API for category {category_id} with payload: {payload}")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return SubCategoryDistributionResponse(**response["data"])
            
            logger.warning(f"Failed to fetch subcategory distribution for category {category_id}: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching subcategory distribution for category {category_id}: {e}", exc_info=True)
            return None

    async def get_top_questions(self, filter_conditions: Optional[FilterConditions] = None,
                              page_filter: Optional[FilterConditions] = None,
                              page: int = 1, page_size: int = 100) -> Optional[TopQuestionsResponse]:
        """
        API 3: Fetch top questions data from insights service
        """
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_TOP_QUESTION_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Calling top questions API (page {page}, size {page_size}) with payload: {payload}")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return TopQuestionsResponse(**response["data"])
            
            logger.warning(f"Failed to fetch top questions: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching top questions: {e}", exc_info=True)
            return None

    async def get_all_top_questions(self, filter_conditions: Optional[FilterConditions] = None,
                                  page_filter: Optional[FilterConditions] = None,
                                  batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch all top questions by paginating through all pages
        """
        all_questions = []
        page = 1
        
        try:
            while True:
                response = await self.get_top_questions(filter_conditions, page_filter, page, batch_size)
                if not response or not response.topQuestions:
                    # No more data available
                    break
                
                current_batch = [q.dict() for q in response.topQuestions]
                all_questions.extend(current_batch)
                
                # If we got less than batch_size, we've reached the end
                if len(current_batch) < batch_size:
                    break
                
                page += 1
                
                # Safety check to prevent infinite loops
                if page > 1000:  # Reasonable upper limit
                    logger.warning(f"Reached maximum page limit (1000) while fetching questions")
                    break
            
            logger.info(f"Fetched {len(all_questions)} total questions across {page-1} pages")
            return all_questions
            
        except Exception as e:
            logger.error(f"Error fetching all top questions: {e}", exc_info=True)
            return [] 
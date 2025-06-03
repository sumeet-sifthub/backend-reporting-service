from typing import Dict, Any, Optional, List, AsyncGenerator
from sifthub.utils import httputil
from sifthub.configs.http_configs import (ANALYTICS_SERVICE_HOST, ANALYTICS_CONFIG_GENERATE_INFO_CARD_ENDPOINT,
    ANALYTICS_CONFIG_GENERATE_CATEGORY_DISTRIBUTION_ENDPOINT, ANALYTICS_CONFIG_GENERATE_SUB_CATEGORY_DISTRIBUTION_ENDPOINT,
    ANALYTICS_CONFIG_GENERATE_TOP_QUESTION_ENDPOINT)
from sifthub.reporting.models.export_models import (FilterConditions, InfoCardsData, CategoryDistributionResponse,
    SubCategoryDistributionResponse, TopQuestionsResponse
)
from sifthub.configs.constants import BATCH_SIZE
from sifthub.utils.logger import setup_logger

logger = setup_logger()


class InsightsAnalyticsClient:
    """Client for calling insights analytics service APIs with complete pagination support"""
    
    def __init__(self):
        self.service_host = ANALYTICS_SERVICE_HOST

    async def get_info_cards(self, filter_conditions: Optional[FilterConditions] = None,
                           page_filter: Optional[FilterConditions] = None,
                           page: int = 1, page_size: int = BATCH_SIZE) -> Optional[InfoCardsData]:
        """Fetch info cards data with pagination support"""
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_INFO_CARD_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching info cards (page {page}, size {page_size})")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return InfoCardsData(**response["data"])
            
            logger.warning(f"Failed to fetch info cards: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching info cards: {e}", exc_info=True)
            return None

    async def get_category_distribution(self, filter_conditions: Optional[FilterConditions] = None, 
                                      page_filter: Optional[FilterConditions] = None,
                                      page: int = 1, page_size: int = BATCH_SIZE) -> Optional[CategoryDistributionResponse]:
        """Fetch category distribution data with pagination support"""
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_CATEGORY_DISTRIBUTION_ENDPOINT
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching category distribution (page {page}, size {page_size})")
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
                                         page_filter: Optional[FilterConditions] = None,
                                         page: int = 1, page_size: int = BATCH_SIZE) -> Optional[SubCategoryDistributionResponse]:
        """Fetch subcategory distribution data with pagination support"""
        try:
            endpoint = ANALYTICS_CONFIG_GENERATE_SUB_CATEGORY_DISTRIBUTION_ENDPOINT.format(category_id=category_id)
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = filter_conditions.dict()
            
            if page_filter:
                payload["pageFilter"] = page_filter.dict()
            
            logger.info(f"Fetching subcategory distribution for category {category_id} (page {page}, size {page_size})")
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
                              page: int = 1, page_size: int = BATCH_SIZE) -> Optional[TopQuestionsResponse]:
        """Fetch top questions data with pagination support"""
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
            
            logger.info(f"Fetching top questions (page {page}, size {page_size})")
            response = await httputil.post(self.service_host, endpoint, payload)
            
            if response and response.get("status") == 200 and response.get("data"):
                return TopQuestionsResponse(**response["data"])
            
            logger.warning(f"Failed to fetch top questions: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching top questions: {e}", exc_info=True)
            return None

    async def get_info_cards_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                   page_filter: Optional[FilterConditions] = None,
                                   batch_size: int = BATCH_SIZE) -> AsyncGenerator[InfoCardsData, None]:
        """Generator for info cards batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching info cards batch {page}")
                
                response = await self.get_info_cards(filter_conditions, page_filter, page, batch_size)
                if not response:
                    logger.info(f"No more info cards available. Completed at page {page}")
                    break
                
                yield response
                
                # Check if this was the last batch (implement your completion logic)
                # For now, assuming info cards is typically single response
                break
                
        except Exception as e:
            logger.error(f"Error in info cards batch generator: {e}", exc_info=True)
            return

    async def get_category_distribution_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                              page_filter: Optional[FilterConditions] = None,
                                              batch_size: int = BATCH_SIZE) -> AsyncGenerator[CategoryDistributionResponse, None]:
        """Generator for category distribution batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching category distribution batch {page}")
                
                response = await self.get_category_distribution(filter_conditions, page_filter, page, batch_size)
                if not response or not response.category:
                    logger.info(f"No more categories available. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.category) < batch_size:
                    logger.info(f"Last category batch processed. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in category distribution batch generator: {e}", exc_info=True)
            return

    async def get_subcategory_distribution_batches(self, category_id: str,
                                                 filter_conditions: Optional[FilterConditions] = None,
                                                 page_filter: Optional[FilterConditions] = None,
                                                 batch_size: int = BATCH_SIZE) -> AsyncGenerator[SubCategoryDistributionResponse, None]:
        """Generator for subcategory distribution batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching subcategory distribution batch {page} for category {category_id}")
                
                response = await self.get_subcategory_distribution(category_id, filter_conditions, page_filter, page, batch_size)
                if not response or not response.subCategory:
                    logger.info(f"No more subcategories available for category {category_id}. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.subCategory) < batch_size:
                    logger.info(f"Last subcategory batch processed for category {category_id}. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in subcategory distribution batch generator for category {category_id}: {e}", exc_info=True)
            return

    async def get_top_questions_batches(self, filter_conditions: Optional[FilterConditions] = None,
                                      page_filter: Optional[FilterConditions] = None,
                                      batch_size: int = BATCH_SIZE) -> AsyncGenerator[TopQuestionsResponse, None]:
        """Generator for top questions batches"""
        page = 1
        
        try:
            while True:
                logger.info(f"Fetching questions batch {page}")
                
                response = await self.get_top_questions(filter_conditions, page_filter, page, batch_size)
                if not response or not response.topQuestions:
                    logger.info(f"No more questions available. Completed at page {page}")
                    break
                
                yield response
                
                if len(response.topQuestions) < batch_size:
                    logger.info(f"Last questions batch processed. Total pages: {page}")
                    break
                
                page += 1
                
                if page > 1000:
                    logger.warning(f"Reached maximum page limit (1000)")
                    break
            
        except Exception as e:
            logger.error(f"Error in questions batch generator: {e}", exc_info=True)
            return

    async def get_all_top_questions(self, filter_conditions: Optional[FilterConditions] = None,
                                  page_filter: Optional[FilterConditions] = None,
                                  batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
        """
        Fetch ALL top questions at once (use only for small datasets or when memory is not a concern)
        For large datasets, prefer get_top_questions_batches()
        """
        all_questions = []
        
        try:
            async for batch in self.get_top_questions_batches(filter_conditions, page_filter, batch_size):
                all_questions.extend([q.dict() for q in batch.topQuestions])
            
            logger.info(f"Fetched total {len(all_questions)} questions")
            return all_questions
            
        except Exception as e:
            logger.error(f"Error fetching all top questions: {e}", exc_info=True)
            return [] 
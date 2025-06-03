from typing import Dict, Any, Optional, List
from sifthub.utils import http_util
from sifthub.configs.app_config import app_config
from sifthub.reporting.models.export_models import (
    FilterConditions, InfoCardsData, CategoryDistributionResponse, 
    SubCategoryDistributionResponse, TopQuestionsResponse
)
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)


class InsightsAPIClient:
    def __init__(self):
        self.base_url = app_config.INSIGHTS_SERVICE_BASE_URL

    async def get_info_cards(self, page_filter: Optional[FilterConditions] = None) -> Optional[InfoCardsData]:
        """Fetch info cards data from insights service"""
        try:
            endpoint = "api/v1/insights-service/generate-answer/overview/info-cards"
            payload = {}
            
            if page_filter:
                payload["pageFilter"] = {
                    "conditions": {k: v.dict() for k, v in page_filter.conditions.items()},
                    "regex": page_filter.regex
                }
            
            response = await http_util.post(self.base_url, endpoint, payload)
            
            if response.get("status") == 200 and response.get("data"):
                return InfoCardsData(**response["data"])
            
            logger.warning(f"Failed to fetch info cards: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching info cards: {e}", exc_info=True)
            return None

    async def get_category_distribution(self, filter_conditions: Optional[FilterConditions] = None, 
                                      page_filter: Optional[FilterConditions] = None) -> Optional[CategoryDistributionResponse]:
        """Fetch category distribution data from insights service"""
        try:
            endpoint = "api/v1/insights-service/generate-answer/overview/category-distribution"
            payload = {}
            
            if filter_conditions:
                payload["filter"] = {
                    "conditions": {k: v.dict() for k, v in filter_conditions.conditions.items()},
                    "regex": filter_conditions.regex
                }
            
            if page_filter:
                payload["pageFilter"] = {
                    "conditions": {k: v.dict() for k, v in page_filter.conditions.items()},
                    "regex": page_filter.regex
                }
            
            response = await http_util.post(self.base_url, endpoint, payload)
            
            if response.get("status") == 200 and response.get("data"):
                return CategoryDistributionResponse(**response["data"])
            
            logger.warning(f"Failed to fetch category distribution: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching category distribution: {e}", exc_info=True)
            return None

    async def get_subcategory_distribution(self, category_id: str, 
                                         filter_conditions: Optional[FilterConditions] = None,
                                         page_filter: Optional[FilterConditions] = None) -> Optional[SubCategoryDistributionResponse]:
        """Fetch subcategory distribution data for a specific category"""
        try:
            endpoint = f"api/v1/insights-service/generate-answer/overview/category/{category_id}/subcategory-distribution"
            payload = {}
            
            if filter_conditions:
                payload["filter"] = {
                    "conditions": {k: v.dict() for k, v in filter_conditions.conditions.items()},
                    "regex": filter_conditions.regex
                }
            
            if page_filter:
                payload["pageFilter"] = {
                    "conditions": {k: v.dict() for k, v in page_filter.conditions.items()},
                    "regex": page_filter.regex
                }
            
            response = await http_util.post(self.base_url, endpoint, payload)
            
            if response.get("status") == 200 and response.get("data"):
                return SubCategoryDistributionResponse(**response["data"])
            
            logger.warning(f"Failed to fetch subcategory distribution for category {category_id}: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching subcategory distribution for category {category_id}: {e}", exc_info=True)
            return None

    async def get_top_questions(self, filter_conditions: Optional[FilterConditions] = None,
                              page_filter: Optional[FilterConditions] = None,
                              page: int = 1, page_size: int = 100) -> Optional[TopQuestionsResponse]:
        """Fetch top questions data from insights service"""
        try:
            endpoint = "api/v1/insights-service/generate-answer/overview/top-questions/list"
            payload = {
                "page": page,
                "pageSize": page_size
            }
            
            if filter_conditions:
                payload["filter"] = {
                    "conditions": {k: v.dict() for k, v in filter_conditions.conditions.items()},
                    "regex": filter_conditions.regex
                }
            
            if page_filter:
                payload["pageFilter"] = {
                    "conditions": {k: v.dict() for k, v in page_filter.conditions.items()},
                    "regex": page_filter.regex
                }
            
            response = await http_util.post(self.base_url, endpoint, payload)
            
            if response.get("status") == 200 and response.get("data"):
                return TopQuestionsResponse(**response["data"])
            
            logger.warning(f"Failed to fetch top questions: {response}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching top questions: {e}", exc_info=True)
            return None

    async def get_all_top_questions(self, filter_conditions: Optional[FilterConditions] = None,
                                  page_filter: Optional[FilterConditions] = None) -> List[Dict[str, Any]]:
        """Fetch all top questions by paginating through all pages"""
        all_questions = []
        page = 1
        page_size = 100
        
        try:
            while True:
                response = await self.get_top_questions(filter_conditions, page_filter, page, page_size)
                if not response or not response.topQuestions:
                    break
                
                all_questions.extend([q.dict() for q in response.topQuestions])
                
                # If we got less than page_size, we've reached the end
                if len(response.topQuestions) < page_size:
                    break
                
                page += 1
            
            logger.info(f"Fetched {len(all_questions)} total questions")
            return all_questions
            
        except Exception as e:
            logger.error(f"Error fetching all top questions: {e}", exc_info=True)
            return []


# Global insights API client instance
insights_api_client = InsightsAPIClient() 
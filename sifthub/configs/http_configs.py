import os

HTTP_PROTOCOL = os.environ.get("HTTP_PROTOCOL", "https://")

#Client Service Config
CLIENT_SERVICE_HOST = os.environ.get("CLIENT_SERVICE_HOST", "localhost:8086")
USER_ROLE_MAPPING_DATA_LOAD_CACHE_BY_ID_ENDPOINT = "/api/v1/product-service/access/cache/user-id/"
ANSWER_LIST_ENDPOINT = "/api/v1/analytics-service/usage-logs/answer/list"
ANSWER_STATS_ENDPOINT = "/api/v1/analytics-service/usage-logs/answer/stats"
AUTOFILL_LIST_ENDPOINT = "/api/v1/analytics-service/usage-logs/autofill/list"
AUTOFILL_STATS_ENDPOINT = "/api/v1/analytics-service/usage-logs/autofill/stats"
TEAMMATE_LIST_ENDPOINT = "/api/v1/analytics-service/usage-logs/teammate/list"
TEAMMATE_STATS_ENDPOINT = "/api/v1/analytics-service/usage-logs/teammate/stats"


# Analytics Service Config
ANALYTICS_SERVICE_HOST = os.environ.get("ANALYTICS_SERVICE_HOST", "localhost:8080")
ANALYTICS_CONFIG_GENERATE_INFO_CARD_ENDPOINT = "/api/v1/insights-service/generate-answer/overview/info-cards"
ANALYTICS_CONFIG_GENERATE_CATEGORY_DISTRIBUTION_ENDPOINT = "/api/v1/insights-service/generate-answer/overview/category-distribution"
ANALYTICS_CONFIG_GENERATE_TOP_QUESTION_ENDPOINT = "/api/v1/insights-service/generate-answer/overview/top-questions/list"
ANALYTICS_CONFIG_GENERATE_SUB_CATEGORY_DISTRIBUTION_ENDPOINT = "/api/v1/insights-service/generate-answer/overview/category/{id}/subcategory-distribution"
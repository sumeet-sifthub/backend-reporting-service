from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime


class ExportMode(str, Enum):
    DOWNLOAD = "download"
    EMAIL = "email"


class ExportModule(str, Enum):
    INSIGHTS = "insights"
    USAGE_LOGS = "usageLogs"


class InsightsType(str, Enum):
    RESPONSE_GENERATION = "responseGeneration"
    PROJECT_COLLABORATION = "projectCollaboration"
    AI_TEAMMATE = "AITeammate"


class UsageLogsType(str, Enum):
    ANSWER = "answer"
    AUTOFILL = "autofill"
    AI_TEAMMATE = "AITeammate"


class InsightsSubType(str, Enum):
    FREQUENT_ASKED_QUESTIONS = "frequentAskedQuestions"


class UsageLogsSubType(str, Enum):
    ALL = "all"


class ExportStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class FilterCondition(BaseModel):
    field: str
    data: str
    operation: str


class FilterConditions(BaseModel):
    conditions: Dict[str, FilterCondition]
    regex: str


class ExportFilter(BaseModel):
    filter: Optional[FilterConditions] = None
    pageFilter: Optional[FilterConditions] = None


class SQSExportMessage(BaseModel):
    eventId: str
    mode: ExportMode
    module: ExportModule
    type: str
    subType: str
    user_id: int
    clientId: int
    productId: int
    filter: Optional[FilterConditions] = None
    pageFilter: Optional[FilterConditions] = None


class ReportAuditLog(BaseModel):
    event_id: str
    client_id: str
    product_id: str
    user_id: str
    status: ExportStatus
    mode: ExportMode
    module: ExportModule
    type: str
    subType: str
    total_time: Optional[int] = None
    request_payload: Optional[Dict[str, Any]] = None
    s3_bucket: Optional[str] = None
    download_url: Optional[str] = None
    active: bool = True
    deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class CategoryData(BaseModel):
    id: str
    category: str
    distribution: float
    trend: float
    direction: str


class SubCategoryData(BaseModel):
    id: str
    subCategory: str
    distribution: float
    trend: float
    direction: str


class TopQuestionData(BaseModel):
    frequency: int
    question: str
    id: str


class InfoCardsData(BaseModel):
    totalQuestions: Dict[str, Any]
    totalQuestionsAnswered: Dict[str, Any]
    documentsAutofilled: Dict[str, Any]
    averageTime: Dict[str, Any]


class APIResponse(BaseModel):
    status: int
    message: str
    data: Any
    error: Optional[str] = None


class CategoryDistributionResponse(BaseModel):
    category: List[CategoryData]


class SubCategoryDistributionResponse(BaseModel):
    subCategory: List[SubCategoryData]


class TopQuestionsResponse(BaseModel):
    topQuestions: List[TopQuestionData] 
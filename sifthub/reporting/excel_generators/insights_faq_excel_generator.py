import time
from io import BytesIO
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, PatternFill, Alignment

from sifthub.reporting.models.export_models import SQSExportMessage
from sifthub.reporting.services.insights_analytics_client import InsightsAnalyticsClient
from sifthub.configs.constants import BATCH_SIZE
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)

# Module-level analytics client - more efficient than creating per request
_analytics_client = InsightsAnalyticsClient()


class InsightsFAQExcelGenerator:
    # Excel generator for insights FAQ reports
    
    async def generate_excel(self, message: SQSExportMessage) -> Optional[BytesIO]:
        # Generate Excel file for insights FAQ report
        try:
            logger.info(f"Starting FAQ Excel generation for event: {message.eventId}")
            # Fetch all required data from APIs
            data = await self._fetch_all_data(message)
            if not data:
                logger.error("Failed to fetch data from analytics APIs")
                return None
            wb = Workbook()
            # Determine sheet suffix based on filter
            sheet_suffix = self._get_sheet_suffix(message)
            
            # Create sheets based on requirements
            await self._create_top_categories_sheet(wb, data, sheet_suffix, message)
            await self._create_detailed_breakdown_sheet(wb, data, sheet_suffix, message)
            await self._create_top_questions_sheet(wb, data, sheet_suffix, message)
            
            # Remove default sheet if we created new ones
            if len(wb.worksheets) > 1 and wb.worksheets[0].title == "Sheet":
                wb.remove(wb.worksheets[0])
            
            # Save to memory stream
            stream = BytesIO()
            wb.save(stream)
            stream.seek(0)
            
            logger.info(f"Successfully generated FAQ Excel for event: {message.eventId}")
            return stream
            
        except Exception as e:
            logger.error(f"Error generating FAQ Excel: {e}", exc_info=True)
            return None
    
    async def _fetch_all_data(self, message: SQSExportMessage) -> Optional[Dict[str, Any]]:
        # Fetch all required data from analytics APIs
        try:
            # API 1: Get info cards data
            info_cards = await _analytics_client.get_info_cards(message.pageFilter)
            if not info_cards:
                logger.error("Failed to fetch info cards data")
                return None
            
            # API 2: Get category distribution  
            categories = await _analytics_client.get_category_distribution(
                message.filter, message.pageFilter
            )
            if not categories:
                logger.error("Failed to fetch category distribution")
                return None
            
            # API 3: Get all top questions (with pagination)
            top_questions = await self._fetch_all_top_questions(message)
            
            # API 4: Get subcategory data for each category
            subcategories = await self._fetch_all_subcategories(message, categories.category)
            
            return {
                "info_cards": info_cards,
                "categories": categories.category,
                "top_questions": top_questions,
                "subcategories": subcategories
            }
            
        except Exception as e:
            logger.error(f"Error fetching analytics data: {e}", exc_info=True)
            return None
    
    async def _fetch_all_top_questions(self, message: SQSExportMessage) -> List[Dict[str, Any]]:
        """Fetch all top questions with pagination"""
        all_questions = []
        page = 1
        
        try:
            while True:
                questions_response = await _analytics_client.get_top_questions(
                    message.filter, message.pageFilter, page, BATCH_SIZE
                )
                
                if not questions_response or not questions_response.topQuestions:
                    break
                
                all_questions.extend([q.dict() for q in questions_response.topQuestions])
                
                # If we got less than batch_size, we've reached the end
                if len(questions_response.topQuestions) < BATCH_SIZE:
                    break
                
                page += 1
            
            logger.info(f"Fetched {len(all_questions)} total questions")
            return all_questions
            
        except Exception as e:
            logger.error(f"Error fetching top questions: {e}", exc_info=True)
            return []
    
    async def _fetch_all_subcategories(self, message: SQSExportMessage, 
                                     categories: List[Any]) -> Dict[str, List[Any]]:
        """Fetch subcategories for all categories"""
        subcategories = {}
        
        try:
            for category in categories:
                category_id = category.id
                subcategory_response = await _analytics_client.get_subcategory_distribution(
                    category_id, message.filter, message.pageFilter
                )
                
                if subcategory_response and subcategory_response.subCategory:
                    subcategories[category_id] = [sub.dict() for sub in subcategory_response.subCategory]
                else:
                    subcategories[category_id] = []
            
            logger.info(f"Fetched subcategories for {len(categories)} categories")
            return subcategories
            
        except Exception as e:
            logger.error(f"Error fetching subcategories: {e}", exc_info=True)
            return {}
    
    async def _create_top_categories_sheet(self, wb: Workbook, data: Dict[str, Any], 
                                         sheet_suffix: str, message: SQSExportMessage):
        """Create the 'Top question categories' sheet"""
        try:
            ws = wb.create_sheet(f"Top question categories - {sheet_suffix}")
            
            # Sheet headers and metadata
            ws['A1'] = "Top question categories"
            ws['A2'] = f"Date range - {self._get_date_range_string(message.pageFilter)}"
            ws['A4'] = "Filters applied -"
            ws['A5'] = f"Users : (All, single user, or comma separated)"
            ws['A6'] = f"Initiated from : (All, single source, or comma separated)"
            
            # Column headers
            headers = ["Category", "Frequency (Questions asked)", "Distribution", "Trend", "Link"]
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=8, column=col)
                cell.value = header
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
            
            # Data rows
            row = 9
            for category in data["categories"]:
                frequency = self._calculate_frequency(data["info_cards"], category.distribution, message)
                
                ws.cell(row=row, column=1, value=category.category)
                ws.cell(row=row, column=2, value=frequency)
                ws.cell(row=row, column=3, value=f"{category.distribution:.2f}%")
                
                # Trend with direction
                trend_symbol = "▲" if category.direction == "INCREASING" else "▼"
                ws.cell(row=row, column=4, value=f"{trend_symbol} {abs(category.trend):.0f}%")
                ws.cell(row=row, column=5, value="View details ↗")
                row += 1
            
            # Auto-fit columns
            for col in range(1, 6):
                ws.column_dimensions[get_column_letter(col)].auto_size = True
                
        except Exception as e:
            logger.error(f"Error creating top categories sheet: {e}", exc_info=True)
    
    async def _create_detailed_breakdown_sheet(self, wb: Workbook, data: Dict[str, Any], 
                                             sheet_suffix: str, message: SQSExportMessage):
        """Create the 'Detailed category breakdown' sheet"""
        try:
            ws = wb.create_sheet(f"Detailed category breakdown - {sheet_suffix}")
            
            # Sheet headers and metadata
            ws['A1'] = "Detailed category breakdown"
            ws['A2'] = f"Date range - {self._get_date_range_string(message.pageFilter)}"
            ws['A4'] = "Filters applied -"
            ws['A5'] = f"Users : (All, single user, or comma separated)"
            ws['A6'] = f"Initiated from : (All, single source, or comma separated)"
            
            # Column headers
            headers = ["Subcategory", "Frequency (Questions asked)", "Distribution", "Trend", "Link"]
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=8, column=col)
                cell.value = header
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="FFB6C1", end_color="FFB6C1", fill_type="solid")
            
            # Data rows
            row = 9
            for category in data["categories"]:
                subcategories = data["subcategories"].get(category.id, [])
                for subcategory in subcategories:
                    frequency = self._calculate_frequency(data["info_cards"], subcategory["distribution"], message)
                    
                    # Add arrow to indicate subcategory
                    ws.cell(row=row, column=1, value=f"→ {subcategory['subCategory']}")
                    ws.cell(row=row, column=2, value=frequency)
                    ws.cell(row=row, column=3, value=f"{subcategory['distribution']:.2f}%")
                    
                    # Trend with direction
                    trend_symbol = "▲" if subcategory["direction"] == "INCREASING" else "▼"
                    ws.cell(row=row, column=4, value=f"{trend_symbol} {abs(subcategory['trend']):.0f}%")
                    
                    ws.cell(row=row, column=5, value="View details ↗")
                    
                    row += 1
            
            # Auto-fit columns
            for col in range(1, 6):
                ws.column_dimensions[get_column_letter(col)].auto_size = True
                
        except Exception as e:
            logger.error(f"Error creating detailed breakdown sheet: {e}", exc_info=True)
    
    async def _create_top_questions_sheet(self, wb: Workbook, data: Dict[str, Any], 
                                        sheet_suffix: str, message: SQSExportMessage):
        """Create the 'Top asked questions' sheet"""
        try:
            ws = wb.create_sheet(f"Top asked questions - {sheet_suffix}")
            
            # Sheet headers and metadata
            ws['A1'] = "Top asked questions"
            ws['A2'] = f"Date range - {self._get_date_range_string(message.pageFilter)}"
            ws['A3'] = "💡 Questions that are similar to each other have been grouped under a single FAQ"
            ws['A5'] = "Filters applied -"
            ws['A6'] = f"Users : (All, single user, or comma separated)"
            ws['A7'] = f"Initiated from : (All, single source, or comma separated)"
            
            # Column headers
            headers = ["Question", "Frequency (Questions asked)", "Link"]
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=9, column=col)
                cell.value = header
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="E6E6FA", end_color="E6E6FA", fill_type="solid")
            
            # Data rows
            row = 10
            for question in data["top_questions"]:
                ws.cell(row=row, column=1, value=question["question"])
                ws.cell(row=row, column=2, value=question["frequency"])
                ws.cell(row=row, column=3, value="View details ↗")
                
                row += 1
            
            # Auto-fit columns
            for col in range(1, 4):
                ws.column_dimensions[get_column_letter(col)].auto_size = True
            
            # Set question column width to be wider
            ws.column_dimensions['A'].width = 80
                
        except Exception as e:
            logger.error(f"Error creating top questions sheet: {e}", exc_info=True)
    
    def _calculate_frequency(self, info_cards: Any, distribution: float, message: SQSExportMessage) -> int:
        """Calculate frequency based on info cards data and distribution"""
        try:
            # Get total count based on filter type
            if message.filter and message.filter.conditions:
                status_condition = message.filter.conditions.get("status")
                if status_condition and status_condition.data:
                    data = status_condition.data
                    if "ANSWERED#@#NO_INFORMATION#@#PARTIAL" in data:
                        total_count = info_cards.totalQuestions.count
                    elif "ANSWERED#@#PARTIAL" in data:
                        total_count = info_cards.totalQuestionsAnswered.count
                    elif "NO_INFORMATION" in data:
                        total_count = info_cards.totalQuestions.count - info_cards.totalQuestionsAnswered.count
                    else:
                        total_count = info_cards.totalQuestions.count
                else:
                    total_count = info_cards.totalQuestions.count
            else:
                total_count = info_cards.totalQuestions.count
            
            # Calculate frequency: total_count * (distribution/100)
            frequency = int(total_count * (distribution / 100))
            return frequency
            
        except Exception as e:
            logger.error(f"Error calculating frequency: {e}", exc_info=True)
            return 0
    
    def _get_sheet_suffix(self, message: SQSExportMessage) -> str:
        """Get sheet suffix based on filter data"""
        try:
            if message.filter and message.filter.conditions:
                status_condition = message.filter.conditions.get("status")
                if status_condition and status_condition.data:
                    data = status_condition.data
                    if "ANSWERED#@#NO_INFORMATION#@#PARTIAL" in data:
                        return "All"
                    elif "ANSWERED#@#PARTIAL" in data:
                        return "Answered"
                    elif "NO_INFORMATION" in data:
                        return "Unanswered"
            return "All"
        except Exception:
            return "All"
    
    def _get_date_range_string(self, page_filter) -> str:
        """Extract date range from page filter"""
        try:
            if page_filter and page_filter.conditions:
                meta_created = page_filter.conditions.get("meta.created")
                if meta_created and meta_created.data:
                    # Parse timestamp range: "1746297000000#@#1748888999999"
                    timestamps = meta_created.data.split("#@#")
                    if len(timestamps) == 2:
                        start_ts = int(timestamps[0]) / 1000  # Convert to seconds
                        end_ts = int(timestamps[1]) / 1000
                        
                        start_date = datetime.fromtimestamp(start_ts).strftime("%b %d, %Y")
                        end_date = datetime.fromtimestamp(end_ts).strftime("%b %d, %Y")
                        
                        return f"{start_date} to {end_date}"
            
            return "Date range not specified"
            
        except Exception as e:
            logger.error(f"Error parsing date range: {e}")
            return "Date range not specified" 
import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from notion_client import Client

from .utils import handle_api_call, create_date_range, process_edt_code

logger = logging.getLogger(__name__)

class TaskOperations:
    def __init__(self, notion_client, config):
        self.notion = notion_client
        self._config = config
        self._property_types = config.get('property_types', {}).get('fields', {})
        self._field_mappings = config.get('field_mappings', {})
        self._status_mappings = config.get('status_config', {}).get('mappings', {})
        self._default_status = config.get('status_config', {}).get('default', 'Not started')
        self._rate_limit_delay = config.get('api_settings', {}).get('rate_limit_delay', 0.3)
        self._max_retries = config.get('api_settings', {}).get('max_retries', 3)
        self._retry_delay = config.get('api_settings', {}).get('retry_delay', 1.0)
        self._tasks_db = config.get('database_ids', {}).get('tasks', '')

    def _get_notion_field_name(self, excel_field: str) -> Optional[str]:
        """Get the Notion property name for an Excel field name"""
        for field_info in self._field_mappings.values():
            excel_name = field_info.get('excel')
            if isinstance(excel_name, list):
                # Handle composite fields
                if excel_field in excel_name:
                    return field_info.get('notion')
            elif excel_name == excel_field:
                return field_info.get('notion')
        return None

    def _prepare_task_properties(self, row: pd.Series) -> Dict[str, Any]:
        """Prepare task properties for Notion API"""
        properties = {}
        
        # Process each field based on its type
        for field_name, value in row.items():
            if pd.isna(value) or field_name not in self._property_types:
                continue

            prop_type = self._property_types[field_name]
            logger.info(f"[PROPERTY DEBUG] Processing field '{field_name}' with type '{prop_type}' and value '{value}'")
            
            if prop_type == "title":
                properties[field_name] = {'title': [{'text': {'content': str(value)}}]}
                
            elif prop_type == "status":
                status_str = str(value).strip()
                logger.debug(f"Processing status value: '{status_str}'")
                # Use the value directly since it was already mapped in the Excel processor
                properties[field_name] = {"status": {"name": status_str}}
                logger.debug(f"Set status property: {properties[field_name]}")
            
            elif prop_type == "select":
                select_value = str(value).strip() if pd.notna(value) else None
                if select_value:
                    properties[field_name] = {"select": {"name": select_value}}
            
            elif prop_type == "rich_text":
                text_value = str(value).strip() if pd.notna(value) else None
                if text_value:
                    properties[field_name] = {"rich_text": [{"text": {"content": text_value}}]}
            
            elif prop_type == "number":
                if pd.notna(value):
                    try:
                        num_value = float(value)
                        properties[field_name] = {"number": num_value}
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert {value} to number for field {field_name}")
            
            elif prop_type == "date":
                # Handle date fields
                if isinstance(value, dict) and ('start' in value or 'end' in value):
                    logger.debug(f"Processing date range for {field_name}: {value}")
                    date_dict = {}
                    if value.get('start'):
                        date_dict['start'] = value['start']
                    if value.get('end'):
                        date_dict['end'] = value['end']
                    if date_dict:
                        properties[field_name] = {"date": date_dict}
                        logger.debug(f"Set date property for {field_name}: {properties[field_name]}")
        
        logger.info(f"[TASK DEBUG] Final properties:")
        for field, prop in properties.items():
            logger.info(f"[TASK DEBUG] - {field}: {prop}")

        return properties

    def create_task_properties(self, task: pd.Series, project_id: str) -> Dict[str, Any]:
        """Create task properties for Notion API"""
        try:
            task_name = task[self._field_mappings["title"]["notion"]]
            logger.info(f"\n[TASK DEBUG] Creating properties for task: {task_name}")
            properties = self._prepare_task_properties(task)
            
            # Add project relation
            properties["Project"] = {"relation": [{"id": project_id}]}
            
            # Add parent task relation if exists
            parent_edt = task.get("Parent task")
            if parent_edt and pd.notna(parent_edt):
                parent_task = self.find_task_by_edt(parent_edt, project_id)
                if parent_task:
                    properties["Parent task"] = {"relation": [{"id": parent_task["id"]}]}
                else:
                    logger.warning(f"Parent task with EDT {parent_edt} not found for task {task_name}")
            
            return properties
        except Exception as e:
            logger.error(f"Error creating task properties: {str(e)}")
            raise

    def find_existing_task(self, task_name: str, edt: str, project_id: str) -> Optional[Dict[str, Any]]:
        """Find an existing task by name and EDT within a project"""
        try:
            # Build filter for exact task name, EDT, and project match
            response = handle_api_call(
                self.notion,
                self._rate_limit_delay,
                self._max_retries,
                self._retry_delay,
                self.notion.databases.query,
                database_id=self._tasks_db,
                filter={
                    "and": [
                        {
                            "property": self._field_mappings["title"]["notion"],
                            "title": {
                                "equals": task_name
                            }
                        },
                        {
                            "property": self._field_mappings["edt"]["notion"],
                            "rich_text": {
                                "equals": edt
                            }
                        },
                        {
                            "property": "Project",
                            "relation": {
                                "contains": project_id
                            }
                        }
                    ]
                }
            )
            
            if response['results']:
                logger.info(f"Found existing task: {task_name}")
                return response['results'][0]
            
            logger.info(f"No existing task found with name: {task_name}")
            return None
        except Exception as e:
            logger.error(f"Error checking for existing task: {str(e)}")
            return None

    def find_task_by_edt(self, edt: str, project_id: str) -> Optional[Dict[str, Any]]:
        """Find a task by its EDT within a project"""
        try:
            response = handle_api_call(
                self.notion,
                self._rate_limit_delay,
                self._max_retries,
                self._retry_delay,
                self.notion.databases.query,
                database_id=self._tasks_db,
                filter={
                    "and": [
                        {
                            "property": self._field_mappings["edt"]["notion"],
                            "rich_text": {
                                "equals": edt
                            }
                        },
                        {
                            "property": "Project",
                            "relation": {
                                "contains": project_id
                            }
                        }
                    ]
                }
            )
            
            if response['results']:
                return response['results'][0]
            return None
        except Exception as e:
            logger.error(f"Error finding task by EDT: {str(e)}")
            return None

    def find_task_by_name_and_project(self, task_name: str, project_id: str, task_data: Optional[pd.Series] = None) -> Optional[Dict[str, Any]]:
        """Find a task by its name, project ID, and other identifying fields."""
        try:
            # Base filter for task name and project
            filter_conditions = [
                {
                    "property": self._field_mappings["title"]["notion"],
                    "title": {
                        "equals": task_name
                    }
                },
                {
                    "property": "Project",
                    "relation": {
                        "contains": project_id
                    }
                }
            ]
            
            # Add EDT to filter if available
            if task_data is not None and pd.notna(task_data.get("EDT")):
                edt = str(task_data["EDT"])
                filter_conditions.append({
                    "property": self._field_mappings["edt"]["notion"],
                    "rich_text": {
                        "equals": edt
                    }
                })
                logger.info(f"Including EDT in search: {edt}")
            
            # Add Fase to filter if available
            if task_data is not None and pd.notna(task_data.get("Fase")):
                fase = str(task_data["Fase"]).strip()
                if fase:
                    filter_conditions.append({
                        "property": self._field_mappings["fase"]["notion"],
                        "select": {
                            "equals": fase
                        }
                    })
                    logger.info(f"Including Fase in search: {fase}")

            response = handle_api_call(
                self.notion,
                self._rate_limit_delay,
                self._max_retries,
                self._retry_delay,
                self.notion.databases.query,
                database_id=self._tasks_db,
                filter={"and": filter_conditions}
            )
            
            results = response.get("results", [])
            if results:
                return results[0]
            return None
            
        except Exception as e:
            logger.error(f"Error finding task: {str(e)}")
            return None

    def create_or_update_task(self, task: pd.Series, project_id: str) -> Optional[str]:
        """Create a new task or update existing task in Notion"""
        try:
            task_name = task[self._field_mappings["title"]["notion"]]
            edt = task.get(self._field_mappings["edt"]["notion"])
            
            # Check if task already exists
            existing_task = self.find_existing_task(task_name, edt, project_id) if edt else None
            
            # Create properties for the task
            properties = self.create_task_properties(task, project_id)
            
            if existing_task:
                # Update existing task
                try:
                    logger.info(f"Updating existing task: {task_name} (EDT: {edt})")
                    response = handle_api_call(
                        self.notion,
                        self._rate_limit_delay,
                        self._max_retries,
                        self._retry_delay,
                        self.notion.pages.update,
                        page_id=existing_task["id"],
                        properties=properties
                    )
                    logger.info(f"Successfully updated task: {task_name}")
                    return existing_task["id"]
                except Exception as e:
                    logger.error(f"Error updating task {task_name}: {str(e)}")
                    logger.error(f"Properties sent: {properties}")
                    raise e
            else:
                # Create new task
                try:
                    logger.info(f"Creating new task: {task_name} (EDT: {edt})")
                    response = handle_api_call(
                        self.notion,
                        self._rate_limit_delay,
                        self._max_retries,
                        self._retry_delay,
                        self.notion.pages.create,
                        parent={"database_id": self._tasks_db},
                        properties=properties
                    )
                    logger.info(f"Successfully created task: {task_name}")
                    return response["id"]
                except Exception as e:
                    logger.error(f"Error creating task {task_name}: {str(e)}")
                    logger.error(f"Properties sent: {properties}")
                    raise e
                    
        except Exception as e:
            logger.error(f"Error in create_or_update_task: {str(e)}")
            logger.error(f"Task data: {task}")
            raise e

    def verify_task_creation(self, task_name: str, project_id: str, task_data: Optional[pd.Series] = None) -> bool:
        """Verify if a task was actually created in Notion"""
        try:
            task = self.find_task_by_name_and_project(task_name, project_id, task_data)
            return task is not None
        except Exception as e:
            logger.error(f"Error verifying task creation: {str(e)}")
            return False

import logging
import os
import time
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import requests
from notion_client import Client
from config.config import NOTION_CONFIG, merge_config
from .projects import ProjectOperations
from .tasks import TaskOperations

logger = logging.getLogger(__name__)

class NotionOperator:
    def __init__(self, notion_token: str, config: Optional[Dict[str, Any]] = None):
        """Initialize the NotionOperator with token and optional config"""
        if not notion_token:
            raise ValueError("Notion token is required")
        
        # Initialize Notion client
        self.notion = Client(auth=notion_token)
        
        # Merge provided config with defaults
        self._config = merge_config(NOTION_CONFIG, config or {})
        
        # Initialize sub-operators
        self.projects = ProjectOperations(self.notion, self._config)
        self.tasks = TaskOperations(self.notion, self._config)
        
        # Validate required database IDs
        self._validate_config()
        
        logger.info("NotionOperator initialized successfully")

    def _validate_config(self):
        """Validate the configuration"""
        if not self._config['database_ids']['projects']:
            raise ValueError("Projects database ID is required in config")
        if not self._config['database_ids']['tasks']:
            raise ValueError("Tasks database ID is required in config")

    def process_project_data(self, project_info: Dict[str, Any], tasks_df: pd.DataFrame) -> Tuple[str, List[str]]:
        """Process project data and create project with tasks in Notion"""
        try:
            # Validate project info
            if not project_info or not isinstance(project_info, dict):
                raise ValueError("Invalid project info format")
            
            if not isinstance(tasks_df, pd.DataFrame) or tasks_df.empty:
                raise ValueError("Invalid tasks data format or empty DataFrame")
            
            # Create project with tasks
            project_id, task_ids = self.projects.create_project_with_tasks(project_info, tasks_df)
            
            logger.info(f"Successfully processed project {project_info.get('name', 'Unknown')}")
            logger.info(f"Created {len(task_ids)} tasks")
            
            return project_id, task_ids
            
        except Exception as e:
            logger.error(f"Error processing project data: {str(e)}")
            raise

    def verify_project_creation(self, project_name: str) -> bool:
        """Verify if a project was successfully created"""
        try:
            project = self.projects.find_existing_project(project_name)
            return project is not None
        except Exception as e:
            logger.error(f"Error verifying project creation: {str(e)}")
            return False

    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration"""
        return self._config.copy()

    def update_config(self, new_config: Dict[str, Any]) -> None:
        """Update the configuration"""
        self._config = merge_config(NOTION_CONFIG, new_config)
        self._validate_config()
        
        # Update sub-operators with new config
        self.projects = ProjectOperations(self.notion, self._config)
        self.tasks = TaskOperations(self.notion, self._config)
        
        logger.info("Configuration updated successfully")

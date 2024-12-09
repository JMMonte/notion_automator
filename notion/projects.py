import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from .utils import handle_api_call
from .tasks import TaskOperations

logger = logging.getLogger(__name__)

class ProjectOperations:
    def __init__(self, notion_client, config):
        self.notion = notion_client
        self._config = config
        self._projects_db = config.get('database_ids', {}).get('projects', '')
        self._rate_limit_delay = config.get('api_settings', {}).get('rate_limit_delay', 0.3)
        self._max_retries = config.get('api_settings', {}).get('max_retries', 3)
        self._retry_delay = config.get('api_settings', {}).get('retry_delay', 1.0)
        self._field_mappings = config.get('field_mappings', {})
        self.task_ops = TaskOperations(notion_client, config)

    def find_existing_project(self, project_name: str) -> Optional[Dict[str, Any]]:
        """Check if a project with the given name exists in Notion"""
        try:
            logger.info(f"Checking for existing project: {project_name}")
            response = handle_api_call(
                self.notion,
                self._rate_limit_delay,
                self._max_retries,
                self._retry_delay,
                self.notion.databases.query,
                database_id=self._projects_db,
                filter={
                    "property": self._field_mappings["title"]["notion"],  # Use mapped field name
                    "title": {
                        "equals": project_name
                    }
                }
            )
            
            if response['results']:
                logger.info(f"Found existing project: {project_name}")
                return response['results'][0]
            
            logger.info(f"No existing project found with name: {project_name}")
            return None
        except Exception as e:
            logger.error(f"Error checking for existing project: {str(e)}")
            return None

    def create_project(self, project_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new project in the Projects database"""
        try:
            if not project_info:
                project_info = {
                    'name': 'Untitled Project',
                    'id': 'PR.0000'
                }
            
            # Normalize project info keys and ensure they are strings
            project_name = str(project_info.get('name', 'Untitled Project')).strip()
            if not project_name:
                project_name = 'Untitled Project'
                
            project_id = str(project_info.get('id', 'PR.0000')).strip()
            if not project_id:
                project_id = 'PR.0000'
            
            logger.info(f"Creating project with name: {project_name} and ID: {project_id}")
            
            # Check if project already exists
            existing_project = self.find_existing_project(project_name)
            if existing_project:
                logger.info(f"Project already exists: {project_name}")
                return existing_project
            
            # Create new project if it doesn't exist
            try:
                properties = {
                    self._field_mappings["title"]["notion"]: {  # Use mapped field name
                        "title": [
                            {
                                "text": {
                                    "content": project_name
                                }
                            }
                        ]
                    },
                    self._field_mappings["status"]["notion"]: {  # Use mapped field name
                        "status": {
                            "name": self._config["status_config"]["default"]
                        }
                    },
                    "ID": {  # Project-specific field
                        "rich_text": [
                            {
                                "text": {
                                    "content": project_id
                                }
                            }
                        ]
                    }
                }

                response = handle_api_call(
                    self.notion,
                    self._rate_limit_delay,
                    self._max_retries,
                    self._retry_delay,
                    self.notion.pages.create,
                    parent={"database_id": self._projects_db},
                    properties=properties
                )
                
                if not response:
                    raise Exception("Notion API returned empty response")
                    
                logger.info(f"Created project: {project_name}")
                return response
                
            except Exception as e:
                error_msg = f"Failed to create project in Notion API: {str(e)}"
                if "Invalid request URL" in str(e):
                    error_msg += "\nPossible cause: Invalid database ID or insufficient permissions"
                elif "Could not find database" in str(e):
                    error_msg += f"\nDatabase ID used: {self._projects_db}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Error creating project {project_info.get('name', 'Unknown')}: {str(e)}")
            raise

    def create_project_with_tasks(self, project_info: Dict[str, Any], tasks_df: pd.DataFrame) -> Tuple[str, List[str]]:
        """Create a project and all its associated tasks"""
        try:
            # First create or get the project
            project = self.create_project(project_info)
            if not project:
                raise Exception(f"Failed to create project: {project_info['name']}")
            
            project_id = project['id']
            task_ids = []
            
            # Create each task
            for idx, task_data in tasks_df.iterrows():
                try:
                    task_id = self.task_ops.create_or_update_task(task_data, project_id)
                    if task_id:
                        task_ids.append(task_id)
                except Exception as e:
                    logger.error(f"Error creating task {task_data.get('Tarefa', 'Unknown')}: {str(e)}")
                    raise
            
            return project_id, task_ids
            
        except Exception as e:
            logger.error(f"Error in create_project_with_tasks: {str(e)}")
            raise

    def find_similar_projects(self, project_name: str, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Find projects with similar names or matching ID in Notion"""
        try:
            # If we have a project ID, try to find an exact ID match first
            if project_id:
                id_match = handle_api_call(
                    self.notion,
                    self._rate_limit_delay,
                    self._max_retries,
                    self._retry_delay,
                    self.notion.databases.query,
                    database_id=self._projects_db,
                    filter={
                        "property": "ID",
                        "rich_text": {
                            "equals": str(project_id)
                        }
                    }
                )
                
                if id_match.get('results'):
                    logger.info(f"Found exact project ID match: {project_id}")
                    return id_match.get('results')
            
            # Try exact name match
            exact_match = handle_api_call(
                self.notion,
                self._rate_limit_delay,
                self._max_retries,
                self._retry_delay,
                self.notion.databases.query,
                database_id=self._projects_db,
                filter={
                    "property": self._field_mappings["title"]["notion"],  # Use mapped field name
                    "title": {
                        "equals": project_name
                    }
                }
            )
            
            if exact_match.get('results'):
                logger.info(f"Found exact project name match: {project_name}")
                return exact_match.get('results')
            
            # If no exact matches, try similar names (but more strict than before)
            # Split project name into words and look for partial matches
            words = project_name.split()
            if len(words) > 1:  # Only look for similar if project name has multiple words
                main_words = [w for w in words if len(w) > 3]  # Filter out short words
                
                # Build OR filter for each main word
                or_filters = []
                for word in main_words:
                    or_filters.append({
                        "property": self._field_mappings["title"]["notion"],  # Use mapped field name
                        "title": {
                            "contains": word
                        }
                    })
                
                if or_filters:
                    response = handle_api_call(
                        self.notion,
                        self._rate_limit_delay,
                        self._max_retries,
                        self._retry_delay,
                        self.notion.databases.query,
                        database_id=self._projects_db,
                        filter={
                            "or": or_filters
                        }
                    )
                    
                    # Filter results to ensure they are actually similar
                    results = response.get('results', [])
                    similar_results = []
                    for result in results:
                        title = result.get('properties', {}).get(self._field_mappings["title"]["notion"], {}).get('title', [])
                        if title and isinstance(title, list) and title[0].get('text', {}).get('content'):
                            existing_name = title[0]['text']['content']
                            # Calculate similarity (number of matching words / total words)
                            existing_words = set(existing_name.lower().split())
                            project_words = set(project_name.lower().split())
                            common_words = existing_words.intersection(project_words)
                            similarity = len(common_words) / max(len(existing_words), len(project_words))
                            
                            # Only include if similarity is above threshold
                            if similarity > 0.7:  # Increased threshold for stricter matching
                                logger.info(f"Found similar project: {existing_name} (similarity: {similarity:.2f})")
                                similar_results.append(result)
                    
                    return similar_results
            
            logger.info(f"No matching projects found for name: {project_name}" + (f", ID: {project_id}" if project_id else ""))
            return []
            
        except Exception as e:
            logger.error(f"Error finding similar projects: {str(e)}")
            return []

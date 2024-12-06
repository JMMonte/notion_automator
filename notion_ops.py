from notion_client import Client
import time
from datetime import datetime
import logging
import pandas as pd
from excel_processor import ExcelProcessor
from typing import Dict, Optional, Any, List
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NotionOperator:
    DEFAULT_CONFIG = {
        "database_ids": {
            "projects": "",  # Must be provided in config
            "tasks": ""      # Must be provided in config
        },
        "status_config": {
            "default": "Not started",
            "mappings": {
                "Not started": "Not started",
                "In progress": "In progress",
                "Done": "Done",
                "Blocked": "Blocked"
            }
        },
        "property_types": {
            "title_field": "Tarefa",  # The field that will be used as the title
            "fields": {
                "Tarefa": "title",
                "Type": "select",
                "EDT": "rich_text",
                "Project": "relation",
                "Status": "status",
                "Fase": "multi_select",
                "Assignee": "people",
                "Datas planeadas": "date",
                "Datas reais": "date",
                "Progresso (dias)": "number"
            }
        },
        "api_settings": {
            "rate_limit_delay": 0.3,
            "max_retries": 3,
            "retry_delay": 1.0,
            "batch_size": 10
        },
        "search_settings": {
            "max_results": 100,
            "sort_direction": "descending"
        }
    }

    def __init__(self, token: str = None, config: Optional[Dict[str, Any]] = None):
        """Initialize NotionOperator with configuration"""
        # Store token if provided
        self._token = token
        self._config = None
        self._database_ids = None
        self._property_types = None
        self._title_field = None
        self._rate_limit_delay = None
        self._max_retries = None
        self._retry_delay = None
        self._batch_size = None
        self._default_status = None
        self._status_mappings = None
        
        # First merge and validate config
        self._config = self._merge_config(config or {})
        self._validate_config()
        
        # Initialize Notion client
        self.notion = Client(auth=self.get_notion_token())
        self.excel_processor = ExcelProcessor()
        
        # Initialize configuration values
        self._database_ids = self._config.get('database_ids', {})
        self._property_types = self._config.get('property_types', {}).get('fields', {})
        self._title_field = self._config.get('property_types', {}).get('title_field', 'Tarefa')
        
        # API settings
        api_settings = self._config.get('api_settings', {})
        self._rate_limit_delay = api_settings.get('rate_limit_delay', 0.5)
        self._max_retries = api_settings.get('max_retries', 3)
        self._retry_delay = api_settings.get('retry_delay', 1.0)
        self._batch_size = api_settings.get('batch_size', 10)
        
        # Status configuration
        status_config = self._config.get('status_config', {})
        self._default_status = status_config.get('default', 'Not started')
        self._status_mappings = {
            "Não iniciado": "Not started",
            "Em curso": "In progress",
            "Concluído": "Done",
            "Bloqueado": "Blocked",
            # Add default mappings as fallback
            "Not started": "Not started",
            "In progress": "In progress",
            "Done": "Done",
            "Blocked": "Blocked"
        }

    @property
    def database_ids(self):
        return self._database_ids
    
    @property
    def property_types(self):
        return self._property_types
    
    @property
    def title_field(self):
        return self._title_field
    
    @property
    def rate_limit_delay(self):
        return self._rate_limit_delay
    
    @property
    def max_retries(self):
        return self._max_retries
    
    @property
    def retry_delay(self):
        return self._retry_delay
    
    @property
    def batch_size(self):
        return self._batch_size
    
    @property
    def default_status(self):
        return self._default_status
    
    @property
    def status_mappings(self):
        return self._status_mappings

    def _merge_config(self, user_config: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge user config with default config"""
        merged = self.DEFAULT_CONFIG.copy()
        
        def deep_update(source: Dict, updates: Dict) -> Dict:
            for key, value in updates.items():
                if key in source and isinstance(source[key], dict) and isinstance(value, dict):
                    source[key] = deep_update(source[key], value)
                else:
                    source[key] = value
            return source
            
        return deep_update(merged, user_config)
        
    def _validate_config(self):
        """Validate the configuration"""
        # Check required database IDs
        if not self._config["database_ids"]["projects"]:
            raise ValueError("Projects database ID is required in configuration")
        if not self._config["database_ids"]["tasks"]:
            raise ValueError("Tasks database ID is required in configuration")
            
        # Validate title field exists in property types
        title_field = self._config["property_types"]["title_field"]
        if title_field not in self._config["property_types"]["fields"]:
            raise ValueError(f"Title field '{title_field}' must be defined in property_types.fields")
            
    @property
    def projects_db(self) -> str:
        """Get projects database ID"""
        return self._config["database_ids"]["projects"]
        
    @property
    def tasks_db(self) -> str:
        """Get tasks database ID"""
        return self._config["database_ids"]["tasks"]
        
    @property
    def default_status(self) -> str:
        """Get default status"""
        return self._config["status_config"]["default"]
        
    @property
    def status_mappings(self) -> Dict[str, str]:
        """Get status mappings"""
        return self._config["status_config"]["mappings"]
        
    @property
    def property_types(self) -> Dict[str, str]:
        """Get property type mappings"""
        return self._config["property_types"]["fields"]
        
    @property
    def rate_limit_delay(self) -> float:
        """Get API rate limit delay"""
        return self._config["api_settings"]["rate_limit_delay"]
        
    def _handle_api_call(self, api_func: callable, *args, **kwargs) -> Any:
        """Handle API calls with retries and rate limiting"""
        max_retries = self._config["api_settings"]["max_retries"]
        retry_delay = self._config["api_settings"]["retry_delay"]
        
        for attempt in range(max_retries):
            try:
                result = api_func(*args, **kwargs)
                time.sleep(self._rate_limit_delay)
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                logger.warning(f"API call failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
    def extract_project_info(self, excel_file):
        """Extract project information from the Excel file"""
        return self.excel_processor.extract_project_info(excel_file)

    def find_existing_project(self, project_name):
        """Check if a project with the given name exists in Notion"""
        try:
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.projects_db,
                filter={
                    "property": "Project name",
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

    def create_project(self, project_info):
        """Create a new project in the Projects database"""
        try:
            if not project_info or not project_info.get('name'):
                raise ValueError("Project info is missing or invalid: No project name provided")
            
            if not project_info.get('id'):
                raise ValueError("Project info is missing or invalid: No project ID provided")
            
            # Check if project already exists
            existing_project = self.find_existing_project(project_info['name'])
            if existing_project:
                logger.info(f"Project already exists: {project_info['name']}")
                return existing_project
            
            # Create new project if it doesn't exist
            try:
                properties = {
                    "Project name": {"title": [{"text": {"content": project_info['name']}}]},
                    "Status": {"status": {"name": "Not started"}},
                    "ID": {"rich_text": [{"text": {"content": str(project_info['id'])}}]},
                }

                response = self._handle_api_call(self.notion.pages.create,
                    parent={"database_id": self.projects_db},
                    properties=properties
                )
                logger.info(f"Created project: {project_info['name']}")
                return response
            except Exception as e:
                error_msg = f"Failed to create project in Notion API: {str(e)}"
                if "Invalid request URL" in str(e):
                    error_msg += "\nPossible cause: Invalid database ID or insufficient permissions"
                elif "Could not find database" in str(e):
                    error_msg += f"\nDatabase ID used: {self.projects_db}"
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Error creating project {project_info.get('name', 'Unknown')}: {str(e)}")
            raise Exception(f"Project creation failed: {str(e)}")

    def task_exists(self, task_name, project_id):
        """Check if a task with the same name already exists in the project
        Returns: None if task doesn't exist, or (task_id, existing_properties) if it does"""
        try:
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.tasks_db,
                filter={
                    "and": [
                        {
                            "property": "Tarefa",
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
                }
            )
            if len(response["results"]) > 0:
                task = response["results"][0]
                return task["id"], task["properties"]
            return None
        except Exception as e:
            logger.error(f"Error checking for existing task: {str(e)}")
            return None

    def create_or_update_task(self, task_data, project_id):
        """Create a new task in the Tasks database or update if it exists"""
        try:
            task_name = task_data["Tarefa"]
            task_type = task_data["Type"]
            edt = str(task_data["EDT"]) if pd.notna(task_data["EDT"]) else ""
            
            logger.info(f"Processing task: {task_name} (EDT: {edt}, Type: {task_type})")
            
            # Initialize properties with required fields
            properties = {
                "Tarefa": {"title": [{"text": {"content": task_name}}]},
                "Type": {"select": {"name": task_type}},
                "EDT": {"rich_text": [{"text": {"content": edt}}]},
                "Project": {"relation": [{"id": project_id}]},
            }

            # Add optional properties based on property types
            for field, value in task_data.items():
                if field in self._property_types and pd.notna(value) and field not in properties:
                    prop_type = self._property_types[field]
                    
                    if prop_type == "status":
                        # Handle status with mappings
                        status_str = str(value).strip()
                        mapped_status = self._status_mappings.get(status_str, self._default_status)
                        logger.info(f"Mapping status for {task_name}: '{status_str}' -> '{mapped_status}'")
                        properties[field] = {"status": {"name": mapped_status}}
                    
                    elif prop_type == "multi_select":
                        # Handle multi-select (e.g., Fase)
                        values = [{"name": v.strip()} for v in str(value).split(",") if v.strip()]
                        if values:
                            properties[field] = {"multi_select": values}
                    
                    elif prop_type == "date":
                        # Handle dates (Datas planeadas and Datas reais)
                        try:
                            if pd.notna(value) and isinstance(value, dict):
                                date_property = {"date": {}}
                                logger.info(f"Processing {field} for task '{task_name}':")
                                logger.info(f"Raw date value: {value}")
                                
                                # Add start date if present
                                if value.get("start"):
                                    start_date = value["start"]
                                    if isinstance(start_date, str):
                                        try:
                                            # Parse the date and format it for Notion (date only)
                                            parsed_date = datetime.strptime(start_date, "%Y-%m-%d")
                                            # Format as ISO 8601 date only
                                            start_iso = parsed_date.strftime('%Y-%m-%d')
                                            date_property["date"]["start"] = start_iso
                                            logger.info(f"Formatted start date for {field}: {start_iso}")
                                        except ValueError as ve:
                                            logger.error(f"Invalid start date format for {field}: {start_date}. Error: {ve}")
                                    else:
                                        logger.error(f"Start date for {field} is not a string: {type(start_date)}")
                                
                                # Add end date if present
                                if value.get("end"):
                                    end_date = value["end"]
                                    if isinstance(end_date, str):
                                        try:
                                            # Parse the date and format it for Notion (date only)
                                            parsed_date = datetime.strptime(end_date, "%Y-%m-%d")
                                            # Format as ISO 8601 date only
                                            end_iso = parsed_date.strftime('%Y-%m-%d')
                                            date_property["date"]["end"] = end_iso
                                            logger.info(f"Formatted end date for {field}: {end_iso}")
                                        except ValueError as ve:
                                            logger.error(f"Invalid end date format for {field}: {end_date}. Error: {ve}")
                                    else:
                                        logger.error(f"End date for {field} is not a string: {type(end_date)}")
                                
                                if date_property["date"]:  # Only add if we have at least one valid date
                                    properties[field] = date_property
                                    logger.info(f"Final date property for {field}: {date_property}")
                                else:
                                    logger.warning(f"No valid dates found for {field}")
                            else:
                                logger.warning(f"Invalid date value for {field}: {value} (type: {type(value)})")
                            
                        except Exception as e:
                            logger.error(f"Error processing date for {field}: {str(e)}", exc_info=True)
                    
                    elif prop_type == "people":
                        # Handle people (e.g., Assignee)
                        if value:
                            # Split multiple assignees if comma-separated
                            assignees = [name.strip() for name in str(value).split(",")]
                            user_ids = []
                            for assignee in assignees:
                                user_id = self.get_user_id(assignee)
                                if user_id:
                                    user_ids.append({"object": "user", "id": user_id})
                            if user_ids:
                                properties[field] = {"people": user_ids}
                    
                    elif prop_type == "select":
                        # Handle single select
                        properties[field] = {"select": {"name": str(value)}}
                    
                    elif prop_type == "rich_text":
                        # Handle rich text
                        properties[field] = {"rich_text": [{"text": {"content": str(value)}}]}
                    
                    elif prop_type == "number":
                        # Handle number fields (like Progresso (dias))
                        try:
                            numeric_value = float(value) if pd.notna(value) else None
                            if numeric_value is not None:
                                properties[field] = {"number": numeric_value}
                                logger.info(f"Added number value for {field}: {numeric_value}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Invalid number value for {field}: {value} - {str(e)}")
                    
            # Add default status if not set
            if "Status" not in properties:
                logger.info(f"Using default status '{self._default_status}' for task: {task_name}")
                properties["Status"] = {"status": {"name": self._default_status}}

            # Debug log for properties
            logger.info(f"Final properties for task '{task_name}':")
            for field, value in properties.items():
                logger.info(f"  {field}: {value}")

            # Check if task already exists
            existing_task = self.find_task_by_name_and_project(task_name, project_id, task_data)
            
            if existing_task:
                # Update existing task
                try:
                    logger.info(f"Updating existing task: {task_name}")
                    response = self._handle_api_call(self.notion.pages.update,
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
                    logger.info(f"Creating new task: {task_name}")
                    response = self._handle_api_call(self.notion.pages.create,
                        parent={"database_id": self.tasks_db},
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
            logger.error(f"Task data: {task_data}")
            raise e

    def find_task_by_name_and_project(self, task_name, project_id, task_data=None):
        """Find a task by its name, project ID, and other identifying fields."""
        try:
            # Base filter for task name and project
            filter_conditions = [
                {
                    "property": "Tarefa",
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
                    "property": "EDT",
                    "rich_text": {
                        "equals": edt
                    }
                })
                logger.info(f"Including EDT in search: {edt}")
            
            # Add Fase to filter if available
            if task_data is not None and pd.notna(task_data.get("Fase")):
                fase_values = [v.strip() for v in str(task_data["Fase"]).split(",")] if pd.notna(task_data.get("Fase")) else []
                for fase in fase_values:
                    filter_conditions.append({
                        "property": "Fase",
                        "multi_select": {
                            "contains": fase
                        }
                    })
                logger.info(f"Including Fase in search: {fase_values}")

            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.tasks_db,
                filter={
                    "and": filter_conditions
                }
            )
            
            results = response.get("results", [])
            if len(results) > 1:
                logger.warning(f"Found multiple tasks matching name '{task_name}', EDT, and Fase in project.")
                logger.warning("Task details:")
                for task in results:
                    props = task["properties"]
                    edt_value = props.get("EDT", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "N/A")
                    fase_values = [item.get("name", "") for item in props.get("Fase", {}).get("multi_select", [])]
                    type_value = props.get("Type", {}).get("select", {}).get("name", "N/A")
                    logger.warning(f"- ID: {task['id']}, EDT: {edt_value}, Fase: {fase_values}, Type: {type_value}")
            elif len(results) == 1:
                task = results[0]
                props = task["properties"]
                edt_value = props.get("EDT", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "N/A")
                fase_values = [item.get("name", "") for item in props.get("Fase", {}).get("multi_select", [])]
                logger.info(f"Found matching task - EDT: {edt_value}, Fase: {fase_values}")
            else:
                logger.info(f"No matching task found with name '{task_name}' and matching EDT/Fase")
            
            return results[0] if results else None
            
        except Exception as e:
            logger.error(f"Error finding task: {str(e)}")
            return None

    def find_tasks_batch(self, tasks_to_find):
        """Find multiple tasks in a single query"""
        if not tasks_to_find:
            return {}
        
        try:
            # Create an OR filter for all tasks
            or_conditions = []
            for task_name, project_id, task_data in tasks_to_find:
                # Base conditions for task name and project
                and_conditions = [
                    {
                        "property": "Tarefa",
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
                
                # Add EDT condition if available
                if task_data is not None and pd.notna(task_data.get("EDT")):
                    edt = str(task_data["EDT"])
                    and_conditions.append({
                        "property": "EDT",
                        "rich_text": {
                            "equals": edt
                        }
                    })
                
                # Add Fase conditions if available
                if task_data is not None and pd.notna(task_data.get("Fase")):
                    fase_values = [v.strip() for v in str(task_data["Fase"]).split(",")] if pd.notna(task_data.get("Fase")) else []
                    for fase in fase_values:
                        and_conditions.append({
                            "property": "Fase",
                            "multi_select": {
                                "contains": fase
                            }
                        })
                
                or_conditions.append({"and": and_conditions})
            
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.tasks_db,
                filter={
                    "or": or_conditions
                }
            )
            
            # Create a mapping of task identifiers to found tasks
            found_tasks = {}
            for result in response.get("results", []):
                task_name = result["properties"]["Tarefa"]["title"][0]["text"]["content"]
                project_ids = [rel["id"] for rel in result["properties"]["Project"]["relation"]]
                edt = result["properties"].get("EDT", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
                fase_values = [item.get("name", "") for item in result["properties"].get("Fase", {}).get("multi_select", [])]
                
                for project_id in project_ids:
                    key = (task_name, project_id, edt, ",".join(sorted(fase_values)))
                    found_tasks[key] = result
            
            return found_tasks
            
        except Exception as e:
            logger.error(f"Error in batch task search: {str(e)}")
            return {}

    def create_project_with_tasks(self, project_info, tasks_df):
        """Create a project and all its associated tasks"""
        try:
            # Create project first
            project_id = self.create_project(project_info)
            if not project_id:
                raise Exception("Failed to create project")

            total_tasks = len(tasks_df)
            created_count = 0
            updated_count = 0
            failed_count = 0
            verification_failed_count = 0
            skipped_count = 0

            logger.info(f"Processing {total_tasks} tasks for project")
            
            # Batch size for operations
            batch_size = self._config["api_settings"].get("batch_size", 10)
            
            # Process tasks in batches
            for batch_start in range(0, total_tasks, batch_size):
                batch_end = min(batch_start + batch_size, total_tasks)
                batch_df = tasks_df.iloc[batch_start:batch_end]
                
                # Prepare batch data
                tasks_to_find = []
                for _, task in batch_df.iterrows():
                    task_name = task.get('Tarefa', 'Unnamed Task')
                    tasks_to_find.append((task_name, project_id, task))
                
                # Find existing tasks in batch
                existing_tasks = self.find_tasks_batch(tasks_to_find)
                
                # Process each task in the batch
                tasks_to_verify = []
                for _, task in batch_df.iterrows():
                    try:
                        task_name = task.get('Tarefa', 'Unnamed Task')
                        
                        # Create task properties
                        task_properties = self.create_task_properties(task, project_id)
                        
                        # Check if task exists with matching EDT and Fase
                        edt = str(task.get("EDT", "")) if pd.notna(task.get("EDT")) else ""
                        fase_values = [v.strip() for v in str(task.get("Fase", "")).split(",")] if pd.notna(task.get("Fase")) else []
                        task_key = (task_name, project_id, edt, ",".join(sorted(fase_values)))
                        
                        existing_task = existing_tasks.get(task_key)
                        
                        if existing_task:
                            # Compare and update if needed
                            should_update = False
                            existing_props = existing_task["properties"]
                            
                            # Compare Type (EDT and Fase already matched)
                            existing_type = existing_props.get("Type", {}).get("select", {}).get("name", "")
                            new_type = str(task.get("Type", "")) if pd.notna(task.get("Type")) else ""
                            
                            if existing_type != new_type:
                                should_update = True
                                logger.info(f"Changes detected for task {task_name}:")
                                logger.info(f"  - Type: {existing_type} -> {new_type}")
                            
                            if should_update:
                                try:
                                    logger.info(f"Updating existing task: {task_name}")
                                    response = self._handle_api_call(self.notion.pages.update,
                                        page_id=existing_task["id"],
                                        properties=task_properties
                                    )
                                    tasks_to_verify.append((task_name, project_id, task))
                                except Exception as e:
                                    failed_count += 1
                                    logger.error(f"Error updating task {task_name}: {str(e)}")
                                    continue
                            else:
                                skipped_count += 1
                                logger.info(f"Skipping task {task_name} - no changes needed")
                        else:
                            # Create new task
                            try:
                                logger.info(f"Creating new task: {task_name}")
                                response = self._handle_api_call(self.notion.pages.create,
                                    parent={"database_id": self.tasks_db},
                                    properties=task_properties
                                )
                                tasks_to_verify.append((task_name, project_id, task))
                            except Exception as e:
                                failed_count += 1
                                logger.error(f"Error creating task {task_name}: {str(e)}")
                                continue
                    
                    except Exception as task_error:
                        failed_count += 1
                        logger.error(f"Error processing task: {str(task_error)}")
                        continue
                
                # Verify all tasks in the batch
                if tasks_to_verify:
                    verification_results = self.verify_tasks_batch(tasks_to_verify)
                    for (task_name, proj_id), verified in verification_results.items():
                        if verified:
                            if (task_name, proj_id, _) in tasks_to_verify[:created_count]:
                                created_count += 1
                            else:
                                updated_count += 1
                        else:
                            verification_failed_count += 1
                            logger.error(f"Task verification failed: {task_name}")
                
                # Add a small delay between batches to respect rate limits
                if batch_end < total_tasks:
                    time.sleep(self._rate_limit_delay)

            # Log final statistics
            logger.info(f"""Task Processing Summary:
                Total Tasks: {total_tasks}
                Created: {created_count}
                Updated: {updated_count}
                Failed: {failed_count}
                Verification Failed: {verification_failed_count}
                Skipped: {skipped_count}
            """)
            
            return project_id
            
        except Exception as e:
            logger.error(f"Error in create_project_with_tasks: {str(e)}")
            raise e

    def find_similar_projects(self, project_name):
        """Find projects with similar names in Notion"""
        try:
            # First, get all projects from the database
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.projects_db,
                filter={
                    "property": "Project name",
                    "title": {
                        "contains": project_name.split()[0]  # Search by first word to find similar names
                    }
                }
            )
            
            # Extract project names and IDs
            similar_projects = []
            for result in response['results']:
                name = result['properties']['Project name']['title'][0]['text']['content'] if result['properties']['Project name']['title'] else "Untitled"
                similar_projects.append({
                    'name': name,
                    'id': result['id'],
                    'url': result['url']
                })
            
            logger.info(f"Found {len(similar_projects)} similar projects")
            return similar_projects
        except Exception as e:
            logger.error(f"Error fetching similar projects: {str(e)}")
            return []

    def get_all_project_names(self):
        """Fetch all project names from Notion"""
        try:
            # Get all projects from the database
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.projects_db
            )
            
            # Extract project names
            project_names = []
            for result in response['results']:
                name = result['properties']['Project name']['title'][0]['text']['content'] if result['properties']['Project name']['title'] else "Untitled"
                project_names.append(name)
            
            logger.info(f"Fetched {len(project_names)} project names")
            return project_names
        except Exception as e:
            logger.error(f"Error fetching project names: {str(e)}")
            return []

    def get_database_structure(self, database_id):
        """Get the structure of a Notion database."""
        try:
            response = self._handle_api_call(self.notion.databases.retrieve,
                database_id=database_id
            )
            return response['properties']
        except Exception as e:
            logger.error(f"Error getting database structure: {str(e)}")
            raise e

    def compare_database_structure(self, dataframe):
        """
        Compare the structure of the input dataframe with the Notion database structure.
        Returns a list of dictionaries containing the comparison results.
        """
        try:
            # Get Notion database structure
            notion_structure = self.get_database_structure(self.tasks_db)
            
            # Initialize results list
            comparison_results = []
            
            # Compare each Notion property with dataframe columns
            for prop_name, prop_info in notion_structure.items():
                matching_cols = [col for col in dataframe.columns if col.lower().replace(" ", "") == prop_name.lower().replace(" ", "")]
                
                # Get property type and options
                prop_type = prop_info.get('type', 'unknown')
                options = []
                
                # Handle different property types
                if prop_type in ['select', 'multi_select', 'status']:
                    type_config = prop_info.get(prop_type, {})
                    if 'options' in type_config:
                        options = [opt.get('name') for opt in type_config['options']]
                
                result = {
                    "Notion Property": prop_name,
                    "Property Type": prop_type,
                    "Available Options": options,
                    "Status": "✅ Matched" if matching_cols else "❌ Missing"
                }
                
                comparison_results.append(result)
            
            # Check for extra columns in dataframe
            df_cols = set(dataframe.columns)
            notion_props = {prop_name.lower().replace(" ", "") for prop_name in notion_structure.keys()}
            extra_cols = [col for col in df_cols if col.lower().replace(" ", "") not in notion_props]
            
            for col in extra_cols:
                comparison_results.append({
                    "Notion Property": col,
                    "Property Type": "N/A",
                    "Available Options": [],
                    "Status": "⚠️ Extra"
                })
            
            return comparison_results
            
        except Exception as e:
            logger.error(f"Error comparing database structure: {str(e)}")
            raise e

    def process_excel_data(self, excel_file):
        """Process Excel data and return cleaned DataFrame ready for Notion import"""
        return self.excel_processor.process_excel_data(excel_file)

    def create_task_properties(self, task, project_id):
        """Create task properties for Notion API"""
        try:
            task_name = task.get('Tarefa', 'Unnamed Task')
            task_type = task.get('Type', 'Tarefa')
            edt = str(task.get('EDT', '')) if pd.notna(task.get('EDT')) else ''
            
            logger.info(f"Processing task: {task_name} (EDT: {edt}, Type: {task_type})")
            
            # Initialize properties with required fields
            properties = {
                "Tarefa": {"title": [{"text": {"content": task_name}}]},
                "Type": {"select": {"name": task_type}},
                "EDT": {"rich_text": [{"text": {"content": edt}}]},
                "Project": {"relation": [{"id": project_id}]},
            }

            # Add optional properties based on property types
            for field, value in task.items():
                if field in self._property_types and pd.notna(value) and field not in properties:
                    prop_type = self._property_types[field]
                    
                    if prop_type == "status":
                        # Handle status with mappings
                        status_str = str(value).strip()
                        mapped_status = self._status_mappings.get(status_str, self._default_status)
                        logger.info(f"Mapping status for {task_name}: '{status_str}' -> '{mapped_status}'")
                        properties[field] = {"status": {"name": mapped_status}}
                    
                    elif prop_type == "multi_select":
                        # Handle multi-select (e.g., Fase)
                        values = [{"name": v.strip()} for v in str(value).split(",") if v.strip()]
                        if values:
                            properties[field] = {"multi_select": values}
                    
                    elif prop_type == "date":
                        # Handle dates (Datas planeadas and Datas reais)
                        try:
                            if pd.notna(value) and isinstance(value, dict):
                                date_property = {"date": {}}
                                
                                # Add start date if present
                                if value.get("start"):
                                    start_date = value["start"]
                                    if isinstance(start_date, str):
                                        try:
                                            # Parse the date and format it for Notion (date only)
                                            parsed_date = datetime.strptime(start_date, "%Y-%m-%d")
                                            # Format as ISO 8601 date only
                                            start_iso = parsed_date.strftime('%Y-%m-%d')
                                            date_property["date"]["start"] = start_iso
                                            logger.info(f"Added start date for {field}: {start_iso}")
                                        except ValueError:
                                            logger.warning(f"Invalid start date format for {field}: {start_date}")
                                    else:
                                        logger.warning(f"Start date for {field} is not a string: {start_date}")
                                
                                # Add end date if present
                                if value.get("end"):
                                    end_date = value["end"]
                                    if isinstance(end_date, str):
                                        try:
                                            # Parse the date and format it for Notion (date only)
                                            parsed_date = datetime.strptime(end_date, "%Y-%m-%d")
                                            # Format as ISO 8601 date only
                                            end_iso = parsed_date.strftime('%Y-%m-%d')
                                            date_property["date"]["end"] = end_iso
                                            logger.info(f"Added end date for {field}: {end_iso}")
                                        except ValueError:
                                            logger.warning(f"Invalid end date format for {field}: {end_date}")
                                    else:
                                        logger.warning(f"End date for {field} is not a string: {end_date}")
                                
                                if date_property["date"]:  # Only add if we have at least one valid date
                                    properties[field] = date_property
                                    logger.info(f"Added date range for {field}: {date_property}")
                                else:
                                    logger.warning(f"No valid dates found for {field}")
                            else:
                                logger.warning(f"Invalid date value for {field}: {value}")
                            
                        except Exception as e:
                            logger.error(f"Error processing date for {field}: {str(e)}")
                    
                    elif prop_type == "people":
                        # Handle people (e.g., Assignee)
                        if value:
                            # Split multiple assignees if comma-separated
                            assignees = [name.strip() for name in str(value).split(",")]
                            user_ids = []
                            for assignee in assignees:
                                user_id = self.get_user_id(assignee)
                                if user_id:
                                    user_ids.append({"object": "user", "id": user_id})
                            if user_ids:
                                properties[field] = {"people": user_ids}
                    
                    elif prop_type == "select":
                        # Handle single select
                        properties[field] = {"select": {"name": str(value)}}
                    
                    elif prop_type == "rich_text":
                        # Handle rich text
                        properties[field] = {"rich_text": [{"text": {"content": str(value)}}]}
                    
                    elif prop_type == "number":
                        # Handle number fields (like Progresso (dias))
                        try:
                            numeric_value = float(value) if pd.notna(value) else None
                            if numeric_value is not None:
                                properties[field] = {"number": numeric_value}
                                logger.info(f"Added number value for {field}: {numeric_value}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Invalid number value for {field}: {value} - {str(e)}")
                    
            # Add default status if not set
            if "Status" not in properties:
                logger.info(f"Using default status '{self._default_status}' for task: {task_name}")
                properties["Status"] = {"status": {"name": self._default_status}}

            # Debug log for properties
            logger.info(f"Final properties for task '{task_name}':")
            for field, value in properties.items():
                logger.info(f"  {field}: {value}")

            return properties
            
        except Exception as e:
            logger.error(f"Error in create_task_properties: {str(e)}")
            logger.error(f"Task data: {task}")
            raise e

    def get_user_id(self, user_name: str) -> Optional[str]:
        """Get Notion user ID by name"""
        try:
            response = self._handle_api_call(self.notion.users.list)
            for user in response['results']:
                if user.get('name', '').lower() == user_name.lower():
                    return user['id']
            logger.warning(f"User not found: {user_name}")
            return None
        except Exception as e:
            logger.error(f"Error getting user ID: {str(e)}")
            return None

    def verify_tasks_batch(self, tasks_to_verify):
        """Verify multiple tasks in a single query"""
        if not tasks_to_verify:
            return {}
        
        try:
            # Create an OR filter for all tasks
            or_conditions = []
            for task_name, project_id, task_data in tasks_to_verify:
                # Base conditions for task name and project
                and_conditions = [
                    {
                        "property": "Tarefa",
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
                
                # Add EDT condition if available
                if task_data is not None and pd.notna(task_data.get("EDT")):
                    edt = str(task_data["EDT"])
                    and_conditions.append({
                        "property": "EDT",
                        "rich_text": {
                            "equals": edt
                        }
                    })
                
                # Add Fase conditions if available
                if task_data is not None and pd.notna(task_data.get("Fase")):
                    fase_values = [v.strip() for v in str(task_data["Fase"]).split(",") if v.strip()]
                    for fase in fase_values:
                        and_conditions.append({
                            "property": "Fase",
                            "multi_select": {
                                "contains": fase
                            }
                        })
                
                or_conditions.append({"and": and_conditions})
            
            response = self._handle_api_call(self.notion.databases.query,
                database_id=self.tasks_db,
                filter={
                    "or": or_conditions
                }
            )
            
            # Create a set of verified task-project pairs with EDT and Fase
            verified_tasks = set()
            for result in response.get("results", []):
                task_name = result["properties"]["Tarefa"]["title"][0]["text"]["content"]
                project_ids = [rel["id"] for rel in result["properties"]["Project"]["relation"]]
                edt = result["properties"].get("EDT", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
                fase_values = [item.get("name", "") for item in result["properties"].get("Fase", {}).get("multi_select", [])]
                
                for project_id in project_ids:
                    verified_tasks.add((task_name, project_id, edt, ",".join(sorted(fase_values))))
            
            # Create verification result dictionary
            verification_results = {}
            for task_name, project_id, task_data in tasks_to_verify:
                edt = str(task_data.get("EDT", "")) if pd.notna(task_data.get("EDT")) else ""
                fase_values = [v.strip() for v in str(task_data.get("Fase", "")).split(",")] if pd.notna(task_data.get("Fase")) else []
                key = (task_name, project_id)
                value = (task_name, project_id, edt, ",".join(sorted(fase_values))) in verified_tasks
                verification_results[key] = value
            
            return verification_results
            
        except Exception as e:
            logger.error(f"Error in batch verification: {str(e)}")
            # Return all as unverified in case of error
            return {(task_name, project_id): False for task_name, project_id, _ in tasks_to_verify}

    def verify_task_creation(self, task_name: str, project_id: str) -> bool:
        """Verify if a task was actually created in Notion"""
        max_retries = 3
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                response = self._handle_api_call(self.notion.databases.query,
                    database_id=self.tasks_db,
                    filter={
                        "and": [
                            {
                                "property": "Tarefa",
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
                    }
                )
                
                if len(response["results"]) > 0:
                    return True
                    
                if attempt < max_retries - 1:
                    logger.info(f"Task verification attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    
            except Exception as e:
                logger.error(f"Error verifying task creation (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    
        return False

    def get_notion_token(self) -> str:
        """Get Notion API token from environment or stored value"""
        if self._token:
            return self._token
        token = os.getenv("NOTION_TOKEN")
        if not token:
            raise ValueError("NOTION_TOKEN environment variable is not set")
        return token

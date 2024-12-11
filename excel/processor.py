import pandas as pd
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional, Set
from logging import getLogger
from config.config import TYPED_EXCEL_CONFIG, ExcelConfig, merge_config, EXCEL_CONFIG, create_excel_config

logger = getLogger(__name__)

class TaskNameGenerator:
    """Handles generation of unique task names"""
    
    def __init__(self, config: ExcelConfig):
        self.config = config
        self.used_names: Set[str] = set()
    
    def reset(self) -> None:
        """Reset the set of used names"""
        self.used_names.clear()
    
    def generate_unique_name(self, base_name: str) -> str:
        """Generate a unique name by adding a numeric suffix if needed"""
        if base_name not in self.used_names:
            self.used_names.add(base_name)
            return base_name
        
        index = 1
        while True:
            new_name = f"{base_name} {self.config.task_naming.unique_suffix_pattern.format(index=index)}"
            if new_name not in self.used_names:
                self.used_names.add(new_name)
                return new_name
            index += 1
    
    def get_task_name(self, row: pd.Series, is_milestone: bool = False) -> str:
        """Generate a meaningful and unique task name"""
        title = row[self.config.task_columns.title]
        edt = row[self.config.task_columns.edt]
        
        if pd.isna(title) or str(title).strip() == "":
            if self.config.task_naming.use_edt_as_fallback and not pd.isna(edt):
                prefix = (self.config.task_naming.empty_milestone_prefix 
                         if is_milestone 
                         else self.config.task_naming.empty_task_prefix)
                base_name = f"{prefix} {edt}"
            else:
                prefix = (self.config.task_naming.empty_milestone_prefix 
                         if is_milestone 
                         else self.config.task_naming.empty_task_prefix)
                base_name = prefix
                if not pd.isna(edt):
                    base_name += f" {edt}"
        else:
            base_name = str(title).strip()
        
        return self.generate_unique_name(base_name)

class DateProcessor:
    """Handles processing of dates from Excel data"""
    
    @staticmethod
    def safe_convert_date(date_value) -> Optional[datetime]:
        """Safely convert Excel date value to datetime"""
        try:
            # Handle pandas Series
            if isinstance(date_value, pd.Series):
                if date_value.empty:
                    return None
                # Get first non-null value if any exist
                non_null = date_value.dropna()
                if non_null.empty:
                    return None
                date_value = non_null.iloc[0]
            
            if pd.isna(date_value) or date_value == '':
                return None
                
            if isinstance(date_value, str):
                # Try to parse string date
                try:
                    return pd.to_datetime(date_value).to_pydatetime()
                except:
                    logger.warning(f"Failed to parse string date: {date_value}")
                    return None
            
            # Handle numeric Excel dates
            try:
                return pd.to_datetime(date_value).to_pydatetime()
            except:
                logger.warning(f"Failed to convert numeric date: {date_value}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to convert date {date_value} ({type(date_value)}): {e}")
            return None
    
    @classmethod
    def process_dates(cls, data: pd.DataFrame, config: ExcelConfig) -> Tuple[Dict[int, Dict[str, datetime]], Dict[int, Dict[str, datetime]]]:
        """Process planned and real dates from the Excel data"""
        planned_dates = {}
        real_dates = {}
        
        # Log available columns for debugging
        columns = [str(col) if not pd.isna(col) else 'nan' for col in data.columns]
        
        # Find the actual column names that best match our expected names
        def find_column(expected_name: str, df: pd.DataFrame) -> Optional[str]:
            # Convert all column names to strings and handle nan
            cols = {str(col) if not pd.isna(col) else '' for col in df.columns}
            
            # First try exact match
            if expected_name in cols:
                return expected_name
            
            # Try case-insensitive match
            expected_lower = expected_name.lower()
            for col in cols:
                if col.lower() == expected_lower:
                    return col
            
            # Try finding column containing the name
            for col in cols:
                if expected_name.lower() in col.lower():
                    return col
            
            logger.warning(f"No match found for column '{expected_name}'")
            return None
        
        # Get actual column names
        planned_start_col = find_column(config.task_columns.planned_start, data)
        planned_end_col = find_column(config.task_columns.planned_end, data)
        actual_start_col = find_column(config.task_columns.actual_start, data)
        actual_end_col = find_column(config.task_columns.actual_end, data)
        
        for idx, row in data.iterrows():
            try:
                # Process planned dates
                if planned_start_col or planned_end_col:
                    start = cls.safe_convert_date(row[planned_start_col]) if planned_start_col else None
                    end = cls.safe_convert_date(row[planned_end_col]) if planned_end_col else None
                    if start is not None or end is not None:
                        planned_dates[idx] = {"start": start, "end": end}
                
                # Process real dates
                if actual_start_col or actual_end_col:
                    start = cls.safe_convert_date(row[actual_start_col]) if actual_start_col else None
                    end = cls.safe_convert_date(row[actual_end_col]) if actual_end_col else None
                    if start is not None or end is not None:
                        real_dates[idx] = {"start": start, "end": end}
                        
            except Exception as e:
                continue  # Skip rows with errors instead of failing
        return planned_dates, real_dates

class ExcelProcessor:
    """Processes Excel files containing project and task data"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize ExcelProcessor with optional configuration"""
        if config:
            # If user config is provided, merge it with base config
            merged_config = merge_config(EXCEL_CONFIG, config)
            self.config = create_excel_config(merged_config)
        else:
            # Otherwise use the typed config directly
            self.config = TYPED_EXCEL_CONFIG
        self.name_generator = TaskNameGenerator(self.config)
        
    def get_project_info(self, excel_file: str) -> Dict[str, str]:
        """Extract project information from Excel file"""
        project_info = {'name': None, 'id': None}
        
        try:
            # Get available sheets and find the planning sheet
            xl = pd.ExcelFile(excel_file)
            available_sheets = xl.sheet_names
            
            # Try different possible sheet names
            sheet_names = ["PLANEAMENTO", "Planeamento", "planeamento"]
            planning_sheet = None
            
            for sheet in available_sheets:
                if sheet.upper() in [s.upper() for s in sheet_names]:
                    planning_sheet = sheet
                    break
            
            if not planning_sheet:
                logger.error(f"Could not find planning sheet. Available sheets: {available_sheets}")
                return project_info
            # Read without headers first to see the actual data
            df = pd.read_excel(excel_file, sheet_name=planning_sheet, header=None)
            
            # Log first few rows for debugging
            for idx in range(min(5, len(df))):
                logger.info(f"Row {idx}: {df.iloc[idx].tolist()}")
            
            if not df.empty:
                # Look through first few rows to find EDT and project name
                for idx in range(min(10, len(df))):  # Check first 10 rows
                    row = df.iloc[idx]
                    row_data = [str(cell).strip() if not pd.isna(cell) else "" for cell in row]
                    
                    # Look for EDT-like value (usually starts with PR.)
                    edt_value = None
                    title_value = None
                    
                    for col_idx, value in enumerate(row_data):
                        if value.startswith('PR.'):
                            edt_value = value
                            # Title is usually in the next column
                            if col_idx + 1 < len(row_data):
                                title_value = row_data[col_idx + 1]
                            break
                    
                    if edt_value and title_value:
                        project_info['id'] = edt_value
                        project_info['name'] = title_value
                        break
                
                if not project_info['name'] or not project_info['id']:
                    logger.error(f"Could not find project name or ID in the {planning_sheet} sheet")
            else:
                logger.error(f"{planning_sheet} sheet is empty")
                
        except Exception as e:
            logger.error(f"Failed to extract project info: {str(e)}")
            logger.exception("Full traceback:")
        
        return project_info

    def get_phase_edt(self, edt: str) -> Optional[str]:
        """Get the phase EDT from a task EDT"""
        if pd.isna(edt):
            return None
        parts = str(edt).split('.')
        if len(parts) >= self.config.phase.edt_parts:
            return '.'.join(parts[:self.config.phase.edt_parts])
        return None

    def is_project(self, row: pd.Series) -> bool:
        """Check if row represents a project (EDT format: PR.XXXX)"""
        try:
            edt = str(row[self.config.task_columns.edt]).strip()
            return edt.startswith('PR.') and len(edt.split('.')) == 2
        except:
            return False

    def is_phase(self, row: pd.Series) -> bool:
        """Check if row represents a phase (EDT format: PR.XXXX.Y)"""
        try:
            edt = str(row[self.config.task_columns.edt]).strip()
            parts = edt.split('.')
            return edt.startswith('PR.') and len(parts) == 3
        except:
            return False

    def is_milestone(self, row: pd.Series) -> bool:
        """Check if row represents a milestone based on EDT or title"""
        try:
            # First check EDT pattern (ends with .M)
            edt = str(row[self.config.task_columns.edt]).strip()
            if edt.endswith('.M'):
                return True
            
            # Then check title for milestone indicators
            title = str(row[self.config.task_columns.title]).strip()
            return title.upper().startswith('MILESTONE:') or 'MILESTONE' in title.upper()
        except:
            return False

    def extract_phase(self, edt: str) -> Optional[str]:
        """Extract phase from EDT (gets parent phase for tasks/milestones)"""
        try:
            if pd.isna(edt):
                return None
                
            parts = str(edt).strip().split('.')
            if len(parts) <= 2:  # Project or invalid EDT
                return None
                
            # For any task/milestone, get its parent phase
            # PR.0091.1.2.M -> PR.0091.1
            # PR.0091.1.1.1 -> PR.0091.1
            phase_edt = '.'.join(parts[:3])  # Take first 3 parts (PR.XXXX.Y)
            
            if phase_edt in self._phase_titles:
                return self._phase_titles[phase_edt]
            return None
        except Exception as e:
            logger.error(f"Error extracting phase from EDT {edt}: {e}")
            return None

    def get_parent_task(self, edt: str) -> Optional[str]:
        """Get the parent task EDT from a task EDT"""
        if pd.isna(edt):
            return None
        
        parts = str(edt).strip().split('.')
        if len(parts) <= 2:  # Project or no parent
            return None
            
        # Get parent EDT by removing the last part
        # PR.0001.1.1.1 -> PR.0001.1.1
        parent_edt = '.'.join(parts[:-1])
        return parent_edt

    def process_excel_data(self, excel_file: str) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
        """Process Excel data and return cleaned DataFrame ready for Notion import"""
        try:
            # Reset name generator for new processing
            self.name_generator.reset()
            
            # Get project info first
            project_info = self.get_project_info(excel_file)
            
            # Read tasks sheet
            logger.info(f"Reading tasks sheet: {self.config.tasks_sheet}")
            df = pd.read_excel(excel_file, sheet_name=self.config.tasks_sheet)
            
            # Find header row
            logger.info(f"Looking for header in column {self.config.header.column} with value {self.config.header.value}")
            header_rows = df.iloc[:, self.config.header.column - 1] == self.config.header.value
            if not header_rows.any():
                # Try with 0-based index
                header_rows = df.iloc[:, self.config.header.column] == self.config.header.value
                if not header_rows.any():
                    raise ValueError(f"Could not find header row with value '{self.config.header.value}' in column {self.config.header.column}")
            header_row = header_rows[header_rows].index[0]
            df.columns = df.iloc[header_row]
            
            # Filter data after header
            all_rows = df.iloc[header_row + 1:].copy()
            all_rows = all_rows[all_rows[self.config.task_columns.title].notna()]
            logger.info(f"Found {len(all_rows)} rows after header")
            
            # First pass: identify and store phase titles
            self._phase_titles = {}
            phase_mask = all_rows.apply(self.is_phase, axis=1)
            project_mask = all_rows.apply(self.is_project, axis=1)
            phase_rows = all_rows[phase_mask]
            
            for _, row in phase_rows.iterrows():
                edt = str(row[self.config.task_columns.edt]).strip()
                title = str(row[self.config.task_columns.title]).strip()
                self._phase_titles[edt] = title
            logger.info(f"Identified {len(self._phase_titles)} phases")
            logger.debug(f"Phase titles: {self._phase_titles}")
            
            # Filter out only project rows, keep phases, tasks and milestones
            task_rows = all_rows[~project_mask].copy()
            milestone_mask = task_rows.apply(self.is_milestone, axis=1)
            
            # Calculate statistics
            stats = {
                'total_phases': len(self._phase_titles),
                'total_tasks': len(task_rows) - sum(milestone_mask) - len(phase_rows),
                'total_milestones': sum(milestone_mask)
            }
            logger.info(f"Statistics: {stats}")
            logger.info(f"After filtering projects and phases: {len(task_rows)} rows")
            
            # Process dates
            planned_dates, real_dates = DateProcessor.process_dates(task_rows, self.config)
            
            # Build final DataFrame with Notion column names and types
            notion_structure = pd.DataFrame()
            
            # Store headers in metadata
            metadata = {
                'headers': list(task_rows.columns),
                'total_rows': len(task_rows)
            }
            
            # Map each field according to config
            for field, mapping in self.config.field_mappings.items():
                notion_name = mapping["notion"]
                
                if field == "title":
                    # Title field (from FASES/TAREFAS)
                    notion_structure[notion_name] = task_rows.apply(
                        lambda row: self.name_generator.get_task_name(row, is_milestone=self.is_milestone(row)), 
                        axis=1
                    )
                
                elif field == "type":
                    # Type field (Milestone, Phase, or Task)
                    def get_type(row):
                        if self.is_milestone(row):
                            return "Milestone"
                        if self.is_phase(row):
                            return "Fase"
                        type_col = self.config.task_columns.type
                        if type_col in row.index:
                            return self.config.type_mapping.get(row[type_col], "Tarefa")
                        return "Tarefa"
                    
                    notion_structure[notion_name] = task_rows.apply(get_type, axis=1)
                
                elif field == "edt":
                    # EDT field (direct mapping)
                    notion_structure[notion_name] = task_rows[mapping["excel"]]
                
                elif field == "status":
                    # Status field (with mapping)
                    status_col = mapping["excel"]
                    logger.info(f"Processing status from column '{status_col}'")
                    
                    # Get raw status values and log them
                    raw_status = task_rows[status_col]
                    logger.debug(f"Raw status values: {raw_status.value_counts().to_dict()}")
                    
                    # Apply mapping with debug logging
                    mapped_status = raw_status.map(self.config.status_mapping)
                    logger.debug(f"Status mapping being used: {self.config.status_mapping}")
                    logger.debug(f"Mapped status values: {mapped_status.value_counts().to_dict()}")
                    
                    # Fill missing values with default
                    default_status = self.config.status_mapping.get("default", "Not started")
                    notion_structure[notion_name] = mapped_status.fillna(default_status)
                    logger.debug(f"Final status values: {notion_structure[notion_name].value_counts().to_dict()}")
                
                elif field == "phase":
                    # Phase field (computed from EDT)
                    notion_structure[notion_name] = task_rows[self.config.task_columns.edt].apply(self.extract_phase)
                
                elif field == "parent":
                    # Parent task field (computed from EDT)
                    notion_structure[notion_name] = task_rows[self.config.task_columns.edt].apply(self.get_parent_task)
                
                elif field == "progress":
                    # Progress field (direct mapping with numeric conversion)
                    logger.info(f"Processing progress field from {mapping['excel']}")
                    progress_col = mapping["excel"]
                    if progress_col in task_rows.columns:
                        try:
                            notion_structure[notion_name] = pd.to_numeric(
                                task_rows[progress_col],
                                errors='coerce'
                            ).fillna(0)
                            logger.debug(f"Progress values: {notion_structure[notion_name].value_counts().to_dict()}")
                        except Exception as e:
                            logger.error(f"Error converting progress values: {e}")
                            notion_structure[notion_name] = pd.Series(0, index=task_rows.index)
                    else:
                        logger.warning(f"Progress column '{progress_col}' not found in DataFrame")
                        notion_structure[notion_name] = pd.Series(0, index=task_rows.index)
                
                elif field == "planned_dates":
                    # Planned dates (composite field)
                    logger.info(f"Processing planned dates for {len(task_rows)} rows")
                    date_ranges = []
                    for idx in task_rows.index:
                        date_range = format_date_range(planned_dates.get(idx))
                        logger.debug(f"Row {idx} planned dates: {date_range}")
                        if date_range:
                            date_ranges.append(date_range)
                        else:
                            date_ranges.append(None)
                    notion_structure[notion_name] = pd.Series(date_ranges, index=task_rows.index)
                
                elif field == "actual_dates":
                    # Actual dates (composite field)
                    logger.info(f"Processing actual dates for {len(task_rows)} rows")
                    date_ranges = []
                    for idx in task_rows.index:
                        date_range = format_date_range(real_dates.get(idx))
                        logger.debug(f"Row {idx} actual dates: {date_range}")
                        if date_range:
                            date_ranges.append(date_range)
                        else:
                            date_ranges.append(None)
                    notion_structure[notion_name] = pd.Series(date_ranges, index=task_rows.index)
            
            logger.info(f"Final structure has {len(notion_structure)} rows")
            logger.debug(f"Final columns: {notion_structure.columns.tolist()}")
            return notion_structure, stats, metadata
        except Exception as e:
            logger.error(f"Failed to process Excel data: {str(e)}", exc_info=True)
            raise

def format_date_range(date_dict):
    if not date_dict:
        logger.debug("No date_dict provided")
        return None
    
    start = date_dict.get('start')
    end = date_dict.get('end')
    logger.debug(f"Raw dates: start={start} ({type(start)}), end={end} ({type(end)})")
    
    if not start and not end:
        logger.debug("No start or end date")
        return None

    try:
        # Convert dates to string format
        start_str = start.strftime("%Y-%m-%d") if start else None
        end_str = end.strftime("%Y-%m-%d") if end else None
        logger.debug(f"Formatted dates: start={start_str}, end={end_str}")

        # If we only have one date, use it for both start and end
        if start_str and not end_str:
            logger.debug("Only start date present, using it for both")
            return {'start': start_str, 'end': start_str}
        if end_str and not start_str:
            logger.debug("Only end date present, using it for both")
            return {'start': end_str, 'end': end_str}

        # Validate that start is before end
        if start and end and start > end:
            logger.debug("Start date after end date, swapping")
            return {'start': end_str, 'end': start_str}
        
        logger.debug(f"Returning date range: start={start_str}, end={end_str}")
        return {'start': start_str, 'end': end_str}
    except Exception as e:
        logger.error(f"Error formatting date range: {e}")
        return None

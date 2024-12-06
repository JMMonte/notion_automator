import pandas as pd
import logging
from typing import Dict, Any, Tuple, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExcelProcessor:
    def __init__(self):
        """Initialize ExcelProcessor with default configurations"""
        self.phase_mapping = {
            "1": "Planeamento",
            "2": "Desenvolvimento",
            "3": "Testes",
            "4": "Rollout"
        }
        
        self.type_mapping = {
            "M": "Milestone",
            "T": "Tarefa"
        }
        
    def extract_project_info(self, excel_file) -> Dict[str, Any]:
        """Extract project information from the Excel file"""
        try:
            df = pd.read_excel(excel_file, sheet_name='FICHA PROJETO')
            
            # Find project name and ID
            project_name = None
            project_id = None
            for idx, row in df.iterrows():
                if 'NOME DO PROJETO' in str(row.values):
                    project_name = df.iloc[idx, 3]  # Column D contains the project name
                if 'ID PROJETO' in str(row.values):
                    project_id = df.iloc[idx, 8]  # Column I contains the project ID
                if project_name and project_id:
                    break
            
            return {
                'name': project_name,
                'id': project_id
            }
        except Exception as e:
            logger.error(f"Error extracting project info: {str(e)}")
            return None

    def process_excel_data(self, excel_file) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Process Excel data and return cleaned DataFrame ready for Notion import"""
        try:
            
            # Read Excel file without header first to get the structure
            raw_data = pd.read_excel(excel_file, sheet_name='PLANEAMENTO', header=None)
            
            # Find the header row (usually row 4)
            header_row = raw_data[raw_data[3] == 'FASES/TAREFAS'].index[0]
            
            # First read with single header to get TRABALHO REALIZADO
            single_header_df = pd.read_excel(excel_file, sheet_name='PLANEAMENTO', header=header_row)
            progress_values = None
            if 'TRABALHO REALIZADO' in single_header_df.columns:
                progress_values = single_header_df['TRABALHO REALIZADO'].copy()
                logger.info(f"Found progress values using single header: {progress_values.head()}")
            
            # Then read with multi-level headers for dates
            header_rows = pd.read_excel(excel_file, sheet_name='PLANEAMENTO', header=[header_row-1, header_row])
            
            # Create a mapping for the date columns based on the multi-level headers
            date_columns = {}
            
            # Log all column headers for debugging
            logger.info("Available columns:")
            for col in header_rows.columns:
                logger.info(f"Column: {col}")
                if isinstance(col, tuple):
                    section, subcol = col[0], col[1]
                    if pd.notna(section):
                        section = str(section).strip().upper()
                        if pd.notna(subcol):
                            subcol = str(subcol).strip().upper()
                            
                            # Map date columns
                            if section in ['PLENEADO', 'PLANEADO', 'PLANEADA']:
                                if 'INÍCIO' in subcol or 'INICIO' in subcol:
                                    date_columns[col] = 'INÍCIO PLANEADO'
                                elif 'FIM' in subcol:
                                    date_columns[col] = 'FIM PLANEADO'
                            elif section in ['REAL', 'REALIZADA']:
                                if 'INÍCIO' in subcol or 'INICIO' in subcol:
                                    date_columns[col] = 'INÍCIO REAL'
                                elif subcol == 'DATA FIM':
                                    date_columns[col] = 'FIM REAL'
            
            # Log the identified date columns
            for original, mapped in date_columns.items():
                logger.info(f"Mapped column {original} to {mapped}")
            
            # Rename the date columns
            new_columns = {}
            for col in header_rows.columns:
                if col in date_columns:
                    new_columns[col] = date_columns[col]
                else:
                    # For non-date columns, use the last level if it's a tuple
                    new_columns[col] = col[-1] if isinstance(col, tuple) else col
            
            cleaned_data = header_rows.copy()
            cleaned_data.columns = [new_columns.get(col, col) for col in cleaned_data.columns]
            
            # Add progress values if found
            if progress_values is not None:
                cleaned_data['Progresso (dias)'] = progress_values
                logger.info(f"Added progress values to DataFrame. Sample: {cleaned_data['Progresso (dias)'].head()}")
            else:
                logger.warning("No progress values found")
                cleaned_data['Progresso (dias)'] = None
            
            # Convert dates to datetime
            for date_col in ["INÍCIO PLANEADO", "FIM PLANEADO", "INÍCIO REAL", "FIM REAL"]:
                if date_col in cleaned_data.columns:
                    # Log some sample values before conversion
                    sample_values = cleaned_data[date_col].head()
                    
                    # Convert to datetime, handle errors, and log conversions
                    cleaned_data[date_col] = pd.to_datetime(cleaned_data[date_col], errors='coerce')
                    non_null_dates = cleaned_data[date_col].count()
                    
                    # Log sample values after conversion
                    sample_values = cleaned_data[date_col].head()
                else:
                    logger.warning(f"Date column {date_col} not found in DataFrame")
            
            # Initialize phase tracking
            current_phase = None
            phase_info = {}
            
            # Initialize columns
            cleaned_data["Type"] = "Tarefa"  # Default type
            cleaned_data["Fase"] = None      # Initialize Fase column

            # First pass: Determine types for each row
            for idx, row in cleaned_data.iterrows():
                if pd.notna(row.iloc[1]):  # Check the type column
                    cleaned_data.at[idx, "Type"] = self.type_mapping.get(row.iloc[1], "Tarefa")

            # Process each row to identify phases and tasks
            for idx, row in cleaned_data.iterrows():
                task_name = row["FASES/TAREFAS"]
                edt = str(row["EDT"]) if pd.notna(row["EDT"]) else ""
                
                # Check if this is a phase row (no assignee and not a milestone)
                is_phase = pd.isna(row["RESPONSÁVEL"]) and row["Type"] != "Milestone"
                
                if is_phase and pd.notna(task_name):
                    current_phase = task_name
                    phase_info[current_phase] = []
                    cleaned_data.at[idx, "Fase"] = current_phase
                elif current_phase and pd.notna(task_name):
                    # Store the current phase for this task or milestone
                    phase_info[current_phase].append(idx)
                    # Assign the current phase to the task/milestone
                    cleaned_data.at[idx, "Fase"] = current_phase

            # Process EDT phases
            for idx, row in cleaned_data.iterrows():
                if pd.notna(row["EDT"]):
                    edt_phases = self._get_phase_from_edt(str(row["EDT"]))
                    if edt_phases:
                        current_phase = cleaned_data.at[idx, "Fase"]
                        if pd.isna(current_phase):
                            # Only use EDT phase if no structural phase exists
                            cleaned_data.at[idx, "Fase"] = ", ".join(edt_phases)
            
            # Function to safely format date
            def safe_format_date(date):
                try:
                    if pd.isna(date):
                        return None
                    # Convert to datetime if not already
                    if not isinstance(date, pd.Timestamp):
                        date = pd.to_datetime(date)
                    # Format to ISO 8601 date string
                    formatted_date = date.strftime("%Y-%m-%d")
                    logger.debug(f"Successfully formatted date {date} to ISO format: {formatted_date}")
                    return formatted_date
                except Exception as e:
                    logger.error(f"Failed to format date {date} (type: {type(date)}): {str(e)}")
                    return None

            # Create planned dates with validation
            def create_date_range(row, start_col, end_col, date_type=""):
                """Create a date range dictionary from start and end dates"""
                start_date = None
                end_date = None
                
                # Convert start date
                if pd.notna(row.get(start_col)):
                    start_date = row[start_col].strftime("%Y-%m-%d")
                
                # Convert end date
                if pd.notna(row.get(end_col)):
                    end_date = row[end_col].strftime("%Y-%m-%d")
                
                # Only create date range if we have at least a start date
                if start_date:
                    date_range = {"start": start_date}
                    if end_date:
                        date_range["end"] = end_date
                    return date_range
                elif end_date:  # If we only have an end date, use it as the start date too
                    return {"start": end_date, "end": end_date}
                return None

            # Process planned dates
            cleaned_data["Datas planeadas"] = cleaned_data.apply(
                lambda row: create_date_range(row, "INÍCIO PLANEADO", "FIM PLANEADO", "Planned"),
                axis=1
            )
            
            # Process real dates
            cleaned_data["Datas reais"] = cleaned_data.apply(
                lambda row: create_date_range(row, "INÍCIO REAL", "FIM REAL", "Real"),
                axis=1
            )
            
            # Log date statistics and validation
            def log_date_stats(date_column, column_name):
                total_dates = cleaned_data[date_column].count()
                valid_dates = cleaned_data[date_column].apply(lambda x: bool(x and (x.get('start') or x.get('end')))).sum()
                
                # Sample some dates for verification
                sample = cleaned_data[cleaned_data[date_column].notna()].head(3)
                for idx, row in sample.iterrows():
                    dates = row[date_column]
                    task = row.get('FASES/TAREFAS', 'Unknown')

            log_date_stats("Datas planeadas", "Planned Dates")
            log_date_stats("Datas reais", "Real Dates")
            
            
            # Filter out metadata rows
            metadata_criteria = (
                (cleaned_data["FASES/TAREFAS"].isna()) |  # Remove empty rows
                (cleaned_data["FASES/TAREFAS"].str.strip() == "") |  # Remove blank rows
                ((cleaned_data["RESPONSÁVEL"].isna()) & (cleaned_data["Type"] != "Milestone")) |  # Remove phases but keep milestones
                (cleaned_data.index == 0)  # Remove first row (project name)
            )
            filtered_data = cleaned_data[~metadata_criteria].reset_index(drop=True)

            # Prepare statistics
            stats = {
                "total_rows": len(cleaned_data),
                "filtered_rows": len(filtered_data),
                "phases_removed": len(cleaned_data[(cleaned_data["RESPONSÁVEL"].isna()) & (cleaned_data["Type"] != "Milestone")]),
                "milestones": len(filtered_data[filtered_data["Type"] == "Milestone"]),
                "tasks": len(filtered_data[filtered_data["Type"] == "Tarefa"]),
                "phases": list(phase_info.keys())
            }

            # Create Notion-ready structure
            notion_columns = [
                "Tarefa", "Status", "Fase", "Assignee", "Datas planeadas", "Datas reais",
                "Type", "EDT", "Progresso (dias)"
            ]
            
            notion_structure = pd.DataFrame(columns=notion_columns)
            notion_structure["Tarefa"] = filtered_data["FASES/TAREFAS"]
            notion_structure["Status"] = filtered_data["STATUS"]
            notion_structure["Fase"] = filtered_data["Fase"]
            notion_structure["EDT"] = filtered_data["EDT"]
            notion_structure["Datas planeadas"] = filtered_data["Datas planeadas"]
            notion_structure["Datas reais"] = filtered_data["Datas reais"]
            notion_structure["Type"] = filtered_data["Type"]
            notion_structure["Progresso (dias)"] = filtered_data["Progresso (dias)"]
            
            return notion_structure, stats

        except Exception as e:
            logger.error(f"Error processing Excel data: {str(e)}")
            raise e

    def _get_phase_from_edt(self, edt: str) -> List[str]:
        """Extract phase from EDT code (e.g., PR.001.1.1)"""
        if pd.isna(edt):
            return []
        
        try:
            parts = str(edt).split(".")
            if len(parts) >= 3:
                phase_number = parts[2]  # Get the phase number
                phase = self.phase_mapping.get(phase_number)
                return [phase] if phase else []
        except (IndexError, AttributeError) as e:
            logger.warning(f"Could not extract phase from EDT: {edt} - {str(e)}")
        return []

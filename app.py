import streamlit as st
import pandas as pd
from notion.api import NotionOperator
import os
from typing import Dict, Any
from dotenv import load_dotenv
from excel.processor import ExcelProcessor
from config.config import COMMON_CONFIG, EXCEL_CONFIG, NOTION_CONFIG, merge_config
from datetime import datetime
import logging
import tempfile

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(page_title="Notion Project Automator", page_icon="📊", layout="wide")
pd.set_option('display.max_rows', None)

def format_date_range(date_dict):
    """Format a date dictionary into a readable string"""
    if pd.isna(date_dict) or date_dict is None:
        return ""
    start = date_dict.get('start', '')
    end = date_dict.get('end', '')
    if start and end:
        return f"{start} → {end}"
    elif start:
        return f"Start: {start}"
    elif end:
        return f"End: {end}"
    return ""

def init_notion_client() -> NotionOperator:
    """Initialize Notion client with configuration"""
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise ValueError("NOTION_TOKEN environment variable is not set. Please check your .env file.")
    return NotionOperator(notion_token=token, config=NOTION_CONFIG)

def init_excel_processor() -> ExcelProcessor:
    """Initialize Excel processor with configuration"""
    return ExcelProcessor(config=EXCEL_CONFIG)

def main():
    # Initialize session state
    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = {}
    if 'uploaded_to_notion' not in st.session_state:
        st.session_state.uploaded_to_notion = set()
    if 'upload_logs' not in st.session_state:
        st.session_state.upload_logs = {}
    if 'widget_counter' not in st.session_state:
        st.session_state.widget_counter = 0
    if 'current_file_index' not in st.session_state:
        st.session_state.current_file_index = 0
    
    st.title("Notion Project Automator 📊")
    
    # Initialize processors
    try:
        notion = init_notion_client()
        excel_processor = init_excel_processor()
    except Exception as e:
        st.error(f"Failed to initialize processors: {str(e)}")
        return

    # File uploader section
    st.header("1. Upload Excel Files")
    uploaded_files = st.file_uploader(
        "Choose Excel files",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        help="Upload one or more Excel files containing project data"
    )

    if not uploaded_files:
        st.info("""
        👋 Welcome to the Notion Project Automator!
        
        This tool helps you automate the process of creating and updating project tasks in Notion.
        
        Get started by uploading your Excel files above.
        """)
        return

    total_files = len(uploaded_files)
    st.write(f"📁 {total_files} file(s) selected")
    
    # Display current file being processed
    if uploaded_files:
        current_file = uploaded_files[st.session_state.current_file_index]
        st.subheader(f"Currently Processing: {current_file.name} ({st.session_state.current_file_index + 1}/{total_files})")
        
        # Initialize log container for current file
        if current_file.name not in st.session_state.upload_logs:
            st.session_state.upload_logs[current_file.name] = []
        
        log_container = st.empty()
        
        def update_log(message, message_type="info"):
            timestamp = datetime.now().strftime("%H:%M:%S")
            emoji_map = {
                "info": "ℹ️",
                "success": "✅",
                "warning": "⚠️",
                "error": "❌"
            }
            emoji = emoji_map.get(message_type, "ℹ️")
            log_entry = f"[{timestamp}] {emoji} {message}"
            st.session_state.upload_logs[current_file.name].append(log_entry)
            
            # Increment counter for unique key
            st.session_state.widget_counter += 1
            log_key = f"log_{current_file.name}_{st.session_state.widget_counter}"
            
            # Update log display
            log_container.text_area(
                "Upload Progress Log",
                "\n".join(st.session_state.upload_logs[current_file.name]),
                height=200,
                key=log_key
            )
        
        # Process current file if not already processed
        if current_file.name not in st.session_state.processed_files:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                    tmp_file.write(current_file.getvalue())
                    temp_path = tmp_file.name

                # Process the file using excel_processor
                project_info = excel_processor.get_project_info(temp_path)
                if not project_info['name'] or not project_info['id']:
                    st.error(f"❌ Could not extract project information from {current_file.name}")
                    if st.button("Skip File"):
                        st.session_state.current_file_index = min(st.session_state.current_file_index + 1, total_files - 1)
                        st.experimental_rerun()
                    return

                # Process the Excel data
                notion_structure, stats, metadata = excel_processor.process_excel_data(temp_path)

                # Header verification step
                st.subheader("2. Verify Headers")
                st.write("Please verify that your Excel headers match the expected Notion headers:")
                
                # Get expected headers from config
                required_headers = {
                    'EDT': excel_processor.config.task_columns.edt,
                    'Title': excel_processor.config.task_columns.title,
                    'Status': excel_processor.config.task_columns.status,
                    'Progress': excel_processor.config.task_columns.progress,
                    'Planned Start': excel_processor.config.task_columns.planned_start,
                    'Planned End': excel_processor.config.task_columns.planned_end,
                    'Actual Start': excel_processor.config.task_columns.actual_start,
                    'Actual End': excel_processor.config.task_columns.actual_end,
                }
                
                optional_headers = {
                    'Type': excel_processor.config.task_columns.type,
                }
                
                # Get actual headers from the DataFrame
                actual_headers = list(metadata['headers'])
                
                # Create a comparison DataFrame for required headers
                comparison_data = []
                for field, expected in required_headers.items():
                    found = expected in actual_headers
                    comparison_data.append({
                        'Field': field,
                        'Expected Header': expected,
                        'Required': '✅',
                        'Found': '✅' if found else '❌'
                    })
                
                # Add optional headers to comparison
                for field, expected in optional_headers.items():
                    found = expected in actual_headers
                    comparison_data.append({
                        'Field': field,
                        'Expected Header': expected,
                        'Required': '❌',
                        'Found': '✅' if found else '❌'
                    })
                
                comparison_df = pd.DataFrame(comparison_data)
                st.dataframe(comparison_df, use_container_width=True)
                
                # Check if all required headers are present
                missing_headers = [h for h in required_headers.values() if h not in actual_headers]
                if missing_headers:
                    st.error(f"❌ Missing required headers: {', '.join(missing_headers)}")
                    if st.button("Skip File"):
                        st.session_state.current_file_index = min(st.session_state.current_file_index + 1, total_files - 1)
                        st.experimental_rerun()
                    return
                
                # Ask for user confirmation
                if not st.checkbox("I confirm that the headers are correct", key=f"header_confirm_{current_file.name}"):
                    st.info("Please confirm the headers are correct to proceed with the upload")
                    return

                # Store processed data in session state
                st.session_state.processed_files[current_file.name] = {
                    'project_info': project_info,
                    'stats': stats,
                    'metadata': metadata,
                    'notion_structure': notion_structure
                }

            except Exception as e:
                st.error(f"❌ Error processing {current_file.name}: {str(e)}")
                if st.button("Skip File"):
                    st.session_state.current_file_index = min(st.session_state.current_file_index + 1, total_files - 1)
                    st.experimental_rerun()
                return
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.error(f"Error cleaning up temporary file for {current_file.name}: {str(e)}")
        
        # Get processed data
        file_data = st.session_state.processed_files[current_file.name]
        project_info = file_data['project_info']
        stats = file_data['stats']
        metadata = file_data['metadata']
        notion_structure = file_data['notion_structure']
        
        # Create tabs for the current file
        tab1, tab2, tab3 = st.tabs([
            "📋 Project Info",
            "🔍 Preview",
            "✨ Notion"
        ])
        
        with tab1:
            st.markdown(f"""
            ### Project Information
            - **Name**: {project_info['name']}
            - **ID**: {project_info['id']}
            
            📊 **File Statistics:**
            - Total Phases: {stats['total_phases']}
            - Total Tasks: {stats['total_tasks']}
            - Total Milestones: {stats['total_milestones']}
            """)
            
        with tab2:
            st.dataframe(notion_structure)
            
        with tab3:
            st.subheader("Similar Projects Check")
            similar_projects = notion.projects.find_similar_projects(
                project_info['name'],  # Use consistent 'name' key
                project_info['id']
            )
            
            selected_project = None
            if similar_projects:
                with st.expander("⚠️ Similar Projects Found", expanded=True):
                    st.write("Select a project to add tasks to, or create a new one:")
                    
                    # Add "Create New Project" option
                    project_options = [{"Project name": "📌 Create New Project", "id": None, "url": None}] + [
                        {
                            "Project name": result.get("properties", {}).get("Project name", {}).get("title", [{}])[0].get("text", {}).get("content", "Untitled"),
                            "id": result.get("id"),
                            "url": result.get("url")
                        }
                        for result in similar_projects
                    ]
                    
                    selected_idx = st.radio(
                        "Project Selection",
                        range(len(project_options)),
                        format_func=lambda i: project_options[i]["Project name"],
                        key=f"project_select_{current_file.name}"
                    )
                    
                    selected_project = project_options[selected_idx]
                    
                    if selected_idx > 0:  # If an existing project is selected
                        st.markdown(f"""
                        **Selected Project Details:**
                        - ID: `{selected_project['id']}`
                        - [View in Notion]({selected_project['url']})
                        """)
            else:
                st.success("✅ No similar projects found - A new project will be created")
            
            # Check if file has been uploaded to Notion
            if current_file.name in st.session_state.uploaded_to_notion:
                st.success(f"✅ Already uploaded to Notion")
                # Show previous logs if any
                if st.session_state.upload_logs[current_file.name]:
                    st.session_state.widget_counter += 1
                    log_key = f"log_{current_file.name}_{st.session_state.widget_counter}"
                    log_container.text_area(
                        "Upload Progress Log",
                        "\n".join(st.session_state.upload_logs[current_file.name]),
                        height=200,
                        key=log_key
                    )
            else:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🚀 Upload to Notion"):
                        try:
                            project = None
                            if selected_project and selected_project.get("id"):
                                # Use existing project
                                update_log(f"Adding tasks to existing project '{selected_project['Project name']}'...", "info")
                                project = selected_project
                            else:
                                # Create new project
                                update_log(f"Creating new project '{project_info['name']}'...", "info")
                                try:
                                    project = notion.projects.create_project(project_info)
                                    if not project:
                                        update_log("Failed to create project", "error")
                                        st.error("❌ Failed to create project")
                                        return
                                except Exception as e:
                                    update_log(f"Error creating project: {str(e)}", "error")
                                    st.error(f"❌ Error creating project: {str(e)}")
                                    return
                        
                            if project:
                                if selected_project and selected_project.get("id"):
                                    update_log(f"Using existing project '{selected_project['Project name']}'", "success")
                                else:
                                    update_log(f"Project '{project_info['name']}' created successfully", "success")
                                
                                total_tasks = len(notion_structure)
                                success_count = 0
                                skip_count = 0
                                error_count = 0
                                
                                update_log(f"Creating {total_tasks} tasks (excluding phases)...", "info")
                                for idx, task in notion_structure.iterrows():
                                    try:
                                        response = notion.tasks.create_or_update_task(task, project["id"])
                                        if response is None:
                                            update_log(f"Task '{task['Tarefa']}' already exists, skipped", "warning")
                                            skip_count += 1
                                        else:
                                            update_log(f"Task '{task['Tarefa']}' created successfully", "success")
                                            success_count += 1
                                    except Exception as e:
                                        update_log(f"Error creating task '{task['Tarefa']}': {str(e)}", "error")
                                        error_count += 1
                                
                                # Final summary
                                update_log("\n=== Final Summary ===", "info")
                                update_log(f"Total tasks processed: {total_tasks}", "info")
                                update_log(f"Successfully created: {success_count}", "success")
                                update_log(f"Skipped: {skip_count}", "warning")
                                update_log(f"Errors: {error_count}", "error")
                                
                                # Mark as uploaded if at least one task was created
                                if success_count > 0:
                                    st.session_state.uploaded_to_notion.add(current_file.name)
                                    
                                # Move to next file
                                st.session_state.current_file_index = min(st.session_state.current_file_index + 1, total_files - 1)
                                st.experimental_rerun()
                                
                        except Exception as e:
                            update_log(f"Error uploading to Notion: {str(e)}", "error")
                
                with col2:
                    if st.button("⏭️ Skip File"):
                        st.session_state.current_file_index = min(st.session_state.current_file_index + 1, total_files - 1)
                        st.experimental_rerun()
        
        # Download option for the current file
        st.subheader("📥 Download Processed Data")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"notion_ready_structure_{current_file.name}_{timestamp}.csv"
        
        csv = notion_structure.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=csv_filename,
            mime="text/csv"
        )

if __name__ == "__main__":
    main()

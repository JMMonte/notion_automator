import os
import streamlit as st
import pandas as pd
from notion_client import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PROJECTS_DB = "544bf32b74694b6287112b40ac3b6f27"
NOTION_TASKS_DB = "012071410dfd4a4f857eefe333a5f6c4"

notion = Client(auth=NOTION_TOKEN)

# Status mapping configuration
STATUS_CONFIG = {
    "default": "Not started",
    "mapping": {
        "Não iniciado": "Not started",
        "Em progresso": "In progress",
        "Em andamento": "In progress",
        "Concluído": "Done",
        "Pausado": "Paused",
        "PARADO": "Paused",
        "Cancelado": "Canceled",
        "Arquivado": "Archived"
    }
}

# Step 1: Load and clean the "PLANEAMENTO" sheet
def load_and_clean_sheet(file) -> pd.DataFrame:
    try:
        # First read to find the header row
        planeamento_data: pd.DataFrame = pd.read_excel(
            io=file,
            sheet_name="PLANEAMENTO",
            header=None,
            engine="openpyxl"
        )
        
        # Find the header row containing "FASES/TAREFAS"
        mask = planeamento_data.apply(lambda row: row.astype(str).str.contains("FASES/TAREFAS", na=False).any(), axis=1)
        header_row_idx: int = mask.loc[mask].index.tolist()[0]
        
        # Second read with the correct header
        initial_cleaned_data: pd.DataFrame = pd.read_excel(
            io=file,
            sheet_name="PLANEAMENTO",
            header=header_row_idx,
            engine="openpyxl"
        )
        return initial_cleaned_data.dropna(how="all").reset_index(drop=True)
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")
        raise

# Step 2: Determine "Type" (Fase, Tarefa, or Milestone) and identify parent tasks
def classify_and_identify_parent_tasks(data):
    data_with_types = data.copy()

    # Initialize columns
    data_with_types["Type"] = None
    data_with_types["Parent Task"] = None
    data_with_types["Level"] = 0

    # Dictionary to store the last item at each level
    level_parents = {}
    
    for idx, row in data_with_types.iterrows():
        edt = row["EDT"]
        task_name = str(row["FASES/TAREFAS"])
        edt_str = str(edt)
        edt_parts = edt_str.split('.')
        
        # Determine type based on content and structure
        if "MILESTONE:" in task_name:
            task_type = "Milestone"
        # PR.XXXX is project level
        elif len(edt_parts) == 2:
            task_type = "Fase"
        # PR.XXXX.Y is phase level
        elif len(edt_parts) == 3 and edt_parts[2].isdigit() and len(edt_parts[2]) <= 2:
            task_type = "Fase"
        elif pd.isna(row["RESPONSÁVEL"]):
            task_type = "Fase"
        else:
            task_type = "Tarefa"
            
        data_with_types.at[idx, "Type"] = task_type
        
        # Special handling for milestone with .M
        if edt_str.endswith('.M'):
            # For PR.0001.1.4.M, we want PR.0001.1 as parent
            edt_parts = edt_str[:-2].split('.')  # Remove .M first
            if len(edt_parts) > 2:  # If we have more than just project and phase
                parent_edt = '.'.join(edt_parts[:-1])  # Join all parts except the last number
                data_with_types.at[idx, "Parent Task"] = parent_edt
                data_with_types.at[idx, "Level"] = len(edt_parts) - 2  # Set level one above parent
            else:
                data_with_types.at[idx, "Level"] = len(edt_parts) - 1
        else:
            # Normal EDT handling
            level = len(edt_parts) - 1
            data_with_types.at[idx, "Level"] = level
            
            # Clear any stored parents at deeper levels
            levels_to_remove = [l for l in level_parents.keys() if l >= level]
            for l in levels_to_remove:
                del level_parents[l]
            
            # Set parent task based on the previous level
            if level > 0:
                parent_level = level - 1
                if parent_level in level_parents:
                    parent_edt = level_parents[parent_level]
                    # Only set parent if the EDTs share the same prefix
                    if str(edt).startswith(str(parent_edt).rsplit('.', 1)[0]):
                        data_with_types.at[idx, "Parent Task"] = parent_edt
            
            # Store this item as potential parent for next items
            level_parents[level] = edt

    return data_with_types

# Step 3: Handle planned and real dates
def process_dates(data):
    date_cleaned_data = data.copy()
    
    # Get the real dates columns by name
    real_start_col = "INÍCIO.1"  # Index 11
    real_end_col = "DATA FIM"    # Index 16
    
    # Get other columns by name patterns
    planned_start_col = "INÍCIO"  # Index 5
    planned_end_col = "FIM"      # Index 9
    status_col = "STATUS"        # Index 18

    # Process dates and convert to date objects (removing time)
    date_cleaned_data["Real Start"] = pd.to_datetime(date_cleaned_data[real_start_col]).dt.date
    date_cleaned_data["Real End"] = pd.to_datetime(date_cleaned_data[real_end_col]).dt.date
    date_cleaned_data["Planned Start"] = pd.to_datetime(date_cleaned_data[planned_start_col]).dt.date
    date_cleaned_data["Planned End"] = pd.to_datetime(date_cleaned_data[planned_end_col]).dt.date

    date_cleaned_data.rename(
        columns={status_col: "Status"},
        inplace=True,
    )

    # Ensure dates are valid and start <= end
    def validate_dates(start, end):
        if pd.notna(start) and pd.notna(end):
            if start > end:
                return start, None  # Ignore the end date if invalid
        return start if pd.notna(start) else None, end if pd.notna(end) else None

    date_cleaned_data["Planned Start"], date_cleaned_data["Planned End"] = zip(
        *date_cleaned_data.apply(lambda row: validate_dates(row["Planned Start"], row["Planned End"]), axis=1)
    )

    date_cleaned_data["Datas planeadas"] = date_cleaned_data.apply(
        lambda row: f"{row['Planned Start']} → {row['Planned End']}" if pd.notna(row["Planned End"]) else f"{row['Planned Start']}",
        axis=1,
    )
    date_cleaned_data["Datas reais"] = date_cleaned_data.apply(
        lambda row: f"{row['Real Start']} → {row['Real End']}" if pd.notna(row["Real End"]) else (f"{row['Real Start']}" if pd.notna(row["Real Start"]) else ""),
        axis=1,
    )

    return date_cleaned_data

# Step 4: Update EDT values based on hierarchy
def update_edt(data):
    edt_data = data.copy()
    edt_data["EDT"] = edt_data["EDT"]  # Use the original EDT directly

    for idx, row in edt_data.iterrows():
        if row["Type"] == "Milestone" and ".M" in str(row["EDT"]):
            edt_data.at[idx, "EDT"] = row["EDT"]  # Retain the .M suffix for milestones

    return edt_data

# Step 5: Extract project info
def extract_project_info(data):
    project_info = data.iloc[0].to_dict()
    remaining_data = data.iloc[1:].reset_index(drop=True)
    return project_info, remaining_data

# Step 6: Create Notion-ready structure
def create_notion_structure(data):
    columns = ["Tarefa", "Type", "Parent Task", "EDT", "Datas planeadas", "Datas reais", "Trabalho Realizado", "Status"]
    notion_structure = pd.DataFrame({col: [] for col in columns})

    notion_structure["Tarefa"] = data["FASES/TAREFAS"]
    notion_structure["Type"] = data["Type"]
    notion_structure["Parent Task"] = data["Parent Task"]
    notion_structure["EDT"] = data["EDT"]
    notion_structure["Datas planeadas"] = data["Datas planeadas"]
    notion_structure["Datas reais"] = data["Datas reais"]
    notion_structure["Trabalho Realizado"] = data.get("TRABALHO REALIZADO", "")  # Include "Trabalho Realizado" column
    notion_structure["Status"] = data.get("Status", STATUS_CONFIG["default"])

    return notion_structure

# Step 7: Notion Integration
# Search for a project by ID in the Notion database
def search_project(project_id):
    results = notion.databases.query(
        database_id=NOTION_PROJECTS_DB,
        filter={
            "property": "ID",
            "rich_text": {
                "equals": project_id
            }
        }
    )
    return results.get("results", [])

# Create a new project in Notion
def create_project(project_info):
    return notion.pages.create(
        **{
            "parent": {"database_id": NOTION_PROJECTS_DB},
            "properties": {
                "Project name": {"title": [{"text": {"content": project_info["FASES/TAREFAS"]}}]},
                "ID": {"rich_text": [{"text": {"content": project_info["EDT"]}}]},
            }
        }
    )

# Update an existing task by EDT
def update_task(task_id, task, project_id):
    status = STATUS_CONFIG["mapping"].get(task["Status"], STATUS_CONFIG["default"])
    parent_relation = []
    if task["Parent Task"]:
        parent_task_id = find_task_by_edt(task["Parent Task"])
        if parent_task_id:
            parent_relation = [{"id": parent_task_id}]

    notion.pages.update(
        page_id=task_id,
        properties={
            "Tarefa": {"title": [{"text": {"content": task["Tarefa"]}}]},
            "Type": {"select": {"name": task["Type"]}},
            "EDT": {"rich_text": [{"text": {"content": task["EDT"]}}]},
            "Project": {"relation": [{"id": project_id}]},
            "Parent task": {"relation": parent_relation},
            "Datas planeadas": {"date": {"start": task["Datas planeadas"].split(" → ")[0],
                                             "end": task["Datas planeadas"].split(" → ")[1] if " → " in task["Datas planeadas"] else None}},
            "Datas reais": {"date": {"start": task["Datas reais"].split(" → ")[0] if task["Datas reais"] else None,
                                         "end": task["Datas reais"].split(" → ")[1] if task["Datas reais"] and " → " in task["Datas reais"] else None}},
            "Progresso (dias)": {"number": float(task["Trabalho Realizado"])},
            "Status": {"status": {"name": status}}
        }
    )

# Check for existing tasks by EDT and return its ID if found
def find_task_by_edt(edt):
    results = notion.databases.query(
        database_id=NOTION_TASKS_DB,
        filter={
            "property": "EDT",
            "rich_text": {
                "equals": edt
            }
        }
    )
    if results.get("results"):
        return results["results"][0]["id"]
    return None

# Upload or update tasks in Notion
def upload_tasks(tasks, project_id):
    # Create progress containers
    progress_container = st.empty()
    status_container = st.empty()
    error_container = st.empty()
    
    # Initialize counters
    total_tasks = len(tasks)
    created_count = 0
    updated_count = 0
    error_count = 0
    errors = []

    for idx, (_, task) in enumerate(tasks.iterrows()):
        # Update progress
        progress = (idx + 1) / total_tasks
        progress_container.progress(progress)
        status_container.write(f"Processing {idx + 1}/{total_tasks} tasks: {task['EDT']}")
        
        try:
            task_id = find_task_by_edt(task["EDT"])
            if task_id:
                update_task(task_id, task, project_id)
                updated_count += 1
            else:
                status = STATUS_CONFIG["mapping"].get(task["Status"], STATUS_CONFIG["default"])
                parent_relation = []
                if task["Parent Task"]:
                    parent_task_id = find_task_by_edt(task["Parent Task"])
                    if parent_task_id:
                        parent_relation = [{"id": parent_task_id}]

                notion.pages.create(
                    **{
                        "parent": {"database_id": NOTION_TASKS_DB},
                        "properties": {
                            "Tarefa": {"title": [{"text": {"content": task["Tarefa"]}}]},
                            "Type": {"select": {"name": task["Type"]}},
                            "EDT": {"rich_text": [{"text": {"content": task["EDT"]}}]},
                            "Project": {"relation": [{"id": project_id}]},
                            "Parent task": {"relation": parent_relation},
                            "Datas planeadas": {"date": {"start": task["Datas planeadas"].split(" → ")[0],
                                                         "end": task["Datas planeadas"].split(" → ")[1] if " → " in task["Datas planeadas"] else None}},
                            "Datas reais": {"date": {"start": task["Datas reais"].split(" → ")[0] if task["Datas reais"] else None,
                                                     "end": task["Datas reais"].split(" → ")[1] if task["Datas reais"] and " → " in task["Datas reais"] else None}},
                            "Progresso (dias)": {"number": float(task["Trabalho Realizado"])},
                            "Status": {"status": {"name": status}}
                        }
                    }
                )
                created_count += 1
        except Exception as e:
            error_count += 1
            errors.append(f"Error processing task {task['EDT']}: {str(e)}")
            
        # Update status message
        status_text = f"""
        **Progress Summary:**
        - Created: {created_count}
        - Updated: {updated_count}
        - Errors: {error_count}
        """
        status_container.markdown(status_text)
        
        # Show errors if any
        if errors:
            error_text = "**Errors:**\n" + "\n".join(f"- {error}" for error in errors[-5:])
            if len(errors) > 5:
                error_text += f"\n- ... and {len(errors) - 5} more errors"
            error_container.markdown(error_text)
    
    # Final summary
    progress_container.empty()
    status_container.markdown(f"""
    **Upload Complete!**
    - Total tasks processed: {total_tasks}
    - Successfully created: {created_count}
    - Successfully updated: {updated_count}
    - Errors encountered: {error_count}
    """)

# Main processing function
def process_excel(file):
    progress_container = st.empty()
    status_container = st.empty()
    
    # Define processing steps
    total_steps = 6
    current_step = 0
    
    def update_progress(step_name):
        nonlocal current_step
        current_step += 1
        progress = current_step / total_steps
        progress_container.progress(progress)
        status_container.markdown(f"**Step {current_step}/{total_steps}:** {step_name}")
    
    update_progress("Loading and cleaning Excel data")
    initial_cleaned_data = load_and_clean_sheet(file)
    
    update_progress("Classifying tasks and identifying parent tasks")
    classified_data = classify_and_identify_parent_tasks(initial_cleaned_data)
    
    update_progress("Processing dates")
    dated_data = process_dates(classified_data)
    
    update_progress("Updating EDT values")
    updated_edt_data = update_edt(dated_data)
    
    update_progress("Extracting project information")
    project_info, task_data = extract_project_info(updated_edt_data)
    
    update_progress("Creating Notion structure")
    notion_structure = create_notion_structure(task_data)
    
    # Clear progress display
    progress_container.empty()
    status_container.empty()
    
    return project_info, notion_structure

# Streamlit app
st.title("Notion Data Transformation App")

uploaded_files = st.file_uploader("Upload Excel files", type="xlsx", accept_multiple_files=True)

if uploaded_files:
    # Overall progress
    total_files = len(uploaded_files)
    overall_progress = st.progress(0)
    overall_status = st.empty()
    
    for file_index, uploaded_file in enumerate(uploaded_files, 1):
        st.markdown(f"### Processing file: {uploaded_file.name}")
        file_status = st.empty()
        
        try:
            project_info, processed_data = process_excel(uploaded_file)
            file_status.success(f"Successfully processed {uploaded_file.name}")
            
            with st.expander(f"Details for {uploaded_file.name}"):
                st.write("Processed Data:")
                st.dataframe(processed_data)

                # Notion Integration
                project_matches = search_project(project_info["EDT"])
                if project_matches:
                    st.write("Matching projects found in Notion:")
                    project_options = []
                    project_ids = {}  # Using dict to store id mapping
                    
                    # Add existing projects
                    for match in project_matches:
                        project_name = match["properties"]["Project name"]["title"][0]["text"]["content"]
                        project_options.append(project_name)
                        project_ids[project_name] = match["id"]
                    
                    # Add create new option
                    project_options.append("Create a new project")
                    
                    selected_option = st.radio(
                        "Select a project or create a new one:",
                        project_options,
                        index=0,  # Default to first matched project
                        key=f"project_select_{file_index}"  # Unique key for each file
                    )

                    if selected_option == "Create a new project":
                        if st.button("Create new project and upload tasks", key=f"create_btn_{file_index}"):
                            new_project = create_project(project_info)
                            upload_tasks(processed_data, new_project["id"])
                            st.success("Project created and tasks uploaded successfully!")
                    else:
                        selected_project_id = project_ids[selected_option]
                        if st.button("Upload tasks to selected project", key=f"upload_btn_{file_index}"):
                            upload_tasks(processed_data, selected_project_id)
                            st.success("Tasks uploaded successfully!")
                else:
                    st.write("No matching project found in Notion.")
                    if st.button("Create new project and upload tasks", key=f"create_btn_{file_index}"):
                        new_project = create_project(project_info)
                        upload_tasks(processed_data, new_project["id"])
                        st.success("Project created and tasks uploaded successfully!")
        
        except Exception as e:
            file_status.error(f"Error processing {uploaded_file.name}: {str(e)}")
        
        # Update overall progress
        overall_progress.progress(file_index / total_files)
        overall_status.text(f"Processed {file_index} of {total_files} files")
    
    overall_status.success(f"Completed processing all {total_files} files")
else:
    st.info("Please upload one or more Excel files to begin processing")

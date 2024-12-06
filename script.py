import pandas as pd

# Load the Excel file
excel_path = "path_to_your_file.xlsx"

# Step 1: Load and clean the "PLANEAMENTO" sheet
planeamento_data = pd.read_excel(excel_path, sheet_name="PLANEAMENTO", header=None)
header_row_idx = planeamento_data[planeamento_data.apply(lambda row: row.str.contains("FASES/TAREFAS", na=False).any(), axis=1)].index[0]
cleaned_data = pd.read_excel(excel_path, sheet_name="PLANEAMENTO", header=header_row_idx)
cleaned_data = cleaned_data.dropna(how="all").reset_index(drop=True)

# Step 2: Identify "Fases" and assign them to tasks
fases = cleaned_data[cleaned_data["FASES/TAREFAS"].notnull() & cleaned_data["RESPONSÁVEL"].isnull()]["FASES/TAREFAS"].reset_index()
cleaned_data["Fase"] = None
for idx, fase_row in fases.iterrows():
    phase_name = fase_row["FASES/TAREFAS"]
    start_idx = fase_row["index"]
    end_idx = fases.loc[idx + 1, "index"] if idx + 1 < len(fases) else len(cleaned_data)
    cleaned_data.loc[start_idx:end_idx, "Fase"] = phase_name

# Step 3: Determine "Type" (Tarefa or Milestone)
cleaned_data["Type"] = cleaned_data.apply(
    lambda row: "Milestone" if pd.isna(row["FASES/TAREFAS"]) else "Tarefa", axis=1
)

# Step 4: Handle planned and real dates
planned_start_col = [col for col in cleaned_data.columns if "INÍCIO" in col][0]
real_start_col = [col for col in cleaned_data.columns if "INÍCIO" in col][1]
planned_end_col = [col for col in cleaned_data.columns if "FIM" in col][0]
real_end_col = [col for col in cleaned_data.columns if "FIM" in col][1]

cleaned_data.rename(
    columns={planned_start_col: "Planned Start", real_start_col: "Real Start",
             planned_end_col: "Planned End", real_end_col: "Real End"},
    inplace=True,
)

# Generate "Datas planeadas" and "Datas reais" in the format "Start Date → End Date"
cleaned_data["Datas planeadas"] = cleaned_data.apply(
    lambda row: f"{row['Planned Start']} → {row['Planned End']}" if pd.notna(row["Planned End"]) else f"{row['Planned Start']}",
    axis=1,
)
cleaned_data["Datas reais"] = cleaned_data.apply(
    lambda row: f"{row['Real Start']} → {row['Real End']}" if pd.notna(row["Real End"]) else f"{row['Real Start']}",
    axis=1,
)

# Step 5: Remove metadata rows and keep valid tasks and milestones
metadata_criteria = (cleaned_data["FASES/TAREFAS"] == cleaned_data["Fase"]) & (cleaned_data["RESPONSÁVEL"].isna())
filtered_data = cleaned_data[~metadata_criteria].reset_index(drop=True)

# Step 6: Create Notion-ready structure
notion_structure = pd.DataFrame(columns=[
    "Tarefa", "Status", "Fase", "Assignee", "Datas planeadas", "Datas reais",
    "Atraso Inicio (days)", "Atraso fim (days)", "Duração Planeada (Business days)",
    "Duração Real (Business days)", "Progresso (dias)", "Progresso (%)", "Project",
    "Blocked by", "Blocking", "ID", "Type", "Area Patrocinador", "Area Responsável",
    "Project ID"
])

# Populate the Notion structure
notion_structure["Tarefa"] = filtered_data["FASES/TAREFAS"]
notion_structure["Status"] = filtered_data["STATUS"]
notion_structure["Fase"] = filtered_data["Fase"]
notion_structure["Assignee"] = filtered_data["RESPONSÁVEL"]
notion_structure["Datas planeadas"] = filtered_data["Datas planeadas"]
notion_structure["Datas reais"] = filtered_data["Datas reais"]
notion_structure["Type"] = filtered_data["Type"]

# Add placeholder fields for remaining columns
for col in notion_structure.columns:
    if col not in ["Tarefa", "Status", "Fase", "Assignee", "Datas planeadas", "Datas reais", "Type"]:
        notion_structure[col] = None

# Save or return the final Notion-ready structure
notion_structure.to_csv("notion_ready_structure.csv", index=False)
from typing import Dict, Any
from dataclasses import dataclass

def deep_update(source: Dict, updates: Dict) -> Dict:
    """Deep update a nested dictionary with another dictionary"""
    for key, value in updates.items():
        if isinstance(value, dict) and key in source and isinstance(source[key], dict):
            source[key] = deep_update(source[key], value)
        else:
            source[key] = value
    return source

def merge_config(base_config: Dict[str, Any], user_config: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge user config with base config"""
    config = base_config.copy()
    return deep_update(config, user_config)

# Configuration Types
@dataclass
class TaskColumns:
    edt: str
    title: str
    type: str
    status: str
    progress: str
    planned_start: str
    planned_end: str
    actual_start: str
    actual_end: str

@dataclass
class HeaderConfig:
    column: str
    value: str

@dataclass
class MilestoneConfig:
    suffix: str
    type_field: str
    type_value: str

@dataclass
class PhaseConfig:
    edt_parts: int
    attribute_name: str

@dataclass
class TaskNamingConfig:
    empty_task_prefix: str
    empty_milestone_prefix: str
    use_edt_as_fallback: bool
    unique_suffix_pattern: str

@dataclass
class ProjectFieldConfig:
    label: str
    sheet: str
    column: int
    value_column: int

@dataclass
class ProjectConfig:
    name: ProjectFieldConfig
    id: ProjectFieldConfig

@dataclass
class TasksConfig:
    header_identifier: HeaderConfig
    columns: TaskColumns

@dataclass
class FieldsConfig:
    project: ProjectConfig
    tasks: TasksConfig

@dataclass
class ExcelConfig:
    task_columns: TaskColumns
    header: HeaderConfig
    milestone: MilestoneConfig
    phase: PhaseConfig
    task_naming: TaskNamingConfig
    type_mapping: Dict[str, str]
    status_mapping: Dict[str, str]
    project_info_sheet: str
    tasks_sheet: str
    fields: FieldsConfig
    field_mappings: Dict[str, Dict[str, Any]]

@dataclass
class NotionConfig:
    database_ids: Dict[str, str]
    property_types: Dict[str, Any]
    api_settings: Dict[str, float]
    status_mapping: Dict[str, str]
    type_mapping: Dict[str, str]

def create_excel_config(config: Dict[str, Any]) -> ExcelConfig:
    """Create ExcelConfig from dictionary configuration"""
    task_config = config["fields"]["tasks"]["columns"]
    header_config = config["fields"]["tasks"]["header_identifier"]
    milestone_config = config["type_config"]["milestone_identifiers"]
    phase_config = config["phase_config"]
    naming_config = config["task_naming"]

    return ExcelConfig(
        task_columns=TaskColumns(
            edt=task_config["edt"],
            title=task_config["title"],
            type=task_config["type"],
            status=task_config["status"],
            progress=task_config["progress"],
            planned_start=task_config["planned_start"],
            planned_end=task_config["planned_end"],
            actual_start=task_config["actual_start"],
            actual_end=task_config["actual_end"]
        ),
        header=HeaderConfig(
            column=header_config["column"],
            value=header_config["value"]
        ),
        milestone=MilestoneConfig(
            suffix=milestone_config["suffix"],
            type_field=milestone_config["type_field"],
            type_value=milestone_config["type_value"]
        ),
        phase=PhaseConfig(
            edt_parts=phase_config["edt_parts"],
            attribute_name=phase_config["attribute_name"]
        ),
        task_naming=TaskNamingConfig(
            empty_task_prefix=naming_config["empty_task_prefix"],
            empty_milestone_prefix=naming_config["empty_milestone_prefix"],
            use_edt_as_fallback=naming_config["use_edt_as_fallback"],
            unique_suffix_pattern=naming_config["unique_suffix_pattern"]
        ),
        type_mapping=config["type_config"]["mappings"],
        status_mapping=config["status_config"]["mappings"],
        project_info_sheet=config["sheets"]["project_info"],
        tasks_sheet=config["sheets"]["tasks"],
        fields=FieldsConfig(
            project=ProjectConfig(
                name=ProjectFieldConfig(
                    label=config["fields"]["project"]["name"]["label"],
                    sheet=config["fields"]["project"]["name"]["sheet"],
                    column=config["fields"]["project"]["name"]["column"],
                    value_column=config["fields"]["project"]["name"]["value_column"]
                ),
                id=ProjectFieldConfig(
                    label=config["fields"]["project"]["id"]["label"],
                    sheet=config["fields"]["project"]["id"]["sheet"],
                    column=config["fields"]["project"]["id"]["column"],
                    value_column=config["fields"]["project"]["id"]["value_column"]
                )
            ),
            tasks=TasksConfig(
                header_identifier=HeaderConfig(
                    column=config["fields"]["tasks"]["header_identifier"]["column"],
                    value=config["fields"]["tasks"]["header_identifier"]["value"]
                ),
                columns=TaskColumns(
                    edt=task_config["edt"],
                    title=task_config["title"],
                    type=task_config["type"],
                    status=task_config["status"],
                    progress=task_config["progress"],
                    planned_start=task_config["planned_start"],
                    planned_end=task_config["planned_end"],
                    actual_start=task_config["actual_start"],
                    actual_end=task_config["actual_end"]
                )
            )
        ),
        field_mappings=config["field_mappings"]
    )

def create_notion_config(config: Dict[str, Any]) -> NotionConfig:
    """Create NotionConfig from dictionary configuration"""
    return NotionConfig(
        database_ids=config["database_ids"],
        property_types=config["property_types"],
        api_settings=config["api_settings"],
        status_mapping=config["status_config"]["mappings"],
        type_mapping=config["type_config"]["mappings"]
    )

# Common configuration shared between Excel and Notion
COMMON_CONFIG: Dict[str, Any] = {
    "status_config": {
        "default": "Not started",
        "mappings": {
            "Not started": "Not started",
            "In progress": "In progress",
            "Done": "Done",
            "Concluído": "Done",
            "Canceled": "Canceled",
            "Archived": "Archived",
            "Blocked": "Blocked",
            "Parado": "Paused"
        }
    },
    "type_config": {
        "default": "Tarefa",
        "mappings": {
            "M": "Milestone",
            "T": "Tarefa"
        },
        "milestone_identifiers": {
            "suffix": ".M",
            "type_field": "TIPO",
            "type_value": "M"
        }
    },
    "phase_config": {
        "edt_parts": 2,  # PR.XXXX.Y format where Y is the phase number
        "attribute_name": "Fase"  # Name of the phase attribute in Notion
    },
    "task_naming": {
        "empty_task_prefix": "Task",  # Prefix for tasks with empty names
        "empty_milestone_prefix": "Milestone",  # Prefix for milestones with empty names
        "use_edt_as_fallback": True,  # Use EDT as name if task name is empty
        "unique_suffix_pattern": "_{index}"  # Pattern for making duplicate names unique
    },
    "field_mappings": {
        # Excel column name -> Notion property name
        "title": {
            "excel": "FASES/TAREFAS",
            "notion": "Tarefa",
            "type": "title"
        },
        "type": {
            "excel": "TIPO",
            "notion": "Type",
            "type": "select"
        },
        "edt": {
            "excel": "EDT",
            "notion": "EDT",
            "type": "rich_text"
        },
        "status": {
            "excel": "STATUS",
            "notion": "Status",
            "type": "status"
        },
        "phase": {
            "excel": "Fase",  # This is computed, not from Excel
            "notion": "Fase",
            "type": "select"
        },
        "planned_dates": {
            "excel": ["INÍCIO", "FIM"],  # Array indicates composite field
            "notion": "Datas planeadas",
            "type": "date"
        },
        "actual_dates": {
            "excel": ["INÍCIO", "DATA FIM"],  # Array indicates composite field
            "notion": "Datas reais",
            "type": "date"
        },
        "progress": {
            "excel": "TRABALHO REALIZADO",
            "notion": "Progresso (dias)",
            "type": "number"
        }
    }
}

# Excel-specific configuration
EXCEL_CONFIG: Dict[str, Any] = {
    "sheets": {
        "project_info": "FICHA PROJETO",
        "tasks": "PLANEAMENTO"
    },
    "fields": {
        "project": {
            "name": {
                "label": "NOME DO PROJETO",
                "sheet": "project_info",
                "column": 2,
                "value_column": 3
            },
            "id": {
                "label": "ID PROJETO",
                "sheet": "project_info",
                "column": 7,
                "value_column": 8
            }
        },
        "tasks": {
            "header_identifier": {
                "column": 3,
                "value": "FASES/TAREFAS"
            },
            "columns": {
                "edt": "EDT",
                "title": "FASES/TAREFAS",
                "type": "TIPO",
                "status": "STATUS",
                "progress": "TRABALHO REALIZADO",
                "planned_start": "INÍCIO",
                "planned_end": "FIM",
                "actual_start": "INÍCIO",
                "actual_end": "DATA FIM"
            }
        }
    }
}

# Notion-specific configuration
NOTION_CONFIG: Dict[str, Any] = {
    "database_ids": {
        "projects": "544bf32b74694b6287112b40ac3b6f27",
        "tasks": "012071410dfd4a4f857eefe333a5f6c4"
    },
    "property_types": {
        # Get field types from field_mappings
        "fields": {
            mapping["notion"]: mapping["type"]
            for mapping in COMMON_CONFIG["field_mappings"].values()
        },
        # Add any additional Notion-specific fields
        "Project": "relation"
    },
    "api_settings": {
        "rate_limit_delay": 0.334
    }
}

# Merge common config into specific configs
EXCEL_CONFIG = merge_config(COMMON_CONFIG, EXCEL_CONFIG)
NOTION_CONFIG = merge_config(COMMON_CONFIG, NOTION_CONFIG)

# Create typed configs
TYPED_EXCEL_CONFIG = create_excel_config(EXCEL_CONFIG)
TYPED_NOTION_CONFIG = create_notion_config(NOTION_CONFIG)

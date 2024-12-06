from typing import Dict, Any

NOTION_CONFIG: Dict[str, Any] = {
    "database_ids": {
        "projects": "544bf32b74694b6287112b40ac3b6f27",  # Projects database
        "tasks": "012071410dfd4a4f857eefe333a5f6c4"      # Tasks database
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
        "title_field": "Tarefa",
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
        "rate_limit_delay": 0.334,  # Notion allows max 3 requests per second (1/3 second per request)
        "max_retries": 3,
        "retry_delay": 1.0,
        "batch_size": 10  # Reduced batch size to avoid hitting rate limits
    },
    "search_settings": {
        "max_results": 100,
        "sort_direction": "descending"
    }
}

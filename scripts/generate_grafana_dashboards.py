#!/usr/bin/env python3
"""
Grafana Dashboard Generator from SQL Queries
Reads SQL queries and creates Grafana dashboards automatically
with appropriate visualizations for each query type
"""

import json
import requests
from pathlib import Path
import base64

# Configuration
GRAFANA_URL = "http://localhost:3000"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "admin"
DATASOURCE_NAME = "PostgreSQL-Gold"

def get_datasource_uid(user, password):
    """Get the UID of PostgreSQL-Gold datasource"""
    try:
        credentials = f"{user}:{password}"
        b64_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {b64_credentials}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            f"{GRAFANA_URL}/api/datasources",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            datasources = response.json()
            for ds in datasources:
                if ds['name'] == DATASOURCE_NAME:
                    print(f"✓ Found datasource: {ds['name']} (UID: {ds['uid']})")
                    return ds['uid']
            print(f"⚠ Datasource '{DATASOURCE_NAME}' not found")
            if datasources:
                print(f"  Available datasources: {[ds['name'] for ds in datasources]}")
            return None
        else:
            print(f"Error fetching datasources: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_visualization_type(title):
    """Determine visualization type based on query title"""
    title_lower = title.lower()
    
    # Data Quality/Validation - use Table (returns multiple rows) - CHECK THIS FIRST
    if any(x in title_lower for x in ['null', 'duplicate', 'validation', 'check', 'test', 'quality']):
        return 'table'
    
    # Summary/KPI metrics - use Stat
    if any(x in title_lower for x in ['summary', 'total', 'count', 'health', 'score', 'kpi']):
        return 'stat'
    
    # Distribution/Proportion - use Pie Chart
    if any(x in title_lower for x in ['distribution', 'gender', 'proportion', 'percentage']):
        return 'piechart'
    
    # Performance/Rankings - use Bar Chart
    if any(x in title_lower for x in ['performance', 'territory', 'leaderboard', 'ranking', 'top']):
        return 'barchart'
    
    # Time Series - use Time Series
    if any(x in title_lower for x in ['time series', 'ingestion', 'trend', 'over time']):
        return 'timeseries'
    
    # Coverage/Success Rate - use Gauge
    if any(x in title_lower for x in ['coverage', 'success', 'match', 'rate']):
        return 'gauge'
    
    # Default to Table
    return 'table'

def read_sql_queries(filepath):
    """Parse SQL file and extract queries with visualization hints"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    queries = []
    lines = content.split('\n')
    
    current_query = []
    current_title = ""
    current_description = ""
    
    for line in lines:
        # Match query markers like "-- 1.1: Title" or "-- 2.3: Title"
        if line.startswith('-- ') and any(c.isdigit() for c in line[:10]):
            if current_query and current_title:
                query_text = '\n'.join(current_query).strip()
                if query_text:
                    viz_type = get_visualization_type(current_title)
                    queries.append({
                        'title': current_title,
                        'description': current_description,
                        'query': query_text,
                        'visualization': viz_type
                    })
            
            # Extract title
            if ':' in line:
                current_title = line.replace('-- ', '').split(':', 1)[1].strip()
            else:
                current_title = line.replace('-- ', '').strip()
            
            current_description = ""
            current_query = []
        elif line.startswith('-- ') and current_title:
            # Description line
            current_description = line.replace('-- ', '').strip()
        elif line.startswith('--'):
            # Skip other comments
            continue
        elif line.strip():
            current_query.append(line)
    
    # Add last query
    if current_query and current_title:
        query_text = '\n'.join(current_query).strip()
        if query_text:
            viz_type = get_visualization_type(current_title)
            queries.append({
                'title': current_title,
                'description': current_description,
                'query': query_text,
                'visualization': viz_type
            })
    
    return queries

def create_dashboard_json(queries, datasource_uid, title="Reporting Dashboard"):
    """Create Grafana dashboard JSON from queries with appropriate visualizations"""
    
    if not datasource_uid:
        print("❌ Cannot create dashboard without datasource UID")
        return None
    
    panels = []
    panel_id = 1
    
    # Create panels for all queries (not limited to 10)
    for idx, query_info in enumerate(queries):
        viz_type = query_info.get('visualization', 'table')
        
        # Configure visualization-specific options
        options = {}
        field_config = {"defaults": {}, "overrides": []}
        
        if viz_type == 'stat':
            options = {
                "textMode": "value_and_name",
                "graphMode": "area",
                "orientation": "auto",
                "colorMode": "value"
            }
        elif viz_type == 'gauge':
            options = {
                "showThresholdLabels": False,
                "showThresholdMarkers": True,
                "orientation": "auto"
            }
            field_config["defaults"]["custom"] = {
                "hideFrom": {"legend": False, "tooltip": False, "viz": False}
            }
        elif viz_type == 'piechart':
            options = {
                "displayLabels": ["name", "percent"],
                "legend": {"displayMode": "list", "placement": "bottom"},
                "pieType": "pie",
                "tooltip": {"mode": "single"}
            }
        elif viz_type == 'barchart':
            options = {
                "groupWidth": 0.7,
                "barRadius": 0,
                "xTickLabelRotation": 0,
                "xTickLabelSpacing": 0,
                "stacking": "none",
                "showTooltip": True,
                "legend": {"calcs": [], "displayMode": "list", "placement": "bottom"}
            }
        elif viz_type == 'timeseries':
            options = {
                "legend": {"calcs": [], "displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi"}
            }
        else:  # table
            options = {
                "showHeader": True,
                "sortBy": [],
                "maxHeight": None
            }
        
        panel = {
            "id": panel_id,
            "title": query_info['title'][:60],
            "description": query_info.get('description', ''),
            "type": viz_type,
            "gridPos": {
                "x": (idx % 3) * 8,
                "y": (idx // 3) * 8,
                "w": 8,
                "h": 8
            },
            "targets": [
                {
                    "rawSql": query_info['query'],
                    "refId": "A",
                    "datasourceUid": datasource_uid,
                    "format": "table"
                }
            ],
            "fieldConfig": field_config,
            "options": options
        }
        panels.append(panel)
        panel_id += 1
    
    dashboard = {
        "dashboard": {
            "title": title,
            "tags": ["reporting", "etl", "insurance"],
            "timezone": "browser",
            "panels": panels,
            "refresh": "5m",
            "time": {
                "from": "now-24h",
                "to": "now"
            },
            "uid": None,
            "version": 0
        },
        "overwrite": True
    }
    
    return dashboard

def post_dashboard(dashboard_json, user, password):
    """Post dashboard to Grafana via API using basic auth"""
    try:
        # Create basic auth header
        credentials = f"{user}:{password}"
        b64_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {b64_credentials}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{GRAFANA_URL}/api/dashboards/db",
            json=dashboard_json,
            headers=headers,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ Dashboard created successfully!")
            print(f"📊 View at: {GRAFANA_URL}/d/{result['uid']}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        print(f"   Make sure Grafana is running on {GRAFANA_URL}")
        return False

def main():
    """Main function"""
    sql_file = Path("sql/01_reporting_queries.sql")
    
    if not sql_file.exists():
        print(f"❌ SQL file not found: {sql_file}")
        return
    
    print(f"📖 Reading queries from {sql_file}...\n")
    queries = read_sql_queries(sql_file)
    print(f"✓ Found {len(queries)} queries\n")
    
    # List all queries with their visualizations
    print("=" * 80)
    print("AVAILABLE QUERIES AND VISUALIZATIONS")
    print("=" * 80)
    for idx, q in enumerate(queries, 1):
        print(f"\n{idx}. {q['title']}")
        if q.get('description'):
            print(f"   Description: {q['description']}")
        print(f"   Visualization: {q['visualization'].upper()}")
    
    print("\n" + "=" * 80)
    print(f"\n🔗 Connecting to Grafana at {GRAFANA_URL}...")
    datasource_uid = get_datasource_uid(GRAFANA_USER, GRAFANA_PASSWORD)
    
    if not datasource_uid:
        print("\n❌ Cannot proceed without datasource. Make sure:")
        print("   1. Grafana is running")
        print("   2. PostgreSQL-Gold datasource is configured")
        return
    
    print(f"\n📊 Creating dashboard with {len(queries)} panels...")
    dashboard = create_dashboard_json(queries, datasource_uid, "Insurance Reporting Dashboard")
    
    if not dashboard:
        return
    
    success = post_dashboard(dashboard, GRAFANA_USER, GRAFANA_PASSWORD)
    
    if success:
        print("\n✅ Setup complete!")
        print(f"📊 Dashboard contains {len(queries)} panels with optimized visualizations")
    else:
        print("\n💡 Alternative: Manually create queries in Grafana UI")

if __name__ == "__main__":
    main()

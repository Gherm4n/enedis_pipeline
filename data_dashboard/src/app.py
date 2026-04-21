import dash
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import polars as pl
import os
import glob
from datetime import datetime
from sqlalchemy import create_engine, text

# Path to processed data
DATA_PATH = os.getenv("DATA_PATH", "../data_analytics/data/processed")

# Database Connection Settings
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def check_db_connection():
    if not all([DB_USER, DB_PASS, DB_NAME, DB_HOST]):
        return False, "Database environment variables missing."
    try:
        conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_str, connect_args={'connect_timeout': 2})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Connected to Database."
    except Exception as e:
        return False, f"Database connection failed: {str(e)}"

db_status, db_msg = check_db_connection()

app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.LUX, dbc.icons.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}]
)

def get_file_list():
    if not os.path.exists(DATA_PATH):
        return []
    files = glob.glob(os.path.join(DATA_PATH, "*.parquet"))
    files.sort(key=lambda x: "decomposition_anomalies" not in x)
    return [os.path.basename(f) for f in files]

def get_category(col_name):
    col_lower = col_name.lower()
    if 'telerelevee' in col_lower: return 'Consommation Télérelevée'
    if 'profilee' in col_lower and 'consommation' in col_lower: return 'Consommation Profilée'
    if 'production' in col_lower: return 'Production'
    if any(x in col_lower for x in ['temp', 'rayon', 'degre', 'ecart']): return 'Météo'
    if any(x in col_lower for x in ['soutirage', 'injection', 'perte']): return 'Réseau / Flux'
    if 'price' in col_lower or 'prix' in col_lower: return 'Prix'
    return 'Autres'

# Custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Enedis Energy Intelligence</title>
        {%favicon%}
        {%css%}
        <style>
            body { background-color: #f4f7f9; font-family: 'Inter', sans-serif; }
            .sidebar { background: #2c3e50; color: white; transition: all 0.3s; }
            .card { border: none; border-radius: 15px; box-shadow: 0 4px 20px 0 rgba(0,0,0,0.05); transition: transform 0.2s; }
            .card:hover { transform: translateY(-5px); }
            .kpi-card { background: white; border-left: 5px solid #2c3e50; }
            .glass-panel { background: rgba(255, 255, 255, 0.8); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.2); }
            .dropdown .Select-control { border-radius: 10px; border: 1px solid #ddd; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

sidebar = html.Div(
    [
        html.Div([
            html.I(className="bi bi-lightning-charge-fill me-2", style={"fontSize": "2rem"}),
            html.H2("ENEDIS", className="mb-0", style={"fontWeight": "800", "letterSpacing": "2px"}),
        ], className="d-flex align-items-center mb-4 pt-3"),
        
        html.P("Energy Intelligence Pipeline", className="text-white-50 small mb-4"),
        html.Hr(style={"borderColor": "rgba(255,255,255,0.1)"}),
        
        dbc.Nav(
            [
                html.Div([
                    dbc.Label("Select Insight Depth", className="text-white-50 small fw-bold mb-2 uppercase"),
                    dcc.Dropdown(
                        id='file-dropdown',
                        options=[{'label': f.replace('.parquet','').replace('_',' '), 'value': f} for f in get_file_list()],
                        value=get_file_list()[0] if get_file_list() else None,
                        clearable=False,
                        className="mb-3 text-dark"
                    ),
                    
                    dbc.Label("Analysis Category", className="text-white-50 small fw-bold mb-2"),
                    dcc.Dropdown(
                        id='category-dropdown',
                        clearable=False,
                        className="mb-3 text-dark"
                    ),
                    
                    dbc.Label("Specific Metric", className="text-white-50 small fw-bold mb-2"),
                    dcc.Dropdown(
                        id='column-dropdown',
                        clearable=False,
                        className="mb-3 text-dark"
                    ),
                ], className="px-2"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    className="sidebar p-4 d-flex flex-column",
    style={
        "position": "fixed", "top": 0, "left": 0, "bottom": 0, "width": "22rem", "zIndex": 1000
    },
)

def make_kpi(title, value, icon, color):
    return dbc.Card(
        dbc.CardBody([
            html.Div([
                html.Div([
                    html.H6(title, className="text-muted mb-1 uppercase fw-bold", style={"fontSize": "0.7rem"}),
                    html.H3(value, id=f"kpi-{title.lower().replace(' ', '-')}", className="mb-0 fw-bold"),
                ]),
                html.I(className=f"bi {icon} text-{color}", style={"fontSize": "1.8rem"})
            ], className="d-flex justify-content-between align-items-center")
        ]), className="kpi-card shadow-sm mb-4"
    )

content = html.Div(
    [
        # Database Status Alert
        dbc.Alert(
            [
                html.I(className="bi bi-wifi-off me-2"),
                f"Offline Mode Active: {db_msg} Using local Parquet files for visualization."
            ],
            color="warning",
            className="mb-4 shadow-sm",
            style={"display": "block" if not db_status else "none"}
        ),
        
        dbc.Row([
            dbc.Col(make_kpi("Avg Value", "---", "bi-graph-up", "primary"), width=2),
            dbc.Col(make_kpi("Max Value", "---", "bi-arrow-up-circle", "success"), width=2),
            dbc.Col(make_kpi("Min Value", "---", "bi-arrow-down-circle", "warning"), width=2),
            dbc.Col(make_kpi("Total Points", "---", "bi-database", "info"), width=2),
            dbc.Col(make_kpi("Anomalies", "---", "bi-exclamation-triangle", "danger"), width=2),
            dbc.Col(make_kpi("Anomaly Ratio", "---", "bi-percent", "secondary"), width=2),
        ], className="mb-2 gx-2"),
        
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([html.I(className="bi bi-activity me-2"), "Temporal Energy Distribution"], className="bg-white fw-bold"),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id='main-graph-v2', style={"height": "50vh"})))
                ]), width=12, lg=8, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([html.I(className="bi bi-pie-chart-fill me-2"), "Quality Summary"], className="bg-white fw-bold"),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id='summary-graph-v2', style={"height": "50vh"})))
                ]), width=12, lg=4, className="mb-4"
            ),
        ]),
        
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([html.I(className="bi bi-calendar3 me-2"), "Monthly Seasonality (Avg)"], className="bg-white fw-bold"),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id='monthly-bar-graph', style={"height": "40vh"})))
                ]), width=12, lg=6, className="mb-4"
            ),
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([html.I(className="bi bi-clock me-2"), "Hourly Usage Pattern"], className="bg-white fw-bold"),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id='hourly-box-graph', style={"height": "40vh"})))
                ]), width=12, lg=6, className="mb-4"
            ),
        ]),
    ],
    style={"margin-left": "24rem", "margin-right": "2rem", "padding": "2rem 0"},
)

app.layout = html.Div([sidebar, content])

# --- Callbacks ---

@callback(
    Output('category-dropdown', 'options'),
    Output('category-dropdown', 'value'),
    Input('file-dropdown', 'value')
)
def update_category_v2(filename):
    if not filename: return [], None
    path = os.path.join(DATA_PATH, filename)
    try:
        df = pl.read_parquet(path, n_rows=1)
        cols = [c for c in df.columns if c not in ['timestamp', 'Area', 'Sequence', 'time_lag'] and not c.endswith('_anomalie_by_decomposition')]
        categories = sorted(list(set(get_category(c) for c in cols)))
        return [{'label': c, 'value': c} for c in categories], categories[0]
    except: return [], None

@callback(
    Output('column-dropdown', 'options'),
    Output('column-dropdown', 'value'),
    Input('file-dropdown', 'value'),
    Input('category-dropdown', 'value')
)
def update_column_v2(filename, category):
    if not filename or not category: return [], None
    path = os.path.join(DATA_PATH, filename)
    try:
        df = pl.read_parquet(path)
        all_cols = df.columns
        plot_cols = [c for c in all_cols if get_category(c) == category and not c.endswith('_anomalie_by_decomposition') and c != 'timestamp']
        options = []
        for c in plot_cols:
            has_anom = any(ac for ac in all_cols if ac.startswith(c) and 'anomalie' in ac)
            options.append({'label': f"✨ {c.replace('_', ' ').title()}" if has_anom else c.replace('_', ' ').title(), 'value': c})
        return options, plot_cols[0] if plot_cols else None
    except: return [], None

def format_value(val):
    if val is None: return "---"
    abs_val = abs(val)
    if abs_val >= 1e9:
        return f"{val/1e9:.2f}G"
    elif abs_val >= 1e6:
        return f"{val/1e6:.2f}M"
    elif abs_val >= 1e3:
        return f"{val/1e3:.2f}k"
    else:
        return f"{val:.2f}"

@callback(
    Output('main-graph-v2', 'figure'),
    Output('summary-graph-v2', 'figure'),
    Output('monthly-bar-graph', 'figure'),
    Output('hourly-box-graph', 'figure'),
    Output('kpi-avg-value', 'children'),
    Output('kpi-max-value', 'children'),
    Output('kpi-min-value', 'children'),
    Output('kpi-total-points', 'children'),
    Output('kpi-anomalies', 'children'),
    Output('kpi-anomaly-ratio', 'children'),
    Input('file-dropdown', 'value'),
    Input('column-dropdown', 'value')
)
def update_dashboard_full(filename, column):
    if not filename or not column:
        return [go.Figure()]*4 + ["---"]*6
    
    path = os.path.join(DATA_PATH, filename)
    try:
        df = pl.read_parquet(path)
        if 'timestamp' in df.columns:
            df = df.sort('timestamp')
            # Extract month/hour if missing
            if 'month_utc' not in df.columns:
                df = df.with_columns(pl.col('timestamp').dt.month().alias('month_utc'))
            if 'hour_utc' not in df.columns:
                df = df.with_columns(pl.col('timestamp').dt.hour().alias('hour_utc'))
            x_axis = df['timestamp']
        else:
            x_axis = list(range(len(df)))

        # 1. Main Figure
        fig_main = go.Figure()
        fig_main.add_trace(go.Scatter(x=x_axis, y=df[column], mode='lines', fill='tozeroy',
                                     fillcolor='rgba(44, 62, 80, 0.1)', line=dict(color='#2c3e50', width=2),
                                     name=column.replace('_', ' ').title()))
        
        anomaly_cols = [c for c in df.columns if (column + "_anomalie") in c]
        anomaly_col = anomaly_cols[0] if anomaly_cols else None
        anom_count = 0
        if anomaly_col:
            anom_df = df.filter(pl.col(anomaly_col) == True)
            anom_count = len(anom_df)
            if anom_count > 0:
                fig_main.add_trace(go.Scatter(x=anom_df['timestamp'], y=anom_df[column], mode='markers',
                                             name='Anomalies', marker=dict(color='#e74c3c', size=8)))

        fig_main.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=30, b=10),
                               xaxis=dict(rangeslider=dict(visible=True, thickness=0.08)))

        # 2. Quality Donut
        fig_summary = go.Figure()
        if anomaly_col:
            fig_summary.add_trace(go.Pie(labels=['Normal', 'Anomaly'], values=[len(df)-anom_count, anom_count],
                                        hole=.6, marker=dict(colors=['#2ecc71', '#e74c3c'])))
            fig_summary.update_layout(margin=dict(l=20, r=20, t=20, b=20), showlegend=False)

        # 3. Monthly Bar
        monthly_avg = df.group_by('month_utc').agg(pl.col(column).mean()).sort('month_utc')
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        fig_monthly = go.Figure(go.Bar(x=[month_names[m-1] for m in monthly_avg['month_utc']], 
                                       y=monthly_avg[column], marker_color='#3498db'))
        fig_monthly.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=30, b=10))

        # 4. Hourly Box
        fig_hourly = px.box(df.to_pandas(), x='hour_utc', y=column, points=False, color_discrete_sequence=['#2c3e50'])
        fig_hourly.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=30, b=10), xaxis_title="Hour of Day")

        # KPIs
        avg_v = format_value(df[column].mean())
        max_v = format_value(df[column].max())
        min_v = format_value(df[column].min())
        total_p = format_value(len(df))
        ratio = f"{(anom_count/len(df))*100:.2f}%" if anomaly_col else "0.00%"
        
        return fig_main, fig_summary, fig_monthly, fig_hourly, avg_v, max_v, min_v, total_p, str(anom_count), ratio
    except Exception as e:
        print(f"Error: {e}")
        return [go.Figure()]*4 + ["err"]*6

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8050))
    app.run(debug=True, port=port, host='0.0.0.0')

import dash
from dash import dcc, html, Input, Output, State
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from parse_txt_to_dataframe import parse_txt_files

# 加载解析后的数据
DATA_DICT = parse_txt_files()
PREFIXES = list(DATA_DICT.keys())

app = dash.Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.H1('实验数据可视化', style={
            'textAlign': 'center',
            'color': '#2962FF',  # 亮蓝作为标题对比色
            'fontFamily': 'Roboto, sans-serif'
        }),
        html.P('本工具用于可视化实验数据，支持按数据前缀和采集时间筛选查看', style={
            'textAlign': 'center',
            'color': '#607D8B',  # 蓝灰色段落文字
            'margin': '10px 0 20px 0'
        })
    ]),
    html.Div([
        html.Div([
            html.Label('选择数据前缀:', style={'color': '#1a237e', 'fontWeight': '500'}),
            dcc.Dropdown(
                id='prefix-dropdown',
                options=[{'label': p, 'value': p} for p in PREFIXES],
                value=PREFIXES[0] if PREFIXES else None,
                placeholder='请选择数据前缀',
                style={'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0.1)'}
            )
        ], style={'width': '300px', 'margin': '20px 10px', 'padding': '15px', 'backgroundColor': '#f5f7fa',  
                  'borderRadius': '10px', 'boxShadow': '0 2px 8px rgba(0,0,0.05)'}),
        html.Div([
            html.Label('选择具体文件:', style={'color': '#1a237e',  'fontWeight': '500'}),
            dcc.Dropdown(
                id='file-dropdown',
                placeholder='请先选择数据前缀',
                style={'borderRadius': '8px', 'boxShadow': '0 2px 4px rgba(0,0,0.1)'}
            )
        ], style={'width': '300px', 'margin': '20px 10px', 'padding': '15px', 'backgroundColor': '#f5f7fa',   'borderRadius': '10px', 'boxShadow': '0 2px 8px rgba(0,0,0.05)'})
    ], style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '0 20px'}),
    html.Div([
        dcc.Graph(id='graph')
    ], style={'margin': '20px', 'padding': '20px', 'backgroundColor': '#f5f7fa',   'borderRadius': '10px', 'boxShadow': '0 2px 8px rgba(0,0,0.05)'}),
    html.Div(id='data-table', style={'margin': '20px', 'padding': '20px', 'backgroundColor': '#f5f7fa',   'borderRadius': '10px', 'boxShadow': '0 2px 8px rgba(0,0,0.05)'})
])

@app.callback(
    Output('file-dropdown', 'options'),
    Output('file-dropdown', 'value'),
    Input('prefix-dropdown', 'value')
)
def update_file_dropdown(selected_prefix):
    if not selected_prefix:
        return [], None
    datetime_keys = list(DATA_DICT[selected_prefix].keys())  # 获取日期时间键列表
    return [{'label': key, 'value': key} for key in datetime_keys], datetime_keys[0] if datetime_keys else None

@app.callback(
    Output('data-table', 'children'),
    Input('file-dropdown', 'value'),
    State('prefix-dropdown', 'value')
)
def update_table(file_key, selected_prefix):  # 参数名从file_index改为file_key
    if not selected_prefix or file_key is None:
        return html.Div('请选择有效数据前缀和文件')
    df = DATA_DICT[selected_prefix][file_key]  # 通过日期时间键获取DataFrame
    return dash.dash_table.DataTable(
        data=df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in df.columns],
        page_size=10,
        style_table={'overflowX': 'auto'},
        style_cell={'padding': '8px'},
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',  # 浅灰色表头
            'fontWeight': 'bold'
        }
    )

@app.callback(
    Output('graph', 'figure'),
    Input('file-dropdown', 'value'),
    State('prefix-dropdown', 'value')
)
def update_graph(file_key, selected_prefix):
    if not selected_prefix or file_key is None:
        return {'data': [], 'layout': {'title': '请选择有效数据前缀和文件'}}
    df = DATA_DICT[selected_prefix][file_key]
    # 处理fre列，确保没有零或负数
    if (df['fre'] <= 0).any():
        return {'data': [], 'layout': {'title': 'fre列包含非正值，无法进行log变换'}}
    log_fre = np.log(df['fre'])
    traces = []
    for col in ['X1', 'Y1', 'X2', 'Y2']:
        traces.append(go.Scatter(
            x=log_fre,
            y=df[col],
            mode='lines',
            name=col,
            line=dict(width=2)
        ))
    return {
        'data': traces,
        'layout': {
            'title': '实验数据曲线',
            'xaxis': {'title': 'log(fre)'},
            'yaxis': {'title': '测量值'},
            'hovermode': 'x unified',
            'plot_bgcolor': '#f0f4f8',  # 浅灰图表背景
            'paper_bgcolor': '#f8f9fa',
            'margin': {'t': 50, 'b': 50, 'l': 50, 'r': 50}
        }
    }

if __name__ == '__main__':
    app.run(debug=True)
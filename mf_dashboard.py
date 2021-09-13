import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input,Output
import plotly.graph_objs as go
import pandas as pd
import get_mf_details
import dash_table

mf_instance = get_mf_details.MutualFunds()
scheme_details = mf_instance.load_mf_scheme_details()
scheme_type = scheme_details['scheme_type'].unique()

external_scripts = [
    {
        'src': 'https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js',
        'integrity': 'sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM',
        'crossorigin': 'anonymous'
    }
]

# external CSS stylesheets
external_stylesheets = [
    {
        'href': 'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO',
        'crossorigin': 'anonymous'
    }
]
# external CSS stylesheets
app = dash.Dash(suppress_callback_exceptions=True,external_stylesheets=external_stylesheets,external_scripts=external_scripts)

count =0

scheme_type_dropdown = dcc.Dropdown(id='scheme_type_dropdown',
                                   options=[{'label': s_type, 'value': s_type } for s_type in scheme_type ],
                                   value="Open Ended")

scheme_category_dropdown = dcc.Dropdown(id="scheme_category_dropdown",
                                        value="Equity Scheme - ELSS")

scheme_nav_name = dcc.Dropdown(id="scheme_nav_name_dropdown",
                               value="Axis Long Term Equity Fund - Regular Plan - Growth",multi=True)

returns = dash_table.DataTable(id="returns-table",style_header={'text-align':'center','padding':'80','background-color':' #0072B5', 'color': 'white'})

graph  = dcc.Graph(id="mf_graph")

html_pre = html.Pre(id="hover-data")

app.layout = html.Div(
             [
             html.H1(["Mutual Fund Comparision Dashboard "],style={'text-align':'center','padding':'80','background-color':' #0072B5', 'color': 'white'}),
             html.Div([scheme_type_dropdown],style={'width': '50%', 'display': 'inline-block'}),
             html.Div([scheme_category_dropdown],style={'width': '50%', 'display': 'inline-block'}),
             html.Div([scheme_nav_name]),
             html.Div([graph]),
             html.Div([html_pre]),
             html.Div([returns])
             ],style={'padding':10})


@app.callback(
    Output("scheme_category_dropdown", "options"),
    [Input("scheme_type_dropdown", "value")]
)
def update_multi_options(value):
    filt = scheme_details['scheme_type'] == value
    scheme_category = scheme_details[filt]["scheme_category"].unique()
    scheme_category = [{"label":s_category ,"value":s_category } for s_category  in scheme_category]
    return scheme_category

@app.callback(
    Output("scheme_nav_name_dropdown", "options"),
    [Input("scheme_category_dropdown", "value")]
)
def update_multi_options(value):
    filt = scheme_details['scheme_category'] == value
    scheme_nav_name = scheme_details[filt]["scheme_nav_name"].unique()
    scheme_nav_name = [{"label":s_nav_name ,"value":s_nav_name } for s_nav_name  in scheme_nav_name]
    return scheme_nav_name

@app.callback(
    Output("mf_graph", "figure"),
    [Input("scheme_nav_name_dropdown", "value")]
)
def create_graph(value):
    # df = mf_instance.get_mf_performance_history([value])
    # data= [go.Scatter(x=df["business_date_l"],y=df["absolute_returns"],name=value)]
    # print(df)
    df = pd.DataFrame()
    if isinstance(value,str):
        df = mf_instance.get_mf_performance_history([value])
    else:
        df = mf_instance.get_mf_performance_history(value)

    data=[]
    for scheme_name in df['scheme_nav_name_l'].unique():
        new_df = df.loc[df['scheme_nav_name_l'] == scheme_name]
        trace = go.Scatter(x=new_df["business_date_l"],y=new_df["absolute_returns"],mode='lines',name=scheme_name,marker=dict(size=15,line=dict(width=2)))
        data.append(trace)
    #print(data)
    layout = go.Layout(yaxis = dict(zeroline=False,showgrid=False),
                       xaxis=dict(showgrid=False),
                       legend=dict(yanchor="top",xanchor="left"),
                       autosize =True
                       )
    return {"data":data ,"layout":layout }

@app.callback(
    Output("returns-table", "columns"),
    Output("returns-table", "data"),
    [Input("scheme_nav_name_dropdown", "value")]
)
def create_graph(value):
    df = pd.DataFrame()
    if isinstance(value,str):
        df = mf_instance.get_scheme_metrics([value])
    else:
        df = mf_instance.get_scheme_metrics(value)
    data=df.to_dict('records')
    return [{"name": i, "id": i} for i in df.columns],data

if __name__ == '__main__':
    app.run_server(debug=True)
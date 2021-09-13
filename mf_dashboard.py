""" Using Dash,This module plots dashboard to track the performance of mutual funds

imports :
    sql_parser - The sql_parser contains the information of the driver,database
    and the tables

  Typical usage example:

  mf = MutualFunds()
"""


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

graph  = dcc.Graph(id="mf_graph")

html_pre = html.Pre(id="hover-data")

absolute_returns = dash_table.DataTable(id="absolute_returns-table",style_header={'text-align':'center','padding':'80','background-color':' #0072B5', 'color': 'white'})
top_mutual_funds = dash_table.DataTable(id="top_mutual_funds-table",style_header={'text-align':'center','padding':'80','background-color':' #0072B5', 'color': 'white'})

app.layout = html.Div(
             [
             html.H1(["Mutual Fund Comparision Dashboard "],style={'text-align':'center','padding':'80','background-color':' #0072B5', 'color': 'white'}),
             html.Div([scheme_type_dropdown],style={'width': '50%', 'display': 'inline-block'}),
             html.Div([scheme_category_dropdown],style={'width': '50%', 'display': 'inline-block'}),
             html.Div([scheme_nav_name]),
             html.Div([graph]),
             html.Div([html_pre]),
             html.Div([absolute_returns]),
             html.Hr(),
             html.H5(["Top Mutual Funds in this category"],style={'text-align':'center','padding':'80'}),
             html.Div([top_mutual_funds])
             ],style={'padding':10})


@app.callback(
    Output("scheme_category_dropdown", "options"),
    [Input("scheme_type_dropdown", "value")]
)
def update_scheme_category(value):
    """ Update scheme_category dropdown based on the scheme_type selected

        Args:
            scheme_type_dropdown - The type of scheme - Open Ended,Close ended or Interval Fund.
        Returns:
           scheme_category_dropdown - Add values in scheme_type_dropdown based on the scheme_type
        Raises:
            N.A
    """
    filt = scheme_details['scheme_type'] == value
    scheme_category = scheme_details[filt]["scheme_category"].unique()
    scheme_category = [{"label":s_category ,"value":s_category } for s_category  in scheme_category]
    return scheme_category

@app.callback(
    Output("scheme_nav_name_dropdown", "options"),
    [Input("scheme_category_dropdown", "value")]
)
def update_scheme_nav_names(value):
    """ Update scheme_nav_name dropdown based on the scheme_category selected

        Args:
            scheme_category_dropdown - The category of scheme
        Returns:
           scheme_nav_name_dropdown - Add values in scheme_nav_nam based on the scheme_category
        Raises:
            N.A
    """
    filt = scheme_details['scheme_category'] == value
    scheme_nav_name = scheme_details[filt]["scheme_nav_name"].unique()
    scheme_nav_name = [{"label":s_nav_name ,"value":s_nav_name } for s_nav_name  in scheme_nav_name]
    return scheme_nav_name

@app.callback(
    Output("mf_graph", "figure"),
    [Input("scheme_nav_name_dropdown", "value")]
)
def create_graph(value):
    """ Create graphs based on the nav_name selected in scheme_nav_name_dropdown

        Args:
            scheme_nav_name_dropdown - The Fund Name
        Returns:
           mf_graph - returns graph based on fund name
        Raises:
            N.A
    """
    df = pd.DataFrame()
    if isinstance(value,str):
        df = mf_instance.get_absolute_returns([value])
    else:
        df = mf_instance.get_absolute_returns(value)

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
    Output("absolute_returns-table", "columns"),
    Output("absolute_returns-table", "data"),
    [Input("scheme_nav_name_dropdown", "value")]
)
def create_absolute_returns_table(value):
    """ Create graphs based on the nav_name selected in scheme_nav_name_dropdown

        Args:
            scheme_nav_name_dropdown - The Fund Name
        Returns:
           columns - columns of the datframe
           data - absolute returns of the fund name selected.
        Raises:
            N.A
    """
    df = pd.DataFrame()
    if isinstance(value,str):
        df = mf_instance.get_scheme_metrics([value])
    else:
        df = mf_instance.get_scheme_metrics(value)
    data=df.to_dict('records')
    return [{"name": i, "id": i} for i in df.columns],data

@app.callback(
    Output("top_mutual_funds-table", "columns"),
    Output("top_mutual_funds-table", "data"),
    [Input("scheme_category_dropdown", "value")]
)
def create_top_mutual_funds_table(value):
    """ A table of all the top performing mutual fund in selected category

        Args:
            scheme_category_dropdown - The Fund Name
        Returns:
           columns - columns of the datframe
           data -  top mutual funds based on the cagr 5 yrs
        Raises:
            N.A
    """
    df = pd.DataFrame()
    print(value)
    df = mf_instance.get_all_metrics('return(5yrs)',value)
    data=df.to_dict('records')
    return [{"name": i, "id": i} for i in df.columns],data

if __name__ == '__main__':
    app.run_server(debug=True)
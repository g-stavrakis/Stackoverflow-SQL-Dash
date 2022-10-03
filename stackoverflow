#%%
from google.cloud import bigquery
import pandas as pd
import plotly.express as px 
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output 
import os

#
credentials_path = '/Users/georgestavrakis/Documents/Projects/Stackoverflow_Dashboard/stackoverflow-dashboard-2d30e656137d.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# %%
# Construct a BigQuery client object.
client = bigquery.Client()

# %%
# Construct a reference to the "Stack_Overflow_Data" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)


# %%
# To manage costs, only run the query if it's less than 500 MB
MAX_MB = 1000*1000*1000  
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_MB)

# Create a QueryJobConfig object to estimate size of query without running it
dry_run_config = bigquery.QueryJobConfig(dry_run=True)

# Create the function to calculate and display the cost of a query
def cost(examine_query):
    print('Cost of this query: {} MB'.format(round(client.query(examine_query, job_config=dry_run_config).total_bytes_processed*10**-6,2)))

# %% Creating the Dashboard

# Start the App 
app = Dash(__name__)

# %%

app.layout = html.Div(children=[

    html.H1('About the Tags of Stackoverflow', style={'text-align': 'center'}),

    dcc.Dropdown(id="slct_num",
            options=[
                {"label": "5", "value": '5'},
                {"label": "10", "value": '10'},
                {"label": "15", "value": '15'},
                {"label": "20", "value": '20'}],
            multi=False,
            value=10,
            style={'width': "40%"}
            ),

    dcc.Graph(id='top_tags', style={'display': 'inline-block'}),
    dcc.Graph(id="top_tags_time", style={'display': 'inline-block'})
])

#%%

@app.callback(
    [Output(component_id='top_tags', component_property='figure'),
    Output(component_id='top_tags_time', component_property='figure')],
    Input(component_id='slct_num', component_property='value')
)
def tags_graphs(slct_num):

    ### For the Top Languages ###
    # Query the dataset
    query_popular_tags ="""
                        SELECT tag_name AS Tag_Names, count AS Instances
                        FROM `bigquery-public-data.stackoverflow.tags`
                        ORDER BY Instances DESC
                        LIMIT {}
                        """.format(slct_num)
    # API request
    popular_tags =client.query(query_popular_tags, job_config=safe_config)
    # Create dataframe
    popular_tags_df = popular_tags.to_dataframe()

    # Create the graph
    fig1 = px.pie(popular_tags_df, values='Instances', names='Tag_Names', hole=.3, title='Top 10 most frequend tags')
    fig1.update_traces(textposition='inside', textinfo='percent+label')
    
    ### Time Series of the Top Languages ###
    # Creat the selet statement based on number of top Languages
    selected = ""
    for tags in popular_tags_df.Tag_Names.to_list():
        if tags == 'c#':
            tags_title = 'c'
        elif tags == 'c++':
            tags_title = 'c_plus_plus'
        else:
            tags_title =tags
        selected += ", COUNTIF(tags LIKE '%{}%') AS Questions_posted_{}".format(tags,tags_title)
    
    # Qyery the data
    query_subtags_time = """
                SELECT EXTRACT(YEAR FROM creation_date) AS year {}
                FROM `bigquery-public-data.stackoverflow.posts_questions`
                GROUP BY year
                ORDER BY year
                """.format(selected)
    # API request
    subtags_time =client.query(query_subtags_time)
    # Create dataframe
    subtags_time_df = subtags_time.to_dataframe()

    # Create the graph
    fig2 = go.Figure()
    for lng in list(subtags_time_df.columns)[1:]:
        fig2.add_trace(go.Scatter(x=subtags_time_df['year'], y=subtags_time_df[lng], mode='lines', name=lng.split('_')[-1]))
    fig2.show()
    
    return fig1, fig2



# %%
# Run the app
if __name__ == '__main__':
    app.run_server(debug=False)
# %%


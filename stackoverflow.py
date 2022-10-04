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

colors = {
    'background': '#b0bec5',
    'text': '#7FDBFF'
}

# %%

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[

    html.H1('About the Tags of Stackoverflow', style={'text-align': 'center'}),

    html.Div([
    html.P('Please select the number of top languages you want to examine.', style={'text-align': 'center'}),
    dcc.Dropdown(id="slct_num",
            options=[
                {"label": "5", "value": '5'},
                {"label": "10", "value": '10'},
                {"label": "15", "value": '15'},
                {"label": "20", "value": '20'}],
            multi=False,
            value=10,
            style={'width': "40%", 'align-items': 'center', 'justify-content': 'center'}
            ),

    dcc.Graph(id='top_tags', style={'display': 'inline-block'}),
    dcc.Graph(id="top_tags_time", style={'display': 'inline-block'})
    ]),

    html.Div([
            html.P('Please select which language you want to further explore', style={'text-align': 'center'}),
            dcc.Dropdown(id="slct_sub",
                options=[
                {"label": "Javascript", "value": 'javascript'},
                {"label": "Python", "value": 'python'},
                {"label": "Java", "value": 'java'},
                {"label": "c#", "value": 'c#'},
                {"label": "php", "value": 'php'},
                {"label": "Android", "value": 'android'},
                {"label": "Html", "value": 'html'},
                {"label": "Jquery", "value": 'jquery'},
                {"label": "c++", "value": 'c++'},
                {"label": "css", "value": 'css'}],
            multi=False,
            value='python',
            style={'width': "40%", 'align-items': 'center', 'justify-content': 'center'}
            ),
            # Adding the graphs
            dcc.Graph(id='top_subtags', style={'display': 'inline-block'}),
            dcc.Graph(id="top_users", style={'display': 'inline-block'})
    ])
])

#%%

@app.callback(
    [Output(component_id='top_tags', component_property='figure'),
    Output(component_id='top_tags_time', component_property='figure'),
    Output(component_id='top_subtags', component_property='figure'),
    Output(component_id='top_users', component_property='figure')],
    
    [Input(component_id='slct_num', component_property='value'),
    Input(component_id='slct_sub', component_property='value')]
)
def tags_graphs(slct_num, slct_sub):
    
    ### For the Top Languages ###
    
    # # Query the dataset
    # query_popular_tags ="""
    #                     SELECT tag_name AS Tag_Names, count AS Instances
    #                     FROM `bigquery-public-data.stackoverflow.tags`
    #                     ORDER BY Instances DESC
    #                     LIMIT {}
    #                     """.format(slct_num)
    # # API request
    # popular_tags =client.query(query_popular_tags, job_config=safe_config)
    # # Create dataframe
    # popular_tags_df = popular_tags.to_dataframe()

    # Create the graph
    fig1 = px.pie(popular_tags_df, values='Instances', names='Tag_Names', hole=.3, title='Top 10 most frequend tags')
    fig1.update_traces(textposition='inside', textinfo='percent+label')
    
    ### Time Series of the Top Languages ###
    # # Creat the selet statement based on number of top Languages
    # selected = ""
    # for tags in popular_tags_df.Tag_Names.to_list():
    #     if tags == 'c#':
    #         tags_title = 'c'
    #     elif tags == 'c++':
    #         tags_title = 'c_plus_plus'
    #     else:
    #         tags_title =tags
    #     selected += ", COUNTIF(tags LIKE '%{}%') AS Questions_posted_{}".format(tags,tags_title)
    
    # # Qyery the data
    # query_subtags_time = """
    #             SELECT EXTRACT(YEAR FROM creation_date) AS year {}
    #             FROM `bigquery-public-data.stackoverflow.posts_questions`
    #             GROUP BY year
    #             ORDER BY year
    #             """.format(selected)
    # # API request
    # subtags_time =client.query(query_subtags_time)
    # # Create dataframe
    # subtags_time_df = subtags_time.to_dataframe()

    # Create the graph
    fig2 = go.Figure()
    for lng in list(subtags_time_df.columns)[1:]:
        fig2.add_trace(go.Scatter(x=subtags_time_df['year'], y=subtags_time_df[lng], mode='lines', name=lng.split('_')[-1]))
    fig2.show()

    ### For Top Sub Languages ###
    # # Query the data
    # query_subtags = """
    #             WITH main_tags AS(
    #             SELECT tags 
    #             FROM `bigquery-public-data.stackoverflow.posts_questions`
    #             WHERE tags LIKE '%{}%'),
                
    #             subtags_lists AS(
    #             SELECT SPLIT(tags, "|") AS subtags
    #             FROM main_tags)
                
    #             SELECT subtag, COUNT(1) AS Instances
    #             FROM subtags_lists,
    #                 UNNEST (subtags) AS subtag
    #             WHERE subtag != '{}'
    #             GROUP BY subtag
    #             Order By Instances DESC
    #             LIMIT 100
    #             """.format(slct_sub,slct_sub)
    # # API request
    # subtags =client.query(query_subtags, job_config=safe_config)
    # # Create dataframe
    # subtags_df = subtags.to_dataframe()

    # # Create the graph
    # subtags_df_plot = subtags_df.head(20).sort_values('Instances')
    fig3 = px.bar(subtags_df_plot, x="Instances", y="subtag", text='Instances', orientation='h', title="Most frequent Sub-tags")
    fig3.show()

    ### For Top Sub Languages answerers ###
    # # Query the data
    # # Users that answered these questions
    # query_tag_users ="""
    # SELECT pa.owner_display_name, COUNT(1) AS total_answers , SUM(pa.score) AS total_score
    # FROM `bigquery-public-data.stackoverflow.posts_questions` AS pq
    # RIGHT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS pa
    # ON pq.id = pa.parent_id
    # WHERE pq.tags LIKE '%{}%' AND pa.owner_display_name!='None'
    # GROUP BY pa.owner_display_name
    # ORDER BY total_score DESC
    # LIMIT 15
    # """.format(slct_sub)
    # # API request
    # tag_users =client.query(query_tag_users)
    # # Create dataframe
    # tag_users_df = tag_users.to_dataframe()

    # Create the graph of the table
    fig4 = go.Figure(data=[go.Table(
    header=dict(values=list(tag_users_df.columns),
                fill_color='orange',
                align='left'),
    cells=dict(values=tag_users_df.transpose().values.tolist(),
               fill_color='lavender',
               align='left'))
])
        
    return fig1, fig2, fig3, fig4



# %%
# Run the app
if __name__ == '__main__':
    app.run_server(debug=False)
# %%


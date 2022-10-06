#%%
from turtle import width
from google.cloud import bigquery
import pandas as pd
import plotly.express as px 
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import os


#
credentials_path = 'LOCAL_PATH_FOR_CREDENTIALS_FILE'
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

# Create the function to query data
def request_data(query):
    # API request
    answered =client.query(query_answered, job_config=safe_config)
    # Create dataframe
    answered_df = answered.to_dataframe()
    return answered_df

# %% Creating the Dashboard


# Start the App 
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

colors = {
    'background': '#b0bec5',
    'text': '#7FDBFF'
}

# %%

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    
    dbc.Container([

    dbc.Row([ html.Img(src='assets/Stack_Overflow_icon.svg.png', style={'height':'10%', 'width':'10%'})], justify='center'),
    # Creating the title
    dbc.Row([
        dbc.Col([html.H1('Stackoverflow Analysis', style={'text-align': 'center'}),
        html.P('Stack Overflow is a question and answer website for professional and enthusiast programmers. It is the flagship site of the Stack Exchange Network. It was created in 2008 by Jeff Atwood and Joel Spolsky. It features questions and answers on a wide range of topics in computer programming.', style={'text-align': 'center'})
    ],width=8)
    ], justify='center'),

    dbc.Row([dcc.Graph(id="user_growth"), html.P('')], justify='center'),

    dbc.Row([

        dbc.Col([dcc.Graph(id="ansered_perc")], width = 4),
        dbc.Col([dcc.Graph(id="ansered_perc_year")], width = 8)

    ])
    
]),

    dbc.Container([
    # Creating the title
    dbc.Row([
        html.P(''),
        html.P(''),
        dbc.Col([html.H1('Tags of Stackoverflow'),
        html.P('')
    ],width=8)
    ]),
    
    dbc.Row([
    dbc.Col([
    # Creating the first slider
    dbc.Row([
        dbc.Col([
         
         dbc.Row([
            html.P('Please select the number of top languages you want to examine.', style={'text-align': 'center'}),
            dcc.Slider(5,20,5,id="slct_num",value=10),
            html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P('')
         ]),

         dbc.Row([
         html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),html.P(''),
         html.P('Please select which language you want to further explore.', style={'text-align': 'center'}),
         dcc.Dropdown(id="slct_sub",
            options=[
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/javascript.svg", height=20),
                    html.Div("Javascript", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "javascript"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/python.svg", height=20),
                    html.Div("Python", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "python"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/java.svg", height=20),
                    html.Div("Java", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "java"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/c.svg", height=20),
                    html.Div("c#", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "c#"},
        
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/php.svg", height=20),
                    html.Div("php", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "php"},

            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/android.svg", height=20),
                    html.Div("Android", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "android"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/html.svg", height=20),
                    html.Div("Html", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "html"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/jquery.svg", height=20),
                    html.Div("Jquery", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "jquery"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/c-2.svg", height=20),
                    html.Div("c++", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "c++"},
            
            {"label": html.Div(
                [
                    html.Img(src="/assets/languages/php.svg", height=20),
                    html.Div("css", style={'font-size': 15, 'padding-left': 10}),
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}
            ),"value": "css"}],
         multi=False,
         value='python',
         style={'align-items': 'center', 'justify-content': 'center'}
         )
         ])
        ])
    ], align="center", justify='center')
    ], align="center",width = 3),

    dbc.Col([

    # Creating the 2 first graphs
    dbc.Row([
        dbc.Col([dcc.Graph(id='top_tags')], width = 6),
        dbc.Col([dcc.Graph(id="top_tags_time")], width = 6),
    ]),

    
    dbc.Row([html.P('')], justify='center'), 

    # Creating the 2 second graphs
    dbc.Row([
        dbc.Col([dcc.Graph(id='top_subtags')], width = 6),
        dbc.Col([dcc.Graph(id="top_users")], width = 6),
    ])

    ],width =9)
])
])
])


#html.Div(style={'backgroundColor': colors['background']}, children=[


#%%

@app.callback(
    [Output(component_id='user_growth', component_property='figure'),
    Output(component_id='ansered_perc', component_property='figure'),
    Output(component_id='ansered_perc_year', component_property='figure'),
    Output(component_id='top_tags', component_property='figure'),
    Output(component_id='top_tags_time', component_property='figure'),
    Output(component_id='top_subtags', component_property='figure'),
    Output(component_id='top_users', component_property='figure')],
    
    [Input(component_id='slct_num', component_property='value'),
    Input(component_id='slct_sub', component_property='value')]
)
def tags_graphs(slct_num, slct_sub):
    
    # Create dataset for user growth
    query_users = """
                SELECT FORMAT_DATE("%Y-%m", creation_date) as date, COUNT(1) AS new_users
                FROM `bigquery-public-data.stackoverflow.users`
                GROUP BY date
                ORDER BY date
                """

    # API request
    users_growth_job = client.query(query_users, job_config=safe_config) 
    # Create dataframe
    users_growth_df = users_growth_job.to_dataframe()
 

    # Visualise the Users Growth
    fig = px.line(users_growth_df, x="date", y="new_users", title='Stackoverflow users growth')
    
    ### For percentage of answered question in Platform ###
    # Create dataset for user growth
    query_answered = """
                    SELECT (COUNTIF(answer_count>0) / COUNT(1))*100 AS Answered_Percentage, (100- (COUNTIF(answer_count>0) / COUNT(1))*100) AS Unanswered
                    FROM `bigquery-public-data.stackoverflow.posts_questions`
                    """
    answered_df = request_data(query_answered)

    # Visualise the data
    fig1 = px.pie(values=answered_df.iloc[0], names=answered_df.columns, 
             hole=.3, title='Answered questions on Stackoverflow')
    fig1.update_traces(textposition='inside', textinfo='percent+label')
    fig1.update_layout(showlegend=False)
    
    ### For percentage per year ###
    # What is the percentage of questions that have been answered over the years?
    query_percentage = """
                        WITH totals AS(
                        SELECT EXTRACT(YEAR FROM creation_date) AS year, COUNT(1) AS num_quest
                        FROM `bigquery-public-data.stackoverflow.posts_questions` 
                        GROUP BY year
                        ORDER BY year),
                        
                        answerd AS(
                        SELECT EXTRACT(YEAR FROM creation_date) AS year, COUNT(1) AS answered_quest
                        FROM `bigquery-public-data.stackoverflow.posts_questions`
                        WHERE answer_count > 0
                        GROUP BY year
                        ORDER BY year)
                        
                        SELECT a.year, t.num_quest AS Total_Number_Questions, a.answered_quest AS Number_Answered_Questions,
                            ROUND((a.answered_quest / t.num_quest)*100,2) AS Answered_Question_Percentage, 
                        FROM answerd AS a
                        INNER JOIN totals AS t
                        ON a.year = t.year
                        ORDER BY a.year
                        """
    percentages_df = request_data(query_percentage)


    fig2 = px.bar(percentages_df, x="year", y="Total_Number_Questions", text='Answered_Question_Percentage',
     hover_data=['Answered_Question_Percentage'], color='Answered_Question_Percentage',
     color_continuous_scale='balance_r', title="Percentage of questions answered  over the years")
    fig2.update_layout(showlegend=False)
    fig2.update(layout_coloraxis_showscale=False)

    
    ### For the Top Languages ###
    
    # Query the dataset
    query_popular_tags ="""
                        SELECT tag_name AS Tag_Names, count AS Instances
                        FROM `bigquery-public-data.stackoverflow.tags`
                        ORDER BY Instances DESC
                        LIMIT {}
                        """.format(str(slct_num))

    popular_tags_df = request_data(query_popular_tags)

    # Create the graph
    fig3 = px.pie(popular_tags_df, values='Instances', names='Tag_Names', hole=.3, title='Top 10 most frequent tags')
    fig3.update_traces(textposition='inside', textinfo='percent+label')
    
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
    subtags_time_df = request_data(query_subtags_time)

    # Create the graph
    fig4 = go.Figure()
    for lng in list(subtags_time_df.columns)[1:]:
        fig4.add_trace(go.Scatter(x=subtags_time_df['year'], y=subtags_time_df[lng], mode='lines', name=lng.split('_')[-1]))
    fig4.update_layout(title="Evolution of top tags over time")

    ### For Top Sub Languages ###
    # Query the data
    query_subtags = """
                WITH main_tags AS(
                SELECT tags 
                FROM `bigquery-public-data.stackoverflow.posts_questions`
                WHERE tags LIKE '%{}%'),
                
                subtags_lists AS(
                SELECT SPLIT(tags, "|") AS subtags
                FROM main_tags)
                
                SELECT subtag, COUNT(1) AS Instances
                FROM subtags_lists,
                    UNNEST (subtags) AS subtag
                WHERE subtag != '{}'
                GROUP BY subtag
                Order By Instances DESC
                LIMIT 100
                """.format(slct_sub,slct_sub)
    subtags_df = request_data(query_subtags)

    # Create the graph
    subtags_df_plot = subtags_df.head(10).sort_values('Instances')
    fig5 = px.bar(subtags_df_plot, x="Instances", y="subtag", text='Instances', orientation='h', title="Most frequent Sub-tags")

    ### For Top Sub Languages answerers ###
    # Query the data
    # Users that answered these questions
    query_tag_users ="""
    SELECT pa.owner_display_name, COUNT(1) AS total_answers , SUM(pa.score) AS total_score
    FROM `bigquery-public-data.stackoverflow.posts_questions` AS pq
    RIGHT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS pa
    ON pq.id = pa.parent_id
    WHERE pq.tags LIKE '%{}%' AND pa.owner_display_name!='None'
    GROUP BY pa.owner_display_name
    ORDER BY total_score DESC
    LIMIT 15
    """.format(slct_sub)
    tag_users_df = request_data(query_tag_users)

    # Create the graph of the table
    fig6 = go.Figure(data=[go.Table(
    header=dict(values=list(tag_users_df.columns),
                fill_color='orange',
                align='left'),
    cells=dict(values=tag_users_df.transpose().values.tolist(),
               fill_color='lavender',
               align='left'))
               
])
    fig6.update_layout(title="Most Knowledgable Users")

        
    return fig, fig1, fig2, fig3, fig4, fig5, fig6



# %%
# Run the app
if __name__ == '__main__':
    app.run_server(debug=False)
# %%


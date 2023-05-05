import pandas as pd
from pymongo import MongoClient
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.express as px

# Connect to MongoDB
client = MongoClient("mongodb://root:rootpassword@localhost:27017")
db = client["food_db"]
collection = db["food_analyze"]

# Read data from MongoDB and convert to DataFrame
data = list(collection.find())
df = pd.DataFrame(data)

 

# Create a Dash app
app = dash.Dash(__name__)

# Define app layout
app.layout = html.Div([
    html.H1("Food Data Visualization"),
    dcc.Checklist(
        id="gender-checklist",
        options=[
            {"label": "Female", "value": "Female"},
            {"label": "Male", "value": "Male"}
        ],
        value=["Female", "Male"]
    ),
    dcc.Graph(id="histogram-graph"),
    dcc.Graph(id="pie-chart"),
 
])

# Create a callback to update the histogram chart
@app.callback(
    Output("histogram-graph", "figure"),
    [Input("gender-checklist", "value")]
)
def update_histogram_chart(selected_genders):
    filtered_df = df[df["GENDER_DESC"].isin(selected_genders)]
    fig = px.histogram(filtered_df, x="WEIGHT", color="GENDER_DESC")
    return fig

# Create a callback to update the pie chart
@app.callback(
    Output("pie-chart", "figure"),
    [Input("gender-checklist", "value")]
)
def update_pie_chart(selected_genders):
    filtered_df = df[df["GENDER_DESC"].isin(selected_genders)]
    fig = px.pie(filtered_df, names="CALORIES_DAY", title="Calories Day")
    return fig
 

# Run the app
if __name__ == "__main__":
    app.run_server()

  


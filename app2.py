import pandas as pd
from pymongo import MongoClient
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots

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
    dcc.Dropdown(
        id='x-axis-dropdown',
        options=[
            {'label': 'Favorite Cuisine', 'value': 'FAV_CUISINE_CODED_DESC'},
            {'label': 'Father Education', 'value': 'FATHER_EDUCATION_DESC'},
            {'label': 'Mother Education', 'value': 'MOTHER_EDUCATION_DESC'},
            {'label': 'Ideal Diet', 'value': 'IDEAL_DIET_CODED_DESC'},
        ],
        value='FAV_CUISINE_CODED_DESC'
    ),
    dcc.Graph(id="line-chart"),
])

# Create a callback to update the line chart
@app.callback(
    Output("line-chart", "figure"),
    [Input("gender-checklist", "value"),
     Input("x-axis-dropdown", "value")]
)
def update_line_chart(selected_genders, x_axis):
    filtered_df = df[df["GENDER_DESC"].isin(selected_genders)]
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    for gender in selected_genders:
        grouped_df = filtered_df.groupby([x_axis, "GENDER_DESC"]).mean().reset_index()
        filtered_grouped_df = grouped_df[grouped_df["GENDER_DESC"] == gender]
        fig.add_trace(go.Scatter(x=filtered_grouped_df[x_axis], y=filtered_grouped_df["WEIGHT"],
                                 mode="lines", name=gender), secondary_y=False)

    fig.update_layout(title="Weight by " + x_axis,
                      xaxis_title=x_axis,
                      yaxis_title="Weight")

    return fig

# Run the app
if __name__ == "__main__":
    app.run_server()
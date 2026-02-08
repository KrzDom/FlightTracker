from flask import Flask, render_template
import plotly.express as px
import plotly.graph_objects as go
import os

from app_utilities import (
    get_all_flights_with_average_price,
    get_statistics,
    fetch_last_entries,
    fetch_avg_price_dbd,
    fetch_pricing_matrices,
    fetch_price_development_by_dow)

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE_DIR, "data", "flights.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


@app.route("/report")
def report():
    flights, prices = fetch_last_entries(limit=500)
    return render_template("report.html", flights=flights, prices=prices)


@app.route("/")
def home():
    statistics_dict = get_statistics()
    df = fetch_avg_price_dbd()
    matrix_queries, matrix_flights = fetch_pricing_matrices()

    # --- Line plot ---
    fig = px.line(
        df,
        x="days_before_departure",
        y="avg_price",
        markers=True,
        title="Average Price vs Days Before Departure",
        labels={
            "days_before_departure": "Days Before Departure",
            "avg_price": "Average Price (€)"
        }
    )
    fig.update_xaxes(autorange="reversed")
    plot_html = fig.to_html(full_html=False)

    # --- Fill missing cells with "N/A" ---
    matrix_queries_filled = matrix_queries.fillna("N/A")
    matrix_flights_filled = matrix_flights.fillna("N/A")

    # Labels
    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    time_labels = ["23-3", "3-7", "7-11", "11-15", "15-19", "19-23"]

    # --- Heatmaps ---
    fig_queries = px.imshow(
        matrix_queries_filled,
        labels=dict(x="Day of Week", y="Time Slot", color="Avg Price"),
        text_auto=True
    )
    fig_queries.update_xaxes(
        tickmode='array',
        tickvals=list(matrix_queries.columns),
        ticktext=day_labels[:matrix_queries.shape[1]]
    )
    fig_queries.update_yaxes(
        tickmode='array',
        tickvals=list(matrix_queries.index),
        ticktext=time_labels[:matrix_queries.shape[0]]
    )

    fig_departures = px.imshow(
        matrix_flights_filled,
        labels=dict(x="Day of Week", y="Time Slot", color="Avg Price"),
        text_auto=True
    )
    fig_departures.update_xaxes(
        tickmode='array',
        tickvals=list(matrix_flights.columns),
        ticktext=day_labels[:matrix_flights.shape[1]]
    )
    fig_departures.update_yaxes(
        tickmode='array',
        tickvals=list(matrix_flights.index),
        ticktext=time_labels[:matrix_flights.shape[0]]
    )

    # --- Convert to HTML ---
    plot_html_queries = fig_queries.to_html(full_html=False)
    plot_html_departures = fig_departures.to_html(full_html=False)

    return render_template(
        "home.html",
        plot_html=plot_html,
        stats=statistics_dict,
        plot_html_queries=plot_html_queries,
        plot_html_departures=plot_html_departures
    )


@app.route("/all_flights")
def all_flights():
    flights_dict = get_all_flights_with_average_price()
    return render_template("display_all_flights.html",  flights=flights_dict)


@app.route("/visual")
def visual():
    # Fetch your data for days before departure
    df_price_dev = fetch_price_development_by_dow()  # must have columns: day_of_week, days_before_departure, avg_price

    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Create the figure
    fig = go.Figure()

    for dow in range(7):
        subset = df_price_dev[df_price_dev["day_of_week"] == dow].sort_values("days_before_departure")
        subset = subset.dropna(subset=["days_before_departure", "avg_price"])  # skip rows with missing data
        if subset.empty:
            continue

        fig.add_trace(
            go.Scatter(
                x=subset["days_before_departure"],
                y=subset["avg_price"],
                mode="lines+markers",
                name=day_names[dow]
            )
        )

    # Layout
    fig.update_layout(
        title="Flight Price Development by Departure Weekday",
        xaxis_title="Days before departure",
        yaxis_title="Average flight price",
        legend_title="Departure weekday",
        template="plotly_white",
        autosize=True,
        height=600,
        margin=dict(l=50, r=50, t=60, b=50)
    )
    fig.update_xaxes(autorange="reversed")  # booking horizon

    # Convert to HTML fragment
    plot_html = fig.to_html(full_html=False)

    # Optional stats for your stat-cards
    
    # Render your template
    return render_template(
        "visual.html",
        plot_html=plot_html,  # only this plot
    )


if __name__ == "__main__":
    app.run(debug=True)

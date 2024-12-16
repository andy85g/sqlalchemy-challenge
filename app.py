import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from flask import Flask, jsonify

# Create a reference to the file.
data_file = Path("Resources/hawaii_measurements.csv")
data_file1 = Path("Resources/hawaii_stations.csv")

#################################################
# Database Setup
#################################################

# Create engine to connect to the SQLite database
database_path = "sqlite:///Resources/hawaii.sqlite"
engine = create_engine(database_path)

# Reflect the existing database into a new model
Base = automap_base()
Base.prepare(engine, reflect=True)

# Reflect the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# API Route: /api/v1.0/precipitation
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Get the most recent date and calculate one year ago
    most_recent_date = database_path['date'].max()
    one_year_ago = most_recent_date - timedelta(days=365)
    
    # Filter data for the last 12 months and drop missing prcp values
    last_12_months_data = database_path[database_path['date'] > one_year_ago].dropna(subset=['prcp'])
    
    # Convert to dictionary with date as key and prcp as value
    precipitation_dict = dict(zip(last_12_months_data['date'].dt.strftime('%Y-%m-%d'), last_12_months_data['prcp']))
    return jsonify(precipitation_dict)

# API Route: /api/v1.0/stations
@app.route("/api/v1.0/stations")
def stations():
    # Get a unique list of station IDs
    station_list = df['station'].unique().tolist()
    return jsonify(station_list)

# API Route: /api/v1.0/tobs
@app.route("/api/v1.0/tobs")
def tobs():
    # Identify the most-active station
    most_active_station = database_path['station'].mode()[0]
    
    # Filter data for the most-active station for the last 12 months
    most_recent_date = database_path['date'].max()
    one_year_ago = most_recent_date - timedelta(days=365)
    last_year_data = database_path[(database_path['station'] == most_active_station) & (database_path['date'] > one_year_ago)]
    
    # Return list of temperature observations (tobs)
    temperature_observations = last_year_data['tobs'].tolist()
    return jsonify(temperature_observations)

# API Route: /api/v1.0/<start> and /api/v1.0/<start>/<end>
@app.route("/api/v1.0/<start>", defaults={'end': None})
@app.route("/api/v1.0/<start>/<end>")
def temperature_summary(start, end):
    # Parse the start and end dates
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d') if end else df['date'].max()
    
    # Filter data for the specified date range
    filtered_data = database_path[(database_path['date'] >= start_date) & (database_path['date'] <= end_date)]
    
    # Calculate TMIN, TAVG, and TMAX
    summary = {
        "TMIN": filtered_data['tobs'].min(),
        "TAVG": filtered_data['tobs'].mean(),
        "TMAX": filtered_data['tobs'].max()
    }
    return jsonify(summary)

# Run the app
if __name__ == "__main__":
    app.run(debug=True)

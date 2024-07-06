# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func
import datetime

from flask import Flask, jsonify
import pandas as pd
import numpy as np

#################################################
# Database Setup
#################################################

# Create engine using the `hawaii.sqlite` database file
engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# Declare a Base using `automap_base()`
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(autoload_with=engine)

# Assign the measurement class to a variable called `Measurement` and
# the station class to a variable called `Station`
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"Below URL use data in YYYY-MM-DD format as start date:<br/>"
        f"/api/v1.0/<start><br/>"
        f"Below URL use data in YYYY-MM-DD format as start date followed / end date:<br/>"
        f"/api/v1.0/orm/<start>/<end><br/>"
        f"/api/v1.0/sql/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
        # Calculate the date one year from the last date in data set.
    start_date = datetime.date(2016, 8, 23)
    
    # Perform a query to retrieve the data and precipitation scores
    precip_data = session.query(Measurement.date, Measurement.station, Measurement.prcp).filter(Measurement.date >= start_date).all()
    
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    df2 = pd.DataFrame(precip_data, columns=["Date", "Station", "Precipitation"])
    # df2 = pd.DataFrame(precip_data)
    
    # Sort the dataframe by date
    df2["Date"] = pd.to_datetime(df2['Date'])
    df2 = df2.sort_values(by="Date", ascending=True).reset_index(drop=True)
    
    # close session
    session.close()
    data = df2.to_dict(orient="records")
    return(jsonify(data))

@app.route("/api/v1.0/stations")
def stations():
    # Perform a query to retrieve the data and precipitation scores
    station_data = session.query(Station.station).all()
    
    session.close()
    stations = list(np.ravel(station_data))
    return jsonify(stations=stations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Perform a query to retrieve the data and precipitation scores
    tobs_data = session.query(Measurement.station, func.count(Measurement.date)).group_by(Measurement.station).order_by(func.count(Measurement.date).desc()).all()
    
    session.close()
    temp = list(np.ravel(tobs_data))
    return jsonify(temp= temp)

@app.route("/api/v1.0/<start>")
def start(start):
    # Perform a query to retrieve the data and precipitation scores
   
    start_date = datetime.datetime.strptime(start, '%Y-%m-%d')

    # Perform a query to retrieve the data and tobs scores
    start_data = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        all()
    df2 = pd.DataFrame(delta_data, columns=["min_tobs", "avg_tobs", "max_tobs"])
    session.close()

    # start1 = list(np.ravel(start_data))
    data = df2.to_dict(orient="records")
    return jsonify(data)
@app.route("/api/v1.0/orm/<start>/<end>")
def startend(start,end):
    # Perform a query to retrieve the data and precipitation scores
   
    start_date = datetime.datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end, '%Y-%m-%d')

    # Perform a query to retrieve the data and tobs scores
    delta_data = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).\
        all()
    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    df2 = pd.DataFrame(delta_data, columns=["min_tobs", "avg_tobs", "max_tobs"])
    session.close()
    data = df2.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/sql/<start>/<end>")
def query_tobs_start_end_raw(start, end):

    query = f"""
            SELECT
                min(tobs) as min_temp,
                avg(tobs) as avg_temp,
                max(tobs) as max_temp
            FROM
                measurement
            WHERE
                date >= '{start}'
                and date < '{end}';
        """

    df = pd.read_sql(text(query), con = engine)
    data = df.to_dict(orient="records")
    return jsonify(data)
    
if __name__ == '__main__':
    app.run(debug=True)
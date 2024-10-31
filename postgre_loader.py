import pandas as pd
from sqlalchemy import create_engine, text
from loaders.postgresql.circuits import Circuits
from loaders.postgresql.constructors_results import ConstructorResults
from loaders.postgresql.constructor_standings import ConstructorStandings
from loaders.postgresql.constructors import Constructors
from loaders.postgresql.driver_standings import DriverStandings
from loaders.postgresql.drivers import Drivers
from loaders.postgresql.lap_times import LapTimes
from loaders.postgresql.pit_stops import PitStops
from loaders.postgresql.qualifying import Qualifying
from loaders.postgresql.races import Races
from loaders.postgresql.results import Results
from loaders.postgresql.seasons import Seasons
from loaders.postgresql.sprint_results import SprintResults
from loaders.postgresql.status import Status

database_url = "postgresql://postgres:2797@localhost:5432/noindexeddb"

engine = create_engine(database_url)

circuits = Circuits(engine, pd, text)
constructor_results = ConstructorResults(engine, pd, text)
constructor_standings = ConstructorStandings(engine, pd, text)
constructors = Constructors(engine, pd, text)
driver_standings = DriverStandings(engine, pd, text)
drivers = Drivers(engine, pd, text)
lap_times = LapTimes(engine, pd, text)
pit_stops = PitStops(engine, pd, text)
qualifying = Qualifying(engine, pd, text)
races = Races(engine, pd, text)
results = Results(engine, pd, text)
seasons = Seasons(engine, pd, text)
sprint_results = SprintResults(engine, pd, text)
status = Status(engine, pd, text)
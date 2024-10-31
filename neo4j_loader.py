import pandas as pd
from neo4j import GraphDatabase
from loaders.neo4j.drivers import LoadDrivers
from loaders.neo4j.constructors import LoadConstructors
from loaders.neo4j.lap_times import LoadLapTimes

class DatabaseDriver:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters=None, database_="neo4j"):
        with self.driver.session(database=database_) as session:
            result = session.run(query, parameters)
            return [record for record in result]

def add_drives_for_relationship(driver, driver_id, constructor_id):
    query = """
    MATCH (driver:Driver), (constructor:Constructor)
    WHERE driver.id = $driver_id AND constructor.id = $constructor_id
    MERGE (driver)-[:DRIVES_FOR]->(constructor)
    """
    parameters = {'driver_id': driver_id, 'constructor_id': constructor_id}
    driver.execute_query(query, parameters=parameters)

def add_turned_in_relationship(driver, driver_id, race_id, lap_number, time):
    query = """
    MATCH (driver:Driver), (lap:Lap)
    WHERE driver.id = $driver_id AND lap.race_id = $race_id AND lap.lap_number = $lap_number
    MERGE (driver)-[:TURNED_IN]->(lap)
    ON CREATE SET lap.time = $time
    """
    parameters = {'driver_id': driver_id, 'race_id': race_id, 'lap_number': lap_number, 'time': time}
    driver.execute_query(query, parameters=parameters)

drivers = LoadDrivers()
constructors = LoadConstructors()
lap_times = LoadLapTimes()

df_results = pd.read_csv('dataset/results.csv')
df_lap_times = pd.read_csv('dataset/lap_times.csv')

db_driver = DatabaseDriver(uri="bolt://localhost:7687", user="neo4j", password="password")

for index, row in df_results.iterrows():
    add_drives_for_relationship(db_driver, driver_id=row['driverId'], constructor_id=row['constructorId'])

# for index, row in df_lap_times.iterrows():
#     add_turned_in_relationship(db_driver, driver_id=row['driverId'], race_id=row['raceId'], lap_number=row['lap'], time=row['time'])

db_driver.close()
import pandas as pd
from neo4j import GraphDatabase

class DatabaseDriver:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters=None, database_="neo4j"):
        with self.driver.session(database=database_) as session:
            result = session.run(query, parameters)
            return [record for record in result]
        
class LoadLapTimes:
    def __init__(self):
        self.df_drivers = pd.read_csv('dataset/lap_times.csv')
        self.db_driver = DatabaseDriver(uri="bolt://localhost:7687", user="neo4j", password="password")

        for index, row in self.df_drivers.iterrows():
            self.add_driver(race_id=row['raceId'], driver_id=row['driverId'], lap=row['lap'], position=row['position'], time=row['time'])
        
        self.db_driver.close()

    def add_driver(self, race_id, driver_id, lap, position, time):
        query = "MERGE (lap:Lap {race_id: $race_id, driver_id: $driver_id, lap: $lap, position: $position, time: $time})"
        parameters = {'race_id': race_id, 'driver_id': driver_id, 'lap': lap, 'position': position, 'time': time}
        self.db_driver.execute_query(query, parameters=parameters, database_="neo4j")
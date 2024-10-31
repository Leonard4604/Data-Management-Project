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
        
class LoadDrivers:
    def __init__(self):
        self.df_drivers = pd.read_csv('dataset/drivers.csv')
        self.db_driver = DatabaseDriver(uri="bolt://localhost:7687", user="neo4j", password="password")

        for index, row in self.df_drivers.iterrows():
            self.add_driver(driver_id=row['driverId'], driver_name=row['forename'], driver_surname=row['surname'], nationality=row['nationality'])
        
        self.db_driver.close()

    def add_driver(self, driver_id, driver_name, driver_surname, nationality):
        query = "MERGE (driver:Driver {id: $driver_id, name: $driver_name, surname: $driver_surname, nationality: $nationality})"
        parameters = {'driver_id': driver_id, 'driver_name': driver_name, 'driver_surname': driver_surname, 'nationality': nationality}
        self.db_driver.execute_query(query, parameters=parameters, database_="neo4j")
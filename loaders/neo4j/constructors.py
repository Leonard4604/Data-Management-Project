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
        
class LoadConstructors:
    def __init__(self):
        self.df_constructors = pd.read_csv('dataset/constructors.csv')

        self.db_driver = DatabaseDriver(uri="bolt://localhost:7687", user="neo4j", password="password")

        for index, row in self.df_constructors.iterrows():
            self.add_constructor(constructor_id=row['constructorId'], constructor_name=row['name'], nationality=row['nationality'])

        self.db_driver.close()
        
    def add_constructor(self, constructor_id, constructor_name, nationality):
        query = "MERGE (constructor:Constructor {id: $constructor_id, name: $constructor_name, nationality: $nationality})"
        parameters = {'constructor_id': constructor_id, 'constructor_name': constructor_name, 'nationality': nationality}
        self.db_driver.execute_query(query, parameters=parameters, database_="neo4j")
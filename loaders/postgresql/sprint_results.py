class SprintResults:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sprint_results (
            result_id INT PRIMARY KEY,
            race_id INT,
            driver_id INT,
            constructor_id INT,
            grid INT,
            position INT,
            position_order INT,
            points FLOAT,
            laps INT,
            time VARCHAR(255),
            fastest_lap INT,
            fastest_lap_time VARCHAR(255),
            status_id INT
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/sprint_results.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['resultId', 'raceId', 'driverId', 'constructorId', 'grid', 'position', 'positionOrder', 'points', 'laps', 'time', 'fastestLap', 'fastestLapTime', 'statusId']]

        df_selected = df_selected.rename(columns={
            'resultId': 'result_id',
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'constructorId': 'constructor_id',
            'grid': 'grid',
            'position': 'position',
            'positionOrder': 'position_order',
            'points': 'points',
            'laps': 'laps',
            'time': 'time',
            'fastestLap': 'fastest_lap',
            'fastestLapTime': 'fastest_lap_time',
            'statusId': 'status_id'
        })

        df_selected.to_sql('sprint_results', self.engine, if_exists='replace', index=False)
class PitStops:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS pit_stops (
            race_id INT,
            driver_id INT,
            stop INT,
            lap INT,
            duration VARCHAR(255),
            PRIMARY KEY (race_id, driver_id, stop)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/pit_stops.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['raceId', 'driverId', 'stop', 'lap', 'duration']]

        df_selected = df_selected.rename(columns={
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'stop': 'stop',
            'lap': 'lap',
            'duration': 'duration'
        })

        df_selected.to_sql('pit_stops', self.engine, if_exists='replace', index=False)
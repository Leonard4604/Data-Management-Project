class LapTimes:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS lap_times (
            race_id INT,
            driver_id INT,
            lap VARCHAR(255),
            position VARCHAR(255),
            time VARCHAR(255),
            PRIMARY KEY (race_id, driver_id, lap)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/lap_times.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['raceId', 'driverId', 'lap', 'position', 'time']]

        df_selected = df_selected.rename(columns={
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'lap': 'lap',
            'position': 'position',
            'time': 'time'
        })

        df_selected.to_sql('lap_times', self.engine, if_exists='replace', index=False)
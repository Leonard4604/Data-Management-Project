class DriverStandings:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS driver_standings (
            driver_standings_id INT PRIMARY KEY,
            race_id INT,
            driver_id INT,
            points INT,
            position INT,
            wins INT
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/driver_standings.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['driverStandingsId', 'raceId', 'driverId', 'points', 'position', 'wins']]

        df_selected = df_selected.rename(columns={
            'driverStandingsId': 'driver_standings_id',
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'points': 'points',
            'position': 'position',
            'wins': 'wins'
        })

        df_selected.to_sql('driver_standings', self.engine, if_exists='replace', index=False)
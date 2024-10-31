class ConstructorStandings:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS constructor_standings (
            constructor_standings_id INT PRIMARY KEY,
            race_id INT,
            constructor_id INT,
            points INT,
            position INT
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/constructor_standings.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['constructorStandingsId', 'raceId', 'constructorId', 'points', 'position']]

        df_selected = df_selected.rename(columns={
            'constructorStandingsId': 'constructor_standings_id',
            'raceId': 'race_id',
            'constructorId': 'constructor_id',
            'points': 'points',
            'position': 'position'
        })

        df_selected.to_sql('constructor_standings', self.engine, if_exists='replace', index=False)
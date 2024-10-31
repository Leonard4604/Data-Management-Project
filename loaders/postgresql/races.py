class Races:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS races (
            race_id INT PRIMARY KEY,
            year INT,
            round INT,
            circuit_id INT,
            name VARCHAR(255),
            date DATE,
            time TIME
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/races.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time']]

        df_selected = df_selected.rename(columns={
            'raceId': 'race_id',
            'year': 'year',
            'round': 'round',
            'circuitId': 'circuit_id',
            'name': 'name',
            'date': 'date',
            'time': 'time'
        })

        df_selected.to_sql('races', self.engine, if_exists='replace', index=False)
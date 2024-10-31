class ConstructorResults:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS constructor_results (
            constructor_results_id INT PRIMARY KEY,
            race_id INT,
            constructor_id INT,
            points INT
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/constructor_results.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['constructorResultsId', 'raceId', 'constructorId', 'points']]

        df_selected = df_selected.rename(columns={
            'constructorResultsId': 'constructor_results_id',
            'raceId': 'race_id',
            'constructorId': 'constructor_id',
            'points': 'points'
        })

        df_selected.to_sql('constructor_results', self.engine, if_exists='replace', index=False)
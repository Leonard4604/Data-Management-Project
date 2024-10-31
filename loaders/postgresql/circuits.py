class Circuits:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS circuits (
            circuit_id INT PRIMARY KEY,
            name VARCHAR(255),
            location VARCHAR(255),
            country VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/circuits.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['circuitId', 'name', 'location', 'country']]

        df_selected = df_selected.rename(columns={
            'circuitId': 'circuit_id',
            'name': 'name',
            'location': 'location',
            'country': 'country'
        })

        df_selected.to_sql('circuits', self.engine, if_exists='replace', index=False)
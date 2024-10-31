class Seasons:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS seasons (
            year INT PRIMARY KEY,
            url VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/seasons.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['year', 'url']]

        df_selected = df_selected.rename(columns={
            'year': 'year',
            'url': 'url'
        })

        df_selected.to_sql('seasons', self.engine, if_exists='replace', index=False)
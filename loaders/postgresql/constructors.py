class Constructors:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS constructors (
            constructor_id INT PRIMARY KEY,
            name VARCHAR(255),
            nationality VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/constructors.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['constructorId', 'name', 'nationality']]

        df_selected = df_selected.rename(columns={
            'constructorId': 'constructor_id',
            'name': 'name',
            'nationality': 'nationality'
        })

        df_selected.to_sql('constructors', self.engine, if_exists='replace', index=False)
class Status:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS status (
            status_id INT PRIMARY KEY,
            status VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/status.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['statusId', 'status']]

        df_selected = df_selected.rename(columns={
            'statusId': 'status_id',
            'status': 'status'
        })

        df_selected.to_sql('status', self.engine, if_exists='replace', index=False)
class Drivers:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id INT PRIMARY KEY,
            forename VARCHAR(255),
            surname VARCHAR(255),
            dob DATE,
            nationality VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/drivers.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['driverId', 'forename', 'surname', 'dob', 'nationality']]

        df_selected = df_selected.rename(columns={
            'driverId': 'driver_id',
            'forename': 'forename',
            'surname': 'surname',
            'dob': 'dob',
            'nationality': 'nationality'
        })

        df_selected.to_sql('drivers', self.engine, if_exists='replace', index=False)
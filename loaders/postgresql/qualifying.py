class Qualifying:
    def __init__(self, engine, pd, text):
        self.engine = engine
        self.pd = pd
        self.text = text
        self.create_table()
        self.load_data()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS qualifying (
            qualify_id INT PRIMARY KEY,
            race_id INT,
            driver_id INT,
            constructor_id INT,
            position VARCHAR(255),
            q1 VARCHAR(255),
            q2 VARCHAR(255),
            q3 VARCHAR(255)
        );
        """

        with self.engine.connect() as connection:
            connection.execute(self.text(create_table_query))

    def load_data(self):
        df = self.pd.read_csv("dataset/qualifying.csv")

        df = df.replace("\\N", self.pd.NA)

        df_selected = df[['qualifyId', 'raceId', 'driverId', 'constructorId', 'position', 'q1', 'q2', 'q3']]

        df_selected = df_selected.rename(columns={
            'qualifyId': 'qualify_id',
            'raceId': 'race_id',
            'driverId': 'driver_id',
            'constructorId': 'constructor_id',
            'position': 'position',
            'q1': 'q1',
            'q2': 'q2',
            'q3': 'q3'
        })

        df_selected.to_sql('qualifying', self.engine, if_exists='replace', index=False)
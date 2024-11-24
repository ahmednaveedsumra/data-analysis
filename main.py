import pandas as pd
from sklearn.cluster import DBSCAN
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError


config = {
    'write': {
        'USERNAME': 'root',
        'PASSWORD': 'ahmad09102',
        'HOST': '127.0.0.1',
        'PORT': '3306',
        'DATABASE_NAME': 'data',
        'TABLE_NAME': 'outlier'
    }
}


class DataProcessor:
    def __init__(self):
        pass



    # def fetch_all_data(self, chunk_size=50000):
    #     config_list = self.create_db_url('default')
    #     engine = self.create_engine(config_list[0])
    #     table_name = config_list[1]
    #
    #     all_data = []
    #
    #     try:
    #         with engine.connect() as connection:
    #             query = f"SELECT * FROM {table_name}"
    #             for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
    #                 all_data.append(chunk)
    #             print("Data fetched successfully in chunks")
    #
    #     except DatabaseError as e:
    #         raise Exception(f"Database error: {e}")
    #
    #     df = pd.concat(all_data, ignore_index=True)
    #
    #     return df
    #
    # def save_to_parquet(self, df):
    #     parquet_filename = "data2.parquet"
    #     df.to_parquet(parquet_filename)
    #     print(f"Data saved to {parquet_filename}")

    # def load_parquet(self, filename):
    #     df = pd.read_parquet(filename)
    #     print(f"Data loaded from {filename}")
    #     return df
    #
    # def process_table(self, df, chunk_size=50000):
    #     config_list_write = self.create_db_url('write')
    #     engine_db2 = self.create_engine(config_list_write[0])
    #
    #     with engine_db2.connect() as connection_db2:
    #         query = """
    #              CREATE TABLE IF NOT EXISTS processed_table (
    #                  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    #                  amhdpid INT,
    #                  `key` VARCHAR(250),
    #                  createdDate DATE,
    #                  rent FLOAT
    #              )
    #              """
    #         connection_db2.execute(text(query))
    #         print("Processed  table created or already exists")
    #
    #         for start in range(0, len(df), chunk_size):
    #             chunk_data = df.iloc[start:start + chunk_size]
    #             insert_query = """
    #                INSERT INTO processed_table (amhdpid, `key`, createdDate, rent)
    #                VALUES
    #                """
    #             values_list = []
    #
    #             for row in chunk_data.itertuples(index=False):
    #                 values_list.append(
    #                     f"({row.id}, \"{row.key}\", '{row.createdDate.strftime('%Y-%m-%d')}', {row.rent})")
    #
    #             insert_query += ",".join(values_list)
    #
    #             connection_db2.execute(text(insert_query))
    #             connection_db2.commit()
    #
    #             print(f"Inserted chunk starting at index {start} into processed table")

    def analysis(self, chunk_size=50000):
        config_list = self.create_db_url('write')
        engine = self.create_engine(config_list[0])
        table_name = config_list[1]

        try:
            with engine.connect() as connection:
                check_table_query = "SHOW TABLES LIKE 'processed_table_child'"
                result = connection.execute(text(check_table_query)).fetchall()
                if not result:
                    print("processed_table does not exist, skipping analysis.")
                    return

                query_dates = "SELECT DISTINCT createdDate FROM processed_table_child"
                result_distinct_dates = connection.execute(text(query_dates))
                dates = result_distinct_dates.fetchall()

                for date in dates:
                    analysis_date = date[0]
                    print(f"Performing analysis for {analysis_date}")

                    query = f"SELECT * FROM processed_table_child WHERE DATE(createdDate) = '{analysis_date}'"
                    chunk_iter = pd.read_sql(query, connection, chunksize=chunk_size)

                    for chunk_index, chunk_data in enumerate(chunk_iter, 1):
                        chunk_data['date'] = [pd.Timestamp(d).toordinal() for d in chunk_data['createdDate']]
                        x = chunk_data[['date', 'rent']].values

                        dbscan = DBSCAN(eps=0.2, min_samples=12)
                        dbscan_labels = dbscan.fit_predict(x)
                        outliers = dbscan_labels == -1
                        chunk_data = chunk_data.drop(columns=['date'])
                        chunk_data['outliers'] = outliers
                        chunk_data['mean'] = chunk_data['rent'].mean()
                        chunk_data['count'] = chunk_data['rent'].count()
                        chunk_data['median'] = chunk_data['rent'].median()

                        if chunk_index == 1:
                            create_table_query = f"""
                               CREATE TABLE IF NOT EXISTS `{table_name}` (
                                   id INT,
                                   amhdpid INT,
                                   `key` VARCHAR(250),
                                   createdDate DATE,
                                   rent FLOAT,
                                   outliers BOOLEAN,
                                   mean FLOAT,
                                   count FLOAT,
                                   median FLOAT
                               )
                               """
                            connection.execute(text(create_table_query))
                            print("Outlier analysis table created")

                        insert_query = f"""
                           INSERT INTO `{table_name}` (id, amhdpid, `key`, createdDate, rent, outliers, mean, count, median)
                           VALUES
                           """
                        values_list = []

                        for row in chunk_data.itertuples(index=False):
                            values_list.append(
                                f"({row.id}, {row.amhdpid}, \"{row.key}\", '{row.createdDate.strftime('%Y-%m-%d')}', {row.rent}, {row.outliers}, {row.mean}, {row.count}, {row.median})"
                            )

                        insert_query += ",".join(values_list)
                        connection.execute(text(insert_query))
                        connection.commit()

                        print(f"Analysis results saved for chunk starting at index {chunk_index}")
        except DatabaseError as e:
            raise Exception(f"Database error: {e}")
    @staticmethod
    def create_db_url(config_set):
        configure = config[config_set]
        configure['PASSWORD'] = configure['PASSWORD'].replace('@', '%40')
        db_url = f"mysql+pymysql://{configure['USERNAME']}:{configure['PASSWORD']}@{configure['HOST']}:{configure['PORT']}/{configure['DATABASE_NAME']}"
        if 'CLIENT_CERT_PATH' in configure and 'CLIENT_KEY_PATH' in configure:
            db_url += f"?ssl_cert={configure['CLIENT_CERT_PATH']}&ssl_key={configure['CLIENT_KEY_PATH']}"
        return [db_url, configure['TABLE_NAME']]

    @staticmethod
    def create_engine(db_url):
        return create_engine(db_url)


if __name__ == '__main__':
    processor = DataProcessor()

    # try:
    #     df = processor.fetch_all_data()
    #     print("All data fetched.")
    # except Exception as e:
    #     print(f"Error in fetching data: {e}")
    #
    # try:
    #     processor.save_to_parquet(df)
    #     print("Data saved to Parquet.")
    # except Exception as e:
    #     print(f"Error in saving data to Parquet: {e}")
    #
    # try:
    #     df = processor.load_parquet("data2.parquet")
    #     processor.process_table(df, chunk_size=50000)
    #     print("Data processed and inserted.")
    # except Exception as e:
    #     print(f"Error in processing data: {e}")

    try:
        processor.analysis(chunk_size=50000)
        print("Analysis completed and saved.")
    except Exception as e:
        print(f"Error in analysis and saving results: {e}")
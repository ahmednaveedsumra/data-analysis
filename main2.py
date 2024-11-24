import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
from sklearn.cluster import DBSCAN


config = {

'write': {
        'USERNAME': 'root',
        'PASSWORD': 'ahmad09102',
        'HOST': '127.0.0.1',
        'PORT': '3306',
        'DATABASE_NAME': 'data',
        'TABLE_NAME': 'outlier2'
    }
}


class DataProcessor:
    def __init__(self):
        pass


    def fetch_all_data_parent(self, chunk_size=50000):
        config_list = self.create_db_url('Parent')
        engine = self.create_engine(config_list[0])
        table_name = config_list[1]

        all_data = []

        try:
            with engine.connect() as connection:
                query = f"SELECT * FROM {table_name}"
                for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
                    all_data.append(chunk)
                print("Data fetched successfully in chunks")

        except DatabaseError as e:
            raise Exception(f"Database error: {e}")

        df_parent = pd.concat(all_data, ignore_index=True)

        return df_parent

    def fetch_all_data_child(self, chunk_size=50000):
        config_list = self.create_db_url('Child')
        engine = self.create_engine(config_list[0])
        table_name = config_list[1]

        all_data = []

        try:
            with engine.connect() as connection:
                query = f"SELECT * FROM {table_name}"
                for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
                    all_data.append(chunk)
                print("Data fetched successfully in chunks")

        except DatabaseError as e:
            raise Exception(f"Database error: {e}")

        df_child = pd.concat(all_data, ignore_index=True)

        return df_child

    def save_to_parquet_parent(self, df_parent):
        parquet_filename = "parent.parquet"
        df_parent.to_parquet(parquet_filename)
        print(f"Data saved to {parquet_filename}")

    def save_to_parquet_child(self, df_child):
        parquet_filename = "child.parquet"
        df_child.to_parquet(parquet_filename)
        print(f"Data saved to {parquet_filename}")

    def load_parquet_parent(self, filename):
        df_parent = pd.read_parquet(filename)
        print(f"Data loaded from {filename}")
        return df_parent

    def load_parquet_child(self, filename):
        df_child = pd.read_parquet(filename)
        print(f"Data loaded from {filename}")
        return df_child

    def process_table_parent(self, df_parent, chunk_size=50000):
        config_list_write = self.create_db_url('write')
        engine_db2 = self.create_engine(config_list_write[0])

        with engine_db2.connect() as connection_db2:
            query = """
              CREATE TABLE IF NOT EXISTS processed_table_parent (
                  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                  amhdhid INT,
                  `key` VARCHAR(250),
                  city VARCHAR(100),
                  state VARCHAR(100),
                  newLatitude FLOAT,
                  newLongitude FLOAT,
                  createdDate DATE
              )
              """
            connection_db2.execute(text(query))
            print("Processed parent table created or already exists")

            for start in range(0, len(df_parent), chunk_size):
                chunk_data = df_parent.iloc[start:start + chunk_size]
                insert_query = """
                INSERT INTO processed_table_parent (amhdhid, `key`,city, state, newLatitude, newLongitude, createdDate)
                VALUES
                """
                values_list = []

                for row in chunk_data.itertuples(index=False):
                    values_list.append(
                        f"({row.id}, \"{row.key}\", \"{row.city}\", \"{row.state}\",{row.newLatitude},{row.newLongitude}, '{row.createdDate.strftime('%Y-%m-%d')}')")

                insert_query += ",".join(values_list)

                connection_db2.execute(text(insert_query))
                connection_db2.commit()

                print(f"Inserted chunk starting at index {start} into processed table")

    def process_table_child(self, df_child, chunk_size=50000):
        config_list_write = self.create_db_url('write')
        engine_db2 = self.create_engine(config_list_write[0])

        with engine_db2.connect() as connection_db2:
            query = """
              CREATE TABLE IF NOT EXISTS processed_table_child (
                  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                  amhdpid INT,
                  amhdhidfk INT,
                  `key` VARCHAR(250),
                  createdDate DATE,
                  rent FLOAT
              )
              """
            connection_db2.execute(text(query))
            print("Processed child table created or already exists")

            for start in range(0, len(df_child), chunk_size):
                chunk_data = df_child.iloc[start:start + chunk_size]
                insert_query = """
                INSERT INTO processed_table_child (amhdpid, amhdhidfk, `key`, createdDate, rent)
                VALUES
                """
                values_list = []

                for row in chunk_data.itertuples(index=False):
                    values_list.append(
                        f"({row.id}, {row.homeId}, \"{row.key}\", '{row.createdDate.strftime('%Y-%m-%d')}', {row.rent})")

                insert_query += ",".join(values_list)

                connection_db2.execute(text(insert_query))
                connection_db2.commit()

                print(f"Inserted chunk starting at index {start} into processed table")

    def analysis(self, chunk_size=50000):
        config_list = self.create_db_url('write')
        engine = self.create_engine(config_list[0])
        table_name = config_list[1]

        try:
            with engine.connect() as connection:
                check_table_query_parent = "SHOW TABLES LIKE 'processed_table_parent'"
                result_parent = connection.execute(text(check_table_query_parent)).fetchall()

                check_table_query_child = "SHOW TABLES LIKE 'processed_table_child'"
                result_child = connection.execute(text(check_table_query_child)).fetchall()

                if not result_parent and not result_child:
                    print("Either processed_table_parent or processed_table_child does not exist, skipping analysis.")
                    return

                print("Both processed_table_parent and processed_table_child exist. Proceeding with analysis...")

                query_dates = "SELECT DISTINCT createdDate FROM processed_table_child"
                result_distinct_dates = connection.execute(text(query_dates))
                dates = result_distinct_dates.fetchall()

                for date in dates:
                    analysis_date = date[0]
                    print(f"Performing analysis for {analysis_date}")

                    query = f"""
                       SELECT parent.amhdhid, child.createdDate, child.rent
                       FROM processed_table_parent AS parent
                       JOIN processed_table_child AS child
                       ON parent.amhdhid = child.amhdhidfk
                       WHERE DATE(child.createdDate) = '{analysis_date}'
                       """
                    chunk_iter = pd.read_sql(query, connection, chunksize=chunk_size)

                    for chunk_index, chunk_data in enumerate(chunk_iter, 1):
                        chunk_data['date'] = [pd.Timestamp(d).toordinal() for d in chunk_data['createdDate']]
                        x = chunk_data[['date', 'rent']].values

                        dbscan = DBSCAN(eps=0.2, min_samples=12)
                        dbscan_labels = dbscan.fit_predict(x)
                        outliers = dbscan_labels == -1

                        chunk_data = chunk_data.drop(columns=['date'])
                        chunk_data['outliers'] = outliers

                        grouped_data = chunk_data.groupby('amhdhid').agg(
                            mean_rent=('rent', 'mean'),
                            count_rent=('rent', 'count'),
                            median_rent=('rent', 'median'),
                            outliers=('outliers', 'sum')
                        ).reset_index()

                        if chunk_index == 1:
                            create_table_query = f"""
                               CREATE TABLE IF NOT EXISTS `{table_name}` (
                                   id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                                   amhdhid INT,
                                   createdDate DATE,
                                   mean_rent FLOAT,
                                   count_rent INT,
                                   median_rent FLOAT,
                                   outliers INT
                               )
                               """
                            connection.execute(text(create_table_query))
                            print("Outlier analysis table created")

                        insert_query = f"""
                           INSERT INTO `{table_name}` (amhdhid, createdDate, mean_rent, count_rent, median_rent, outliers)
                           VALUES
                           """
                        values_list = []

                        for row in grouped_data.itertuples(index=False):
                            values_list.append(
                                f"({row.amhdhid}, '{analysis_date.strftime('%Y-%m-%d')}', {row.mean_rent}, {row.count_rent}, {row.median_rent}, {row.outliers})"
                            )

                        insert_query += ",".join(values_list)
                        connection.execute(text(insert_query))
                        connection.commit()

                        print(f"Analysis results saved for chunk starting at index {chunk_index}")
        except DatabaseError as e:
            raise Exception(f"Database error: {e}")



    def analysis_by_home(self, chunk_size=50000):
        config_list = self.create_db_url('write')
        engine = self.create_engine(config_list[0])
        table_name = "outlier2"  # Table name where analysis results will be saved

        try:
            with engine.connect() as connection:
                # Step 1: Create the output table (outlier2) if it doesn't exist
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,  -- Auto-incrementing ID
                    amhdhid INT,                                 -- Unique Home ID
                    amhdpid INT,                                 -- Unique Home Property ID
                    rent FLOAT,                                  -- Rent associated with the property
                    createdDate DATE,                            -- Created Date for the analysis
                    mean_rent FLOAT,                             -- Mean Rent for the home
                    count_rent INT,                              -- Count of Rent records for the home
                    median_rent FLOAT,                           -- Median Rent for the home
                    outliers INT                                 -- 0 if no outliers, 1 if outliers detected
                )
                """
                connection.execute(text(create_table_query))
                print(f"Table `{table_name}` created or already exists.")

                # Step 2: Get all amhdhid values from the parent table
                query_amhdhid = "SELECT amhdhid FROM processed_table_parent"
                result_amhdhid = connection.execute(text(query_amhdhid)).fetchall()

                # Step 3: Check if the child table exists
                check_table_query_child = "SHOW TABLES LIKE 'processed_table_child'"
                result_child = connection.execute(text(check_table_query_child)).fetchall()

                if not result_child:
                    print("processed_table_child does not exist, skipping analysis.")
                    return

                print("Proceeding with analysis...")

                # Step 4: Loop through each amhdhid and get associated rents from the child table
                for amhdhid_tuple in result_amhdhid:
                    amhdhid = amhdhid_tuple[0]
                    print(f"Performing analysis for amhdhid: {amhdhid}")

                    # Query rents and amhdpid associated with the current amhdhid
                    query_rents = f"""
                    SELECT child.amhdpid, child.createdDate, child.rent
                    FROM processed_table_child AS child
                    WHERE child.amhdhidfk = {amhdhid}
                    """
                    rents_data = pd.read_sql(query_rents, connection)

                    if rents_data.empty:
                        print(f"No rents found for amhdhid: {amhdhid}")
                        continue

                    # Step 5: Calculate mean, median, and count of rents for this amhdhid
                    rent_mean = rents_data['rent'].mean()
                    rent_median = rents_data['rent'].median()
                    rent_count = rents_data['rent'].count()

                    # Step 6: Apply DBSCAN on the rent values of this amhdhid
                    x = rents_data[['rent']].values  # Rent values for DBSCAN
                    dbscan = DBSCAN(eps=0.5, min_samples=3)  # Adjust DBSCAN params as needed
                    dbscan_labels = dbscan.fit_predict(x)

                    # Step 7: Insert results into the output table for each amhdpid
                    for idx, row in rents_data.iterrows():
                        # Mark rent as an outlier if DBSCAN label is -1 (outlier)
                        outlier = 1 if dbscan_labels[idx] == -1 else 0

                        insert_query = f"""
                        INSERT INTO `{table_name}` (amhdhid, amhdpid, rent, createdDate, mean_rent, count_rent, median_rent, outliers)
                        VALUES
                        """
                        insert_query += f"({amhdhid}, {row['amhdpid']}, {row['rent']}, '{row['createdDate'].strftime('%Y-%m-%d')}', {rent_mean}, {rent_count}, {rent_median}, {outlier})"

                        connection.execute(text(insert_query))
                        connection.commit()

                    print(f"Analysis results for amhdhid {amhdhid} saved")

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
    #     df_parent = processor.fetch_all_data_parent()
    #     print("All parent data fetched.")
    # except Exception as e:
    #     print(f"Error in fetching parent data: {e}")
    # try:
    #     df_child = processor.fetch_all_data_child()
    #     print("All child data fetched.")
    # except Exception as e:
    #     print(f"Error in fetching child data: {e}")
    #
    #
    # try:
    #     processor.save_to_parquet_parent(df_parent)
    #     print("Data saved to Parent Parquet.")
    # except Exception as e:
    #     print(f"Error in saving data to Parent Parquet: {e}")
    # try:
    #     processor.save_to_parquet_child(df_child)
    #     print("Data saved to Child Parquet.")
    # except Exception as e:
    #     print(f"Error in saving data to Child Parquet: {e}")

    # try:
    #     df_parent = processor.load_parquet_parent("parent.parquet")
    #     processor.process_table_parent(df_parent, chunk_size=50000)
    #     print("Data processed and inserted in parent table.")
    # except Exception as e:
    #     print(f"Error in processing data in parent table: {e}")
    # try:
    #     df_child = processor.load_parquet_child("child.parquet")
    #     processor.process_table_child(df_child, chunk_size=50000)
    #     print("Data processed and inserted in child table.")
    # except Exception as e:
    #     print(f"Error in processing data in child table: {e}")

    # try:
    #     processor.analysis(chunk_size=50000)
    #     print("Analysis completed and saved.")
    # except Exception as e:
    #     print(f"Error in analysis and saving results: {e}")

    try:
        processor.analysis_by_home(chunk_size=50000)
        print("Analysis completed and saved.")
    except Exception as e:
        print(f"Error in analysis and saving results: {e}")








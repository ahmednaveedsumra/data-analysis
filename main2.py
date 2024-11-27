import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from datetime import datetime
from config import config


class S3Uploader:

    def __init__(self, aws_config):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY']
        )
        self.bucket_name = aws_config['BUCKET_NAME']

    def upload_to_s3(self, dataframe, file_name):
        try:
            temp_file = f"{file_name}.parquet"
            dataframe.to_parquet(temp_file)
            print(f"Parquet file {temp_file} created locally.")

            s3_file_path = f"analysis_ahmad/{file_name}.parquet"

            self.s3_client.upload_file(temp_file, self.bucket_name, s3_file_path)
            print(f"File {s3_file_path} uploaded to S3 bucket {self.bucket_name}.")
        except NoCredentialsError:
            print("AWS credentials not found.")
        except Exception as e:
            print(f"Error uploading file to S3: {e}")

class DatabaseHandler:
    def __init__(self, db_config):
        self.db_config = db_config

    def initialize_engine(self):
        db_url = self._create_db_url()
        return create_engine(db_url)

    def _create_db_url(self):
        configure = self.db_config
        configure['PASSWORD'] = configure['PASSWORD'].replace('@', '%40')
        db_url = f"mysql+pymysql://{configure['USERNAME']}:{configure['PASSWORD']}@{configure['HOST']}:{configure['PORT']}/{configure['DATABASE_NAME']}"
        if 'CLIENT_CERT_PATH' in configure and 'CLIENT_KEY_PATH' in configure:
            db_url += f"?ssl_cert={configure['CLIENT_CERT_PATH']}&ssl_key={configure['CLIENT_KEY_PATH']}"
        return db_url

    def save_to_database(self, dataframe, table_name):
        engine = self.initialize_engine()
        try:
            with engine.connect() as connection:
                dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)
                print(f"Data saved to table: {table_name}")
        except DatabaseError as e:
            raise Exception(f"Error saving data to database: {e}")


class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.s3_uploader = S3Uploader(config['AWS'])
        self.parent_db_handler = DatabaseHandler(config['Default'])
        self.child_db_handler = DatabaseHandler(config['Default'])
        self.home_db_handler = DatabaseHandler(config['Write'])
        self.date_db_handler = DatabaseHandler(config['Write'])

    def fetch_all_data(self, db_handler, table_name, chunk_size=50000):
        engine = db_handler.initialize_engine()
        all_data = []
        try:
            with engine.connect() as connection:
                query = f"SELECT * FROM {table_name}"
                for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
                    all_data.append(chunk)
                print(f"Data fetched successfully from table {table_name} in chunks.")
        except DatabaseError as e:
            raise Exception(f"Database error: {e}")

        return pd.concat(all_data, ignore_index=True)


    def save_to_parquet(self, dataframe, file_prefix):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_filename = f"{file_prefix}_{timestamp}.parquet"
        dataframe.to_parquet(parquet_filename)
        print(f"Data saved to {parquet_filename}")
        return parquet_filename

    def perform_home_based_analysis_and_save(self, df_parent, df_child):
        df_child['createdDate'] = pd.to_datetime(df_child['createdDate'])
        df_child_filtered = df_child[df_child['createdDate'] >= '2023-01-01']
        df_child_filtered.loc[:, 'rent'] = pd.to_numeric(df_child_filtered['rent'], errors='coerce')
        df_child_filtered = df_child_filtered.dropna(subset=['rent'])

        df_merged = df_child_filtered.merge(
            df_parent[['id', 'city']], left_on='homeId', right_on='id', how='left'
        )

        def detect_outliers(group):
            Q1 = group['rent'].quantile(0.25)
            Q3 = group['rent'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            group['is_outlier'] = (~group['rent'].between(lower_bound, upper_bound)).astype(int)
            return group

        df_with_outliers = df_merged.groupby('city').apply(detect_outliers)

        rent_stats = df_with_outliers.groupby('homeId').agg(
            mean_rent=('rent', 'mean'),
            median_rent=('rent', 'median'),
            rent_count=('rent', 'count')
        ).reset_index()

        df_final = df_with_outliers.merge(rent_stats, on='homeId', how='left')

        df_final_selected = df_final[[
            'homeId', 'id_x', 'rent', 'createdDate', 'key', 'city', 'is_outlier', 'mean_rent', 'median_rent', 'rent_count'
        ]].rename(columns={'id_x': 'childId'})

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"outliertable_home_{timestamp}"
        self.home_db_handler.save_to_database(df_final_selected, table_name)

        file_name = f"home_based_analysis_{timestamp}"
        df_final_selected.to_csv(f"{file_name}.csv", index=False)
        print(f"Home-based analysis saved as CSV: {file_name}.csv")

        self.s3_uploader.upload_to_s3(df_final_selected, file_name)

    def perform_date_based_analysis_and_save(self, df_child):
        df_child['createdDate'] = pd.to_datetime(df_child['createdDate'])
        df_child_filtered = df_child[df_child['createdDate'] >= '2023-01-01']
        df_child_filtered.loc[:, 'rent'] = pd.to_numeric(df_child_filtered['rent'], errors='coerce')
        df_child_filtered = df_child_filtered.dropna(subset=['rent'])

        def detect_outliers(group):
            Q1 = group['rent'].quantile(0.25)
            Q3 = group['rent'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            group['is_outlier'] = (~group['rent'].between(lower_bound, upper_bound)).astype(int)
            return group

        df_with_outliers = df_child_filtered.groupby('createdDate').apply(detect_outliers).reset_index(drop=True)

        rent_stats = df_with_outliers.groupby('createdDate').agg(
            mean_rent=('rent', 'mean'),
            median_rent=('rent', 'median'),
            rent_count=('rent', 'count')
        ).reset_index()

        df_final = df_with_outliers.merge(rent_stats, on='createdDate', how='left')

        df_final_selected = df_final[[
            'id', 'rent', 'createdDate', 'key',  'is_outlier', 'mean_rent', 'median_rent', 'rent_count'
        ]]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"outliertable_date_{timestamp}"
        self.date_db_handler.save_to_database(df_final_selected, table_name)

        file_name = f"date_based_analysis_{timestamp}"
        df_final_selected.to_csv(f"{file_name}.csv", index=False)
        print(f"Date-based analysis saved as CSV: {file_name}.csv")

        self.s3_uploader.upload_to_s3(df_final_selected, file_name)


if __name__ == '__main__':
    processor = DataProcessor(config)

    try:
        df_parent = processor.fetch_all_data(processor.parent_db_handler, 'amh_daily_homes')
        print("All parent data fetched.")

        df_child = processor.fetch_all_data(processor.child_db_handler, 'amh_daily_pricing')
        print("All child data fetched.")

        parent_parquet_file = processor.save_to_parquet(df_parent, "parent")
        print(f"Parent data saved to Parquet: {parent_parquet_file}")

        child_parquet_file = processor.save_to_parquet(df_child, "child")
        print(f"Child data saved to Parquet: {child_parquet_file}")
    except Exception as e:
        print(f"Error during data fetching or saving to Parquet: {e}")
        exit()

    try:

        df_parent = pd.read_parquet(parent_parquet_file)
        df_child = pd.read_parquet(child_parquet_file)

        processor.perform_home_based_analysis_and_save(df_parent, df_child)
        print("Home-based analysis completed.")
    except Exception as e:
        print(f"Error during home-based analysis: {e}")

    try:
        processor.perform_date_based_analysis_and_save(df_child)
        print("Date-based analysis completed.")
    except Exception as e:
        print(f"Error during date-based analysis: {e}")





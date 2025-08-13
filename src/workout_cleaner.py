import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

class SupabaseCleaner:

    def __init__(self):
        load_dotenv()
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_ANON_KEY')
        if not self.url or not self.key:
            raise ValueError('BMissing SUPABASE_URL or SUPABASE_KEY in .env file')
        self.client = self._create_client()

    def _create_client(self) -> Client:
        return create_client(self.url, self.key)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        response = self.client.table(table_name).select('*').execute()
        if not response.data:
            raise ValueError(f'Table {table_name} is empty or do not exist.')
        return pd.DataFrame(response.data)

    def overwrite_table(self, table_name: str, df: pd.DataFrame, chunk_size: int = 500):
        self.client.table(table_name).delete().neq('row_id', 0).execute()

        records = df.to_dict(orient='records')
        for i in range(0, len(records), chunk_size):
            self.client.table(table_name).insert(records[i:i+chunk_size]).execute()

    def execute_sql(self, sql: str):
        print(f'[INFO] SQL to execute:\n{sql}\n---')

    def drop_column(self, table_name: str, column_name: str):
        sql = f'ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name};'
        self.execute_sql(sql)

class WorkoutsProcessor:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def remove_column(self, column_name: str):
        if column_name in self.df.columns:
            self.df = self.df.drop(columns=[column_name])
        return self

    def get_df(self) -> pd.DataFrame:
        return self.df


def main():
    table_name = 'workouts'

    supabase = SupabaseCleaner()

    # ----------------- Schema operations -----------------
    supabase.drop_column(table_name, 'equipment_type')

    # ----------------- Data operations -----------------
    df = supabase.fetch_table(table_name)
    print(f'Fetched {len(df)} rows')

    processor = WorkoutsProcessor(df)
    processor.remove_column('equipment_type')
    df_clean = processor.get_df()

    supabase.overwrite_table(table_name, df_clean)

if __name__ == '__main__':
    main()

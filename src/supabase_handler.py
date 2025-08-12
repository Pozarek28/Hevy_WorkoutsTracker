import os
import time
import pandas as pd
from supabase import create_client
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

class SupabaseHandler:
    def __init__(self):
        load_dotenv()
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        self.db_url = os.getenv('DATABASE_URL')
        self.engine = create_engine(self.db_url)

        if not all([self.supabase_url, self.supabase_key, self.db_url]):
            raise ValueError('Missing required environment variables for Supabase connection')

        self.supabase = create_client(self.supabase_url, self.supabase_key)

    def table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table_from_df(self, df: pd.DataFrame, table_name: str):
        with self.engine.connect() as conn:
            df.head(0).to_sql(table_name, conn, if_exists='replace', index=False)
        print(f'Table {table_name} created successfully')

    def get_existing_table(self, table_name: str) -> pd.DataFrame:
        all_rows = []
        offset = 0
        limit = 1000

        while True:
            resp = self.supabase.table(table_name).select('*').range(offset, offset + limit - 1).execute()
            if not resp.data:
                break
            all_rows.extend(resp.data)
            offset += limit
            time.sleep(0.2)

        return pd.DataFrame(all_rows)
    
    def _ensure_datetime_to_string(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
        return df
    
    def _insert_in_chunks(self, df: pd.DataFrame, table_name: str, chunk_size: int = 400):
        records = df.to_dict(orient='records')
        for i in range(0, len(records), chunk_size):
            self.supabase.table(table_name).insert(records[i:i+chunk_size]).execute()

    def overwrite_table(self, df: pd.DataFrame, table_name: str):
        df = self._ensure_datetime_to_string(df)

        if not self.table_exists(table_name):
            self.create_table_from_df(df, table_name)
            print(f'Table {table_name} created')

        self.supabase.table(table_name).delete().neq('workout_id', '').execute()
        self._insert_in_chunks(df, table_name)
        print(f'Table {table_name} overwritten with {len(df)} rows')

    def append_unique(self, df: pd.DataFrame, table_name: str):
        df = self._ensure_datetime_to_string(df)

        if not self.table_exists(table_name):
            self.create_table_from_df(df, table_name)
            self._insert_in_chunks(df, table_name)
            print(f'Created table {table_name} with {len(df)} rows')
            return
        
        existing_ids = self.supabase.table(table_name).select('row_id').execute()
        existing_ids_set = {row['row_id'] for row in existing_ids.data}

        df_new = df[~df['row_id'].isin(existing_ids_set)]

        if df_new.empty:
            print(f'No new rows to insert into {table_name}')
            return

        self._insert_in_chunks(df_new, table_name)
        print(f'Inserted {len(df_new)} new rows into {table_name}')
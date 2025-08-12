from src.api_client import get_data, BASE_URL_WORKOUTS, BASE_URL_ROUTINES
from src.data_processor import DataProcessor
from src.supabase_handler import SupabaseHandler

def main():

    processor = DataProcessor()
    supabase_handler = SupabaseHandler()

    # Get workouts data
    workouts_data = get_data(BASE_URL_WORKOUTS)
    df_workouts = processor.process_workouts(workouts_data)

    # Get routines data
    routines_data = get_data(BASE_URL_ROUTINES)
    df_routines = processor.process_routines(routines_data)

    supabase_handler.append_unique(df_workouts, 'workouts')
    supabase_handler.overwrite_table(df_routines, 'routines')

if __name__ == '__main__':
    main()

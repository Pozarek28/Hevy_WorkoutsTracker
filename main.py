from src.api_client import get_data, BASE_URL_WORKOUTS, BASE_URL_ROUTINES
from src.data_processor import DataProcessor

def main():

    processor = DataProcessor()

    # Get workouts data
    workouts_data = get_data(BASE_URL_WORKOUTS)
    df_workouts = processor.process_workouts(workouts_data)

    # Get routines data
    routines_data = get_data(BASE_URL_ROUTINES)
    df_routines = processor.process_routines(routines_data)

if __name__ == "__main__":
    main()

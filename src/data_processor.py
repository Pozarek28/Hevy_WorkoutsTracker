import pandas as pd
import re

class DataProcessor:
    def __init__(self):
        pass

    def _clean_exercise_title(self, exercise_title):
        # Extract content in parentheses and clean exercise title
        match = re.search(r'\((.*?)\)', exercise_title)
        if match:
            content = match.group(1)
            exercise_title = re.sub(r'\s*\(.*?\)', '', exercise_title)
        else:
            content = None
        return exercise_title.strip(), content

    def _assign_equipment_type(self, row):
        # Assign equipment type based on exercise title keywords
        exercise_title = row['exercise_title'].lower()
        if 'butterfly' in exercise_title:
            return 'Machine'
        elif 'face pull' in exercise_title:
            return 'Cable'
        elif 'seated' in exercise_title:
            return 'Machine'
        elif 'cable' in exercise_title:
            return 'Cable'
        elif 't bar' in exercise_title:
            return 'Machine'
        elif 'rope' in exercise_title:
            return 'Cable'
        else:
            return row['equipment_type']

    def process_routines(self, routines_data):
        # Process routines data into a cleaned DataFrame 
        routines_list = routines_data['routines']
        parsed_data = []

        for routine in routines_list:
            workout_id = routine['id']
            title = routine['title']
            updated_time = routine['updated_at']
            created_time = routine['created_at']

            for exercise in routine['exercises']:
                exercise_title = exercise['title']
                exercise_notes = exercise['notes']

                for set_info in exercise['sets']:
                    set_index = set_info['index']

                    parsed_data.append({
                        'workout_id': workout_id,
                        'created_at': created_time,
                        'updated_at': updated_time,
                        'workout_title': title,
                        'exercise_title': exercise_title,
                        'exercise_notes': exercise_notes,
                        'set_index': set_index + 1,
                    })

        df = pd.DataFrame(parsed_data)

        # Clean exercise titles and assign equipment
        df[['exercise_title', 'equipment_type']] = df['exercise_title'].apply(
            lambda x: pd.Series(self._clean_exercise_title(x))
        )
        df['equipment_type'] = df.apply(self._assign_equipment_type, axis=1)

        # Format date columns
        df[['created_at', 'updated_at']] = df[['created_at', 'updated_at']].apply(pd.to_datetime)
        df[['created_at', 'updated_at']] = df[['created_at', 'updated_at']].apply(
            lambda x: x.dt.floor('s').dt.tz_localize(None)
        )

        # Group sets
        cols_to_group = [col for col in df.columns if col != 'set_index']
        routines_grouped = df.groupby(cols_to_group).agg(
            sets=('set_index', 'count')
        ).reset_index()

        routines_grouped['workout_plan'] = routines_grouped['workout_title'].str.split(' ').str[0]

        return routines_grouped

    def process_workouts(self, workouts_data):
        # Process workouts data into a cleaned DataFrame
        workouts_list = workouts_data['workouts']
        parsed_data = []

        for workout in workouts_list:
            workout_id = workout['id']
            title = workout['title']
            start_time = workout['start_time']
            end_time = workout['end_time']

            for exercise in workout['exercises']:
                exercise_title = exercise['title']
                exercise_notes = exercise['notes']
                superset_id = exercise['superset_id']

                for set_info in exercise['sets']:
                    parsed_data.append({
                        'workout_id': workout_id,
                        'workout_title': title,
                        'start_time': start_time,
                        'end_time': end_time,
                        'superset_id': superset_id,
                        'exercise_title': exercise_title,
                        'exercise_notes': exercise_notes,
                        'set_index': set_info['index'] + 1,
                        'weight_kg': set_info['weight_kg'],
                        'reps': set_info['reps'],
                        'duration': set_info['duration_seconds']
                    })

        df = pd.DataFrame(parsed_data)

        # Format datetime
        df[['start_time', 'end_time']] = df[['start_time', 'end_time']].apply(pd.to_datetime)
        df[['start_time', 'end_time']] = df[['start_time', 'end_time']].apply(
            lambda x: x.dt.floor('s').dt.tz_localize(None)
        )

        # Clean exercise titles and assign equipment
        df[['exercise_title', 'equipment_type']] = df['exercise_title'].apply(
            lambda x: pd.Series(self._clean_exercise_title(x))
        )
        df['equipment_type'] = df.apply(self._assign_equipment_type, axis=1)

        df['workout_plan'] = df['workout_title'].str.split(' ').str[0]

        return df

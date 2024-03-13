import pandas as pd

# Read the Data containing the Fredericton Transit Schedule as an Excel file
# Each worksheet contains the schedule for a different route, with the worksheet name being the route identifier
# The second row of each worksheet contains the column headers, below which is the schedule for the route

# Read the Excel file into a dictionary of DataFrames, with the keys being the worksheet names
# The DataFrames are indexed by the second row of each worksheet
# Read as strings only, as we will be converting the values to DateTime objects later
transit_schedule_data = pd.read_excel('data/Fredericton_Transit_Schedule.xlsx', sheet_name=None, header=1, index_col=None, dtype=str)
# STEP 1: Pre-Processing of the Dataframes
# Read all the values as DateTime objects, and remove any values that are not able to be read as DateTime objects
# This will remove any values that are not in the format HH:MM:SS
for route, schedule in transit_schedule_data.items():
    try:
        # Read the values as DateTime objects, and remove any values that are not able to be read as DateTime time objects
        # Retain only the time portion of the DateTime objects
        schedule = schedule.apply(pd.to_datetime, errors='coerce')
        # Set Noon as a Time object
        noon = pd.to_datetime('12:00:00', format='%H:%M:%S').time()
        # Iterate through all the values in the DataFrame
        for column in schedule.columns:
            after_noon_flag = False
            for row in schedule.index:
                if not pd.isnull(schedule.at[row, column]):
                    if after_noon_flag and schedule.at[row, column].hour !=12:
                        #  Convert everything to PM after the first occurrence of a time in PM
                        schedule.at[row, column] = schedule.at[row, column].replace(hour=schedule.at[row, column].hour + 12).time()
                        continue
                    if schedule.at[row, column].time() >= noon:
                        after_noon_flag = True
                    schedule.at[row, column] = schedule.at[row, column].time()
        transit_schedule_data[route] = schedule
    except ValueError:
        # If the values are already in the correct format, then the above line will throw a ValueError
        # In this case, we can just pass
        pass
# Write out the pre-processed DataFrames to a new Excel file
with pd.ExcelWriter('data/Fredericton_Transit_Schedule_Preprocessed.xlsx') as writer:
    for route, schedule in transit_schedule_data.items():
        schedule.to_excel(writer, sheet_name=route, index=False)
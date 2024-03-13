import pandas as pd
import datetime as dt

pd.options.mode.chained_assignment = None

transit_data = pd.read_csv("data/Fredericton_Hotspot_Transit_w_Boarding_Stops.csv")
transit_data["Start_Date"] = pd.to_datetime(
    transit_data["Start_Date"], format="%Y-%m-%d"
).dt.date
# Convert the Start_Time column to datetime format and normalize it to 24-hour format, and retain just the hour of the day
transit_data["Start_Time"] = pd.to_datetime(transit_data["Start_Time"]).dt.time

# Read every value as string, as we will be converting the values to DateTime objects later
transit_schedule_data = pd.read_excel(
    "data\\resources\\Fredericton Transit Schedule\\Fredericton_Transit_Schedule.xlsx",
    sheet_name=None,
    header=0,
    index_col=None,
    dtype=str,
)
transit_data_for_routes = []

# STEP 1: Pre-Processing of the Dataframes
# For each of the values in the Start_Time column, find out where it lies in the schedule based on that
# Assign the boarding_stop(column_name) value, for each row
for route, schedule in transit_schedule_data.items():
    transit_data_for_route = transit_data[transit_data["Route"] == route].copy().reset_index(drop=True)
    # Create a new column to store the boarding_stop value
    transit_data_for_route["Boarding_Stop"] = [float("nan")]*transit_data_for_route.shape[0]
    schedule_df = pd.DataFrame(columns=["Boarding_Stop"])
    for column_name in schedule.columns:
        for element in schedule[column_name]:
            schedule_df.loc[element, "Boarding_Stop"] = column_name
    # Change index to a Column = Stop_Time
    schedule_df.reset_index(inplace=True)
    schedule_df.rename({"index": "Stop_Time"}, axis=1, inplace=True)
    # Drop any rows with missing values
    schedule_df.dropna(inplace=True)
    # Convert the Stop_Time column to datetime format and normalize it to 24-hour format
    schedule_df["Stop_Time"] = pd.to_datetime(schedule_df["Stop_Time"]).dt.time
    # Sort by the Stop_Time column
    schedule_df.sort_values("Stop_Time", inplace=True)
    schedule_df.reset_index(drop=True, inplace=True)
    # Find the closest time in the schedule for each row in the transit_data_for_route DataFrame, that is after the Start_Time
    for index, row in transit_data_for_route.iterrows():
        start_time = row["Start_Time"]
        # Assume that users board the bus 2 minutes before the scheduled departure time
        # So add 2 minutes to the Start_Time, it is already a time object
        if start_time.minute < 58:
            start_time = dt.time(start_time.hour, start_time.minute + 2)
        else:
            start_time = dt.time(start_time.hour + 1, (start_time.minute - 58))
        # Find the closest time in the schedule for each row in the transit_data_for_route DataFrame, that is after the Start_Time
        closest_time = schedule_df[schedule_df["Stop_Time"] >= start_time][
            "Stop_Time"
        ].min()
        # Find the boarding_stop value for the closest_time
        boarding_stop = schedule_df[schedule_df["Stop_Time"] == closest_time][
            "Boarding_Stop"
        ].values
        if len(boarding_stop) >= 1:
            boarding_stop = boarding_stop[0]
            # Assign the boarding_stop value to the row in the transit_data_for_route DataFrame
            transit_data_for_route.loc[index, "Boarding_Stop"] = boarding_stop
        else:
            pass
    # Append transit data for the current route to a list to be concatenated later
    transit_data_for_routes.append(transit_data_for_route)
transit_data = pd.concat(transit_data_for_routes)
transit_data.dropna(inplace=True)
transit_data.reset_index(drop=True, inplace=True)
# Write out the pre-processed transit data to a CSV file
transit_data.to_csv("data/Fredericton_Hotspot_Transit_w_Boarding_Stops.csv", index=False)

""" WEATHER DATA PRE-PROCESSING """
weather_data = pd.read_csv("data/Daily_Weather.csv")
relevant_weather_attributes = [
    # Temperature-related Variables
    "max_temperature",  # Influences rider comfort on hot days
    "avg_temperature",  # Provides overall daily weather comfort
    "min_temperature",  # Influences rider comfort on cold days
    # Humidity-related Variables
    "max_relative_humidity",  # High humidity can affect comfort
    "avg_relative_humidity",  # Average humidity level's impact on comfort
    "min_relative_humidity",  # Low humidity can affect comfort
    # Wind-related Variables
    "max_wind_speed",  # Strong winds can impact comfort
    "avg_wind_speed",  # Average daily wind speed
    # Precipitation-related Variables
    "precipitation",  # Presence of precipitation in general
    "rain",  # Rain's impact on transit usage
    "snow",  # Snow's impact on transit usage
    # Daylight-related Variables
    "sunrise_hh",  # Morning sunlight's influence on commuter ridership
    # Visibility-related Variables
    "avg_visibility",  # Average daily visibility
]

# Normalise the Sunrise Time to 24-hour format, and retain just the hour of the day
weather_data["sunrise_hh"] = pd.to_datetime(weather_data["sunrise_hhmm"]).dt.hour
weather_data.drop("sunrise_hhmm", axis=1, inplace=True)
weather_data = weather_data[["date"] + relevant_weather_attributes]
weather_data["date"] = pd.to_datetime(weather_data["date"], infer_datetime_format=True).dt.date
# Merge the two datasets on the date column
transit_weather_data = pd.merge(
    transit_data, weather_data, left_on="Start_Date", right_on="date"
)
transit_weather_data.drop("Start_Date", axis=1, inplace=True)
# Convert the column names to lowercase
transit_weather_data.columns = transit_weather_data.columns.str.lower()

# Data Preparation: Data Wrangling & Feature Engineering
""" Date & Time Features """
# Create Separate columns for day, month and year - Date Columns
transit_weather_data["day"] = transit_weather_data["date"].apply(lambda x: x.day)
transit_weather_data["month"] = transit_weather_data["date"].apply(lambda x: x.month)
transit_weather_data["year"] = transit_weather_data["date"].apply(lambda x: x.year)
# Assign week of the year based on the date, 1-52, as integer
transit_weather_data["week_of_year"] = transit_weather_data["date"].apply(
    lambda x: x.isocalendar()[1]
)
# Create a new column 'month_year' by combining 'month' and 'year' and sort the DataFrame by this column
transit_weather_data['month_year'] = transit_weather_data.apply(lambda row: row['date'].strftime('%b-%Y'), axis=1)


""" Sessions & Popularity Calculations """
# Calculate 'total_sessions' & 'unique_users' for each route for a day, then week, then month
transit_weather_data["total_sessions_daily"] = transit_weather_data.groupby(
    ["route", transit_weather_data["date"]]
)["session_id"].transform("count")
transit_weather_data["unique_users_daily"] = transit_weather_data.groupby(
    ["route", transit_weather_data["date"]]
)["user_id"].transform("nunique")
transit_weather_data["week_of_year"] = transit_weather_data["date"].apply(lambda x: x.isocalendar()[1])
transit_weather_data["total_sessions_weekly"] = transit_weather_data.groupby(
    ["route", "week_of_year"]
)["session_id"].transform("count")
transit_weather_data["unique_users_weekly"] = transit_weather_data.groupby(
    ["route", "week_of_year"]
)["user_id"].transform("nunique")
transit_weather_data["month_of_year"] = transit_weather_data["date"].apply(lambda x: x.month)
transit_weather_data["total_sessions_monthly"] = transit_weather_data.groupby(
    ["route", "month_of_year"]
)["session_id"].transform("count")
transit_weather_data["unique_users_monthly"] = transit_weather_data.groupby(
    ["route", "month_of_year"]
)["user_id"].transform("nunique")

# Define the popularity threshold to categorize routes
# As the 25th, 50th & 75th percentile of the total_sessions_daily column
threshold_high_popularity = transit_weather_data['total_sessions_daily'].quantile(0.75)
threshold_moderate_popularity = transit_weather_data['total_sessions_daily'].quantile(0.50)
threshold_low_popularity = transit_weather_data['total_sessions_daily'].quantile(0.25)
# Create a target variable for route daily popularity categories
transit_weather_data['daily_popularity'] = 'Very Less Popular'
transit_weather_data.loc[transit_weather_data['total_sessions_daily'] >= threshold_low_popularity, 'daily_popularity'] = 'Less Popular'
transit_weather_data.loc[transit_weather_data['total_sessions_daily'] >= threshold_moderate_popularity, 'daily_popularity'] = 'Moderately Popular'
transit_weather_data.loc[transit_weather_data['total_sessions_daily'] >= threshold_high_popularity, 'daily_popularity'] = 'Highly Popular'
# Assign Season based on the month
transit_weather_data["Season"] = transit_weather_data["date"].apply(
    lambda x: "Winter"
    if x.month in [12, 1, 2, 3, 4]
    else ("Summer" if x.month in [5, 6, 7, 8] else "Fall")
)
# Treat Missing Values: Imputation of Missing Values with Mean based on Week of the Year, so that the seasonality is preserved
for column in relevant_weather_attributes:
    if column == "sunrise_hh":
        # Skip the sunrise column, as it is not numeric
        continue
    # Generate the names for the new columns: avg_weekly_<column> & avg_monthly_<column>
    avg_weekly_column = "avg_weekly_" + column.replace("avg_", "")
    avg_monthly_column = "avg_monthly_" + column.replace("avg_", "")
    # Group by week of the year and calculate the mean for each week
    transit_weather_data[avg_weekly_column] = transit_weather_data.groupby(
        transit_weather_data["week_of_year"]
    )[column].transform("mean").round(2)
    # Calculate Average Monthly Value as well
    transit_weather_data[avg_monthly_column] = transit_weather_data.groupby(
        transit_weather_data["month_of_year"]
    )[column].transform("mean").round(2)
    # Replace missing values in the column with the weekly mean
    transit_weather_data[column].fillna(
        transit_weather_data[avg_weekly_column], inplace=True
    )
""" Segregating Data based on popularity """
unique_routes = transit_weather_data['route'].unique()
unique_users = transit_weather_data['user_id'].unique()
unique_sessions = transit_weather_data['session_id'].unique()
# Create a DataFrame to store unique user counts for each route
unique_users_per_route = transit_weather_data.groupby('route')['user_id'].nunique().sort_values(ascending=False).reset_index()
# Create a DataFrame to store unique usages for each route
unique_usages_per_route = transit_weather_data.groupby('route')['session_id'].nunique().sort_values(ascending=False).reset_index()
# Combine the two DataFrames
route_usage_metrics = unique_usages_per_route.merge(unique_users_per_route, on='route', how='left').rename({"user_id": "unique_users", "session_id": "unique_usages"}, axis=1)
route_usage_metrics.sort_values('unique_usages', ascending=False, inplace=True)
# Remove outliers only for the routes with the lowest usage, to avoid skewing the data
lower_threshold = 400
route_usage_metrics = route_usage_metrics[route_usage_metrics['unique_usages'] >= lower_threshold]
# Segregate the routes into 3 levels of popularity: Highly Popular, Moderately Popular, Less Popular
median_usage = route_usage_metrics['unique_usages'].median()
route_usage_metrics['overall_popularity'] = 'Less Popular'
route_usage_metrics.loc[route_usage_metrics['unique_usages'] >= median_usage, 'overall_popularity'] = 'Moderately Popular'
route_usage_metrics.loc[route_usage_metrics['unique_usages'] >= median_usage * 2, 'overall_popularity'] = 'Highly Popular'
# We are going to do any further analysis based on the 3 levels of popularity
# Separate the routes into 3 DataFrames based on popularity
less_popular_routes = route_usage_metrics[route_usage_metrics['overall_popularity'] == 'Less Popular']
moderately_popular_routes = route_usage_metrics[route_usage_metrics['overall_popularity'] == 'Moderately Popular']
highly_popular_routes = route_usage_metrics[route_usage_metrics['overall_popularity'] == 'Highly Popular']
# Assign popularity levels to the transit_weather_data DataFrame, as 'overall_popularity'
transit_weather_data = transit_weather_data.merge(route_usage_metrics[['route', 'overall_popularity']], on='route', how='left')
transit_weather_data.reset_index(drop=True, inplace=True)
transit_weather_data.drop_duplicates(inplace=True)
transit_weather_data.columns = transit_weather_data.columns.str.lower()
transit_weather_data.to_csv("data/Transit_Weather.csv", index=False)

# Arrange the columns in a logical order, with all weather attributes coming later in the DataFrame
# Find the index of the 'date' column
transit_weather_data = transit_weather_data[
    [
        "date",
        "day",
        "month",
        "year",
        "week_of_year",
        "month_year",
        "season",
        "route",
        "boarding_stop",
        "start_time",
        "user_id",
        "session_id",
        "total_sessions_daily",
        "unique_users_daily",
        "total_sessions_weekly",
        "unique_users_weekly",
        "total_sessions_monthly",
        "unique_users_monthly",
        "overall_popularity",
        "max_temperature",
        "avg_temperature",
        "min_temperature",
        "max_relative_humidity",
        "avg_relative_humidity",
        "min_relative_humidity",
        "max_wind_speed",
        "avg_wind_speed",
        "precipitation",
        "rain",
        "snow",
        "sunrise_hh",
        "avg_visibility",
        "avg_weekly_max_temperature",
        "avg_monthly_max_temperature",
        "avg_weekly_temperature",
        "avg_monthly_temperature",
        "avg_weekly_min_temperature",
        "avg_monthly_min_temperature",
        "avg_weekly_max_relative_humidity",
        "avg_monthly_max_relative_humidity",
        "avg_weekly_relative_humidity",
        "avg_monthly_relative_humidity",
        "avg_weekly_min_relative_humidity",
        "avg_monthly_min_relative_humidity",
        "avg_weekly_max_wind_speed",
        "avg_monthly_max_wind_speed",
        "avg_weekly_wind_speed",
        "avg_monthly_wind_speed",
        "avg_weekly_precipitation",
        "avg_monthly_precipitation",
        "avg_weekly_rain",
        "avg_monthly_rain",
        "avg_weekly_snow",
        "avg_monthly_snow",
        "avg_weekly_visibility",
        "avg_monthly_visibility",
    ]
]

""" Preliminary Data Description"""
print("Seasons: Winter (Dec-Apr), Summer (May-Aug), Fall (Sep-Nov)")
# Print how many days, months and years of data we have
# Find out the min & max dates, using the date column
first_date = transit_weather_data['date'].min()
last_date = transit_weather_data['date'].max()
print(f"The data spans from {dt.datetime.strftime(first_date, '%d-%b-%Y')} to {dt.datetime.strftime(last_date, '%d-%b-%Y')}")
# Describe range as x years, y months and z days
date_range = last_date - first_date
years = date_range.days // 365
months = (date_range.days % 365) // 30
days = (date_range.days % 365) % 30
print(f"The data spans {years} years, {months} months and {days} days")



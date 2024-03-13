import datetime as dt

import pandas as pd

pd.options.mode.chained_assignment = None

transit_data = pd.read_csv("data/Fredericton_Hotspot_Transit.csv")
transit_data["Start_Date"] = pd.to_datetime(
    transit_data["Start_Date"], infer_datetime_format=True
).dt.date
transit_data["Start_Time"] = pd.to_datetime(
    transit_data["Start_Time"], infer_datetime_format=True
).dt.time
# Remove any rows where the start_time is before 6:00 AM or after 11:00 PM
transit_data = transit_data[
    transit_data["Start_Time"].between(dt.time(6, 0), dt.time(23, 0))
]
# Remove any rows where the start_date is on a sunday
for i in range(len(transit_data)):
    if transit_data.iloc[i]["Start_Date"].weekday() == 6:
        # Drop the row if the start_date is on a sunday
        transit_data.loc[i, "ObjectId"] = "drop"
# Remove any rows with missing values or having drop in the ObjectId column
transit_data = transit_data.dropna()
transit_data = transit_data[transit_data["ObjectId"] != "drop"]
# Remove any rows where the user_ID, route, start_date are the same AND the start_time is within 5 minutes of another
transit_data_sorted = transit_data.sort_values(
    ["User_ID", "Route", "Start_Date", "Start_Time"], ignore_index=True
)
for i in range(1, len(transit_data_sorted)):
    if (
            (transit_data_sorted.iloc[i]["User_ID"] == transit_data_sorted.iloc[i - 1]["User_ID"])
            and (transit_data_sorted.iloc[i]["Route"] == transit_data_sorted.iloc[i - 1]["Route"])
            and (transit_data_sorted.iloc[i]["Start_Date"] == transit_data_sorted.iloc[i - 1]["Start_Date"])
    ):
        if (dt.datetime.combine(dt.date(1, 1, 1), transit_data_sorted.iloc[i]["Start_Time"]) - dt.datetime.combine(
                dt.date(1, 1, 1), transit_data_sorted.iloc[i - 1]["Start_Time"])).seconds < 300:
            # Drop the row if the start_time is within 5 minutes of another row with the same user_ID, route, start_date
            transit_data_sorted.loc[i-1,"ObjectId"] = "drop"
# Drop all the rows that have a start_time within 5 minutes of another row with the same user_ID, route, start_date
# The row with the earlier time is dropped because the user is more likely to board once again at the later time
preprocessed_transit_data = transit_data_sorted[transit_data_sorted["ObjectId"] != "drop"]
preprocessed_transit_data = preprocessed_transit_data.drop(columns=["ObjectId"])
# Write the pre-processed data to a new CSV file
preprocessed_transit_data.to_csv("data/Fredericton_Hotspot_Transit_Preprocessed.csv", index=False)


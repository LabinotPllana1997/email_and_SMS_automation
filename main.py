import os
from datetime import date 
import pandas as pd
from send_emails import send_email
from deta import app
from twilio.rest import Client

# Public GoogleSheets url - not secure! - Accessible to anyone with link
# SHEET_ID = os.environ.get("SHEET_ID")
SHEET_ID_enable = "1t3ZYQMPQiaEuIIYhDGdciNfQ3Rhq_VE0MiW3l4GhpIc"

SHEET_NAME = "Sheet1"  


# This turns the google sheet into a readable CSV file
URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_enable}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

def send_sms(to, message):
    """
    Sends an SMS message to the specified phone number using the Twilio API.
    
    Parameters:
    - to: str: The phone number to send the message to.
    - message: str: The message to be sent in the SMS.

    Returns:
    - None
    """
    account_sid = os.environ.get("account_sid")
    auth_token = os.environ.get("auth_token")

    client = Client(account_sid, auth_token)
    
    from_number = "+15732843696"  # Your Twilio phone number
    
    client.messages.create(to=to, from_=from_number, body=message)


def load_df(url):
    '''
    Loads the Google Sheet data into a Pandas DataFrame.

    Parameters:
    url (str): URL of the Google Sheet

    Returns:
    df (DataFrame): DataFrame with the sheet data.
    '''
    parse_dates = ["session_date"]
    df = pd.read_csv(url, parse_dates=parse_dates)

    # Applying lambda function to add '+' to numbers, so SMS can be sent
    df["number"] = df["number"].apply(lambda x: "+" + str(x))
    return df

# print(load_df(URL))


def query_data_and_send_reminders(df):
    '''
    Queries the data in the DataFrame and sends reminders if the session date is today and rescheduled is "no".

    Parameters:
    df (DataFrame): DataFrame with the sheet data.

    Returns:
    result (str): Total number of emails and SMS sent.
    '''
    present = date.today()
    email_counter = 0
    sms_counter = 0
    for _, row in df.iterrows():
        if (present == row["session_date"].date()) and (row["rescheduled"] == "no"):
            send_email(
                subject=f'Reminder For Your Session Today at {row["session_time"]}',
                receiver_email=row["email"],
                name=row["name"],
                session_time=row["session_time"],
                student_name=row["student_name"],
                session_date=row["session_date"].strftime("%d, %b %Y"),  # example: 11, Aug 2022
                school_subject=row["school_subject"],
                zoom_link=row["zoom_link"],
            )
            email_counter += 1
            send_sms(to=row["number"], message=f'Reminder: You have a session today at {row["session_time"]}! Click here to join: {row["zoom_link"]}')
            sms_counter += 1

    return f"Total Emails Sent: {email_counter}, Total SMS Sent: {sms_counter}"


# df = load_df(URL)
# result = query_data_and_send_reminders(df)
# print(result)


@app.lib.cron()
def cron_job(event):
    
    df = load_df(URL)
    result = query_data_and_send_reminders(df)
    return result
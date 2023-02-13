import os
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
import ssl

from dotenv import load_dotenv  # pip install python-dotenv

PORT = 465
EMAIL_SERVER = "smtp.gmail.com"

# Load the environment variables
current_dir = Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()
envars = current_dir / ".env"
load_dotenv(envars)

# Read environment variables
sender_email = os.environ.get("EMAIL")
password_email = os.environ.get("PASSWORD")


def send_email(subject,
                receiver_email,
                name,
                student_name,
                session_date,
                session_time,
                school_subject,
                zoom_link):

    # Create the base text message.
    msg = EmailMessage()
    msg["Subject"] = subject
    # subjectvariable = "Reminder for Your ", f"{school_subject}", "Session Today @", f"{session_date}"
    # msg["Subject"] = subjectvariable
    msg["From"] = formataddr(("Plan-A Tuition", f"{sender_email}"))
    msg["To"] = receiver_email
    msg["BCC"] = sender_email

    msg.set_content(
        f"""\
        Hi {name},
        This is a reminder for your {school_subject} session today. The details for the session are:
        student name: {student_name}
        Date & time: {session_date} at {session_time}
        Zoom Link: {zoom_link}
        Best regards,
        Plan-A Tuition
        """
    )
    # Add the html version.  This converts the message into a multipart/alternative
    # container, with the original text message as the first part and the new html
    # message as the second part.
    msg.add_alternative(
        f"""\
    <html>
      <body>
        <p>Hi {name},</p>
        <p>This is a reminder for your {school_subject} session today. The details for the session are:</p>
        <p><strong>Student Name:</strong> {student_name}</p>
        <p><strong>Date & time:</strong> {session_date} at {session_time}</p>
        <p><strong>Zoom Link:</strong> {zoom_link}</p>
        <p>Best regards,</p>
        <p>Plan-A Tuition</p>
        <img align="left" alt="Coding" width="80" src="https://static.wixstatic.com/media/490b95_bb5003fe799246089ea98ead454cb5de~mv2.jpg/v1/fit/w_2500,h_1330,al_c/490b95_bb5003fe799246089ea98ead454cb5de~mv2.jpg">
      </body>
    </html>
    """,
        subtype="html",
    )
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(EMAIL_SERVER, PORT, context=context) as server:
        server.login(sender_email, password_email)
        server.sendmail(sender_email, receiver_email, msg.as_string())


if __name__ == "__main__":
    send_email(
        subject="Reminder For Your Session With Eva Today at 7 pm!",
        name="Tutor Name",
        student_name="Eva",
        session_time="7 pm",
        receiver_email="ahmed@theaicore.com",
        session_date="11, Nov 2022",
        school_subject="Maths",
        zoom_link="https://planatuition.zoom.us/j/2454830037",
    )
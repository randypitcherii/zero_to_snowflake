import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# config
LABS_DIR = './lab_content/lab_scripts'
FROM_NAME = 'Jane Doe'
FROM_EMAIL = 'your_email@asdf.com' # put the organizer's email here 
FROM_EMAIL_PASSWORD = 'your_password' # put the organizer's email account password here 
SNOWFLAKE_USERS_PASSWORD_PREFIX = 'aA1ffa' # This prefix covers the complexity requirements from snowflake regardless of the hash results
SNOWFLAKE_ACCOUNT_URL = 'https://asdf.snowflakecomputing.com/' # put the snowflake account url for the instance you're using
EMAIL_BODY = """
Hello:

My name is {from_name}. I'm leading your upcoming Snowflake training with Hashmap.

For our session, you will need the following:
- Your username: {username}
- Your password: {password}
- Our Snowflake login: {snowflake_account_url}

You will also need the attached lab SQL file. Please do not modify this.

It is very important that you wait until we start the training to run SQL commands. This Snowflake instance has a limited number of credits and excessive early usage will run the account dry before we can finish the training.

Thank you and please reach out with any questions!

{from_name}
Engineering @ Hashmap
"""

def send_email(send_to, username):
    msg = MIMEMultipart()

    # email meta
    msg['Subject'] = 'Hashmap Zero to Snowflake | Snowflake Training Material'
    msg['From'] = FROM_EMAIL
    msg['To'] = send_to 

    # body
    password = f'{SNOWFLAKE_USERS_PASSWORD_PREFIX}{hashlib.md5(username.encode("utf-8")).hexdigest()}' # generates random-ish password per user
    msg.attach(MIMEText(EMAIL_BODY.format(username=username, password=password, snowflake_account_url=SNOWFLAKE_ACCOUNT_URL, from_name=FROM_NAME)))

    # attach file
    filename = f"{LABS_DIR}/{username}_snowflake_lab.sql"
    with open(filename, "rb") as f: 
      ext = 'sql'
      attachedfile = MIMEApplication(f.read(), _subtype = ext)
      attachedfile.add_header('content-disposition', 'attachment', filename=filename)
    msg.attach(attachedfile)

    # send email
    smtp = smtplib.SMTP(host="smtp.gmail.com", port= 587) # Go here to make this work in your gmail account - https://myaccount.google.com/lesssecureapps
    smtp.starttls()
    smtp.login(FROM_EMAIL, FROM_EMAIL_PASSWORD)
    smtp.sendmail(FROM_EMAIL, send_to, msg.as_string())
    smtp.close()

def main():
  attendees = get_attendees()
  for (username, send_to) in attendees:
    print(f"Sending email to {send_to} with username {username}")
    # send_email(send_to, username)

def get_attendees():
  return [
    ("USER_01", "asdf@asdf.com"),
    ("USER_02", "asdfasdf@asdf.com"),
  ]

if __name__ == "__main__":
    main()

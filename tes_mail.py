import smtplib

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL = "ssdmprojecteerv@gmail.com"
APP_PASSWORD = "quhbwuhjsfdsaouy"
PASSWORD="ab12cd34de56"
try:
    # Connect to the SMTP server
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.set_debuglevel(1)  # Enable debug output to see communication logs
    server.ehlo()  # Identify ourselves to the SMTP server
    server.starttls()  # Upgrade the connection to secure (TLS)
    server.ehlo()  # Re-identify as encrypted
    server.login(EMAIL, APP_PASSWORD)  # Login to your Gmail account
    print("Login successful!")
    server.quit()
except smtplib.SMTPAuthenticationError as e:
    print(f"Authentication failed: {e}")
except Exception as e:
    print(f"An error occurred: {e}")

'''
def send_email_async(advice_type: str, symbol: str):
    def email_task():
        try:
            sender_email = smtp_user 
            recipient_email = smtp_recv
            smtp_server = "smtp.gmail.com"
            smtp_port = 587
            smtp_username = smtp_user
            smtp_password = smtp_pass 

            subject = f"Trade Alert: {advice_type} Signal for {symbol}"
            body = f"""
            A {advice_type} signal has been generated for symbol {symbol}.
            Please review your trading strategy and take appropriate action.
            """

            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = recipient_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            # Uncomment the following to enable sending emails
            # with smtplib.SMTP(smtp_server, smtp_port) as server:
            #     server.starttls()
            #     server.login(smtp_username, smtp_password)
            #     server.sendmail(sender_email, recipient_email, msg.as_string())

            logging.info(f"Email sent successfully for {advice_type} signal to {recipient_email}.")
        except Exception as e:
            logging.error(f"Failed to send email: {e}")

    # Create a ThreadPoolExecutor locally
    with ThreadPoolExecutor(max_workers=5) as local_executor:
        local_executor.submit(email_task)'''
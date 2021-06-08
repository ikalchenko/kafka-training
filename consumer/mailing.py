import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def compose_email(subject, sender, recipient_address, products):
    message = MIMEMultipart()
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = recipient_address
    products_str = ''
    for product in products:
        products_str += f'\n- {product}'
    message.attach(MIMEText(f'Thanks for your purchase! Order info: {products_str}', 'plain'))
    return message.as_string()


def send_order_ack_email(to, subject, products):
    sender_name = os.environ.get('SENDER_NAME')
    is_success = True

    try:
        server = smtplib.SMTP(host=os.environ.get('SMTP_HOST'),
                              port=os.environ.get('SMTP_PORT'))
        server.ehlo()
        server.starttls()
        server.login(user=os.environ.get('SENDER_EMAIL'),
                     password=os.environ.get('SENDER_PASSWORD'))
        server.sendmail(sender_name, to, compose_email(subject,
                                                       sender_name,
                                                       to,
                                                       products))
    except smtplib.SMTPAuthenticationError:
        print('Login Error, verify username/password')
        is_success = False
    except smtplib.SMTPDataError:
        print('Invalid data')
        is_success = False
    return is_success

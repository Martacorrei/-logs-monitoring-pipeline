import logging
import os

from dotenv import load_dotenv
import yagmail

load_dotenv()

logging.info(f"Current working directory: {os.getcwd()}")

class Mail:
    def __init__(self):
        self.pword = os.getenv('PWORD')
        self.mail_no_reply = os.getenv('MAIL_NR')
        self.mail_dest = os.getenv('MAIL_DEST')
        logging.info(f"Email destination: {self.mail_dest}")

        # SMTP server details
        self.smtp_server ='mail.datacolab.pt'
        self.smtp_port = 465

    def send_email(self, subject, body) -> str:
        """
        Send notification mail
        """
        try:
            with yagmail.SMTP(user=self.mail_no_reply, 
                            password=self.pword, 
                            host=self.smtp_server, 
                            port=self.smtp_port) as yag:
                yag.send(self.mail_dest.split(','), subject, body)
            return subject

        except Exception as e:
            raise ValueError(f"Error sending email: {e}")

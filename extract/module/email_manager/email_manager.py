import logging
import boto3
import os
# from botocore.exceptions import AlreadyExistsException
from .ses_identities import SesIdentity
from .ses_templates import SesTemplate
from .ses_mail_sender import SesMailSender, SesDestination


logger = logging.getLogger(__name__)


class EmailManager:
    """
    Sends email to recipient using Amazon SES
    """

    def __init__(self,
                 sender: str = "ses.genshinsubredditanalytics@gmail.com",
                 reciever: str = "malifputrayasa@gmail.com"):
        self.ses_client = boto3.client('ses')
        self.sender_addr = sender
        self.recver_addr = reciever
        self.ses_template = SesTemplate(self.ses_client)
        # <-- absolute dir the script is in
        self.script_dir = os.path.dirname(__file__)
        self.email_templates = {
            "GSA-Start": (
                "Extraction Starting - Genshin Subreddit Analytics",
                "Start scraping r/{{subreddit}}",
                self.open_file("templates/start.html")
            ),
            "GSA-Error": (
                "Extraction Error - Genshin Subreddit Analytics",
                "Error scraping r/{{subreddit}}",
                self.open_file("templates/error.html")
            ),
            "GSA-Complete": (
                "Extraction Completed - Genshin Subreddit Analytics",
                "Completed scraping r/{{subreddit}}",
                self.open_file("templates/complete.html")
            )
        }

    def open_file(self, rel_path):
        abs_file_path = os.path.join(self.script_dir, rel_path)
        return open(abs_file_path, "r").read()

    def create_template(self, template_name):
        self.ses_template.create_template(
            template_name,
            *self.email_templates[template_name]
        )

    def update_template(self, template_name):
        self.ses_template.update_template(
            template_name,
            *self.email_templates[template_name]
        )

    def send(self,
             template_name: str,
             template_data: dict):

        template_list = [template["Name"]
                         for template in self.ses_template.list_templates()]

        if template_name not in template_list:
            self.create_template(template_name)
        else:
            self.update_template(template_name)

        self.ses_template.get_template(template_name)
        self.__send(
            template_name,
            template_data
        )

    def __send(self,
               template_name: str,
               template_data: dict):
        # Send msg to recipient
        logging.basicConfig(level=logging.INFO,
                            format='%(levelname)s: %(message)s')

        ses_identity = SesIdentity(self.ses_client)
        ses_mail_sender = SesMailSender(self.ses_client)
        status = ses_identity.get_identity_status(self.sender_addr)
        verified = status == 'Success'

        if verified and self.ses_template.verify_tags(template_data):
            ses_mail_sender.send_templated_email(
                self.sender_addr,
                SesDestination([self.recver_addr]),
                template_name,
                template_data
            )


if __name__ == "__main__":
    EM = EmailManager()
    EM.send("GSA-Start", {"subreddit": "niloumains",
            "start_time": "June 26th, 2003", "end_time": "March 13th, 2023"})
    EM.send("GSA-Complete", {"subreddit": "niloumains",
            "start_time": "June 26th, 2003", "end_time": "March 13th, 2023"})
    EM.send("GSA-Error", {"subreddit": "niloumains",
                          "start_time": "June 26th, 2003",
                          "end_time": "March 13th, 2023",
                          "error_trace": "ET"})

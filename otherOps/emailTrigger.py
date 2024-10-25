MODULE_PATH="otherOps.email"

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from otherOps.exceptions import format_exception_email

class Email:
    def __init__(self,host:str,user:str,pswd:str,port:int) -> None:
        self.host=host
        self.user=user
        self.pswd=pswd
        self.port=port

    def send(self,subject:str,body:str,sender:str,reciever:str):
        #Setup the MIME
        mail = MIMEMultipart()
        mail['From'] = sender
        mail['To'] = reciever
        mail['Subject'] = subject
        # Attach the email body in html if dont want to attach html we can use plain
        mail.attach(MIMEText(body, 'html'))

        # If File attachment is required

        # attach_file_name = 'TP_python_prev.pdf'
        # attach_file = open(attach_file_name, 'rb') # Open the file as binary mode
        # payload = MIMEBase('application', 'octate-stream')
        # payload.set_payload((attach_file).read())
        # encoders.encode_base64(payload) #encode the attachment
        # #add payload header with filename
        # payload.add_header('Content-Decomposition', 'attachment', filename=attach_file_name)
        # message.attach(payload)

        #Create SMTP session for sending the mail
        session = smtplib.SMTP(self.host,self.port) #use gmail with port
        session.starttls() #enable security
        session.login(self.user, self.pswd) #login with mail_id and password
        text = mail.as_string()
        session.sendmail(sender, reciever,text)
        session.quit()
    
    @staticmethod
    def template(client:str,middleware:str,jobname:str,error_message:str):
        # <!DOCTYPE html>
        html_text = f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Document</title>
        </head>
        <body>
            <h1 style="color:rgb(66, 2, 176);font-size:40px;font-family:Arial, Helvetica, sans-serif"><b>Error</b></h1>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Client Name:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{client}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Middlware Name:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{middleware}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Job Name:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{jobname}</p>
            <hr>
            <h2 style="color: black;font-size: 20px;font-family:Arial, Helvetica, sans-serif"><b>Error Message:</b></h2>
            <p style="color: black;font-size: 15px;font-family:Arial, Helvetica, sans-serif">{error_message}</p>
            <hr>
        </body>
        </html>
        """
        return html_text
    
def trigger_email(**kwargs):
    """
    Required Params:\n
    - host\n
    - user\n
    - pswd\n
    - port\n
    - subject\n
    - sender\n
    - reciever\n
    - client\n
    - middleware\n
    - jobname\n
    - exception
    """
    # Fething param values
    host=kwargs.pop('host')
    user=kwargs.pop('user')
    pswd=kwargs.pop('pswd')
    port=kwargs.pop('port')
    subject=kwargs.pop('subject')
    sender=kwargs.pop('sender')
    reciever=kwargs.pop('reciever')
    client=kwargs.pop('client')
    middleware=kwargs.pop('middleware')
    jobname=kwargs.pop('jobname')
    exception:Exception=kwargs.pop('exception')
    error_message = format_exception_email(exception)

    #Trigger email
    for r_email in reciever:
        emailJob = Email(host,user,pswd,port)
        body = emailJob.template(client,middleware,jobname,error_message)
        emailJob.send(subject,body,sender,r_email)
        print('Mail Sent')
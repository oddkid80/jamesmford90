import smtplib

user = ''
pword = ''
#must enable app password on google

try:
    server = smtplib.SMTP_SSL('smtp.gmail.com',465)
    server.ehlo()
    server.login(user,pword)
    server.sendmail('','','testing')
except Exception as ex:
    print(ex)

from datetime import datetime
import email
import getpass
import imaplib
import json
import os

import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

# other functions for emails

# def get_emails(result_bytes):
#     msgs = []
#     for num in result_bytes[0].split():
#         typ, data = con.fetch(num, '(RFC822)')
#         msgs.append(data)
#     return msgs

# def get_body(msg):
#     if msg.is_multipart():
#         return get_body(msg.get_payload(0))
#     else:
#         return msg.get_payload(None,True)

# def get_attachments(msg):
#     for part in msg.walk():
#         if part.get_content_maintype() == 'multipart':
#             continue
#         if part.get('Content-Disposition') is None:
#             continue
#         filename = part.get_filename()
#         if bool(filename):
#             filepath = os.path.join(attach_dir, filename)
#             with open(filepath, 'wb') as filesave:
#                 filesave.write(part.get_payload(decode=True))            


# search for emails meeting parameters
def search(key, value, con):
    result, data = con.search(None, key, '"{}"'.format(value))
    return data


# get first attachment if spreadsheet
def get_attachment(email_list):
    success = False
    for each in email_list:
        if success is False:
            print(each)
            result, data = con.fetch(each,'(RFC822)')
            raw = email.message_from_bytes(data[0][1])
            for part in raw.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                filename = part.get_filename()
                if bool(filename):
                    file_ext = os.path.splitext(filename)[-1].lower()
                    if file_ext == '.xlsx' or file_ext == '.xls' or file_ext == '.csv':
                        filepath = os.path.join(attach_dir, f'Latest BOH{file_ext}')
                        with open(filepath, 'wb') as filesave:
                            filesave.write(part.get_payload(decode=True))
                            success = True
                            return filepath


# time checkers
start_time = datetime.now()
time_check = datetime.now()

# outlook credentials
outlook_user = 'el0015@fusheng.com'
outlook_pass = getpass.getpass(prompt='Outlook Password: ')
imap_url = 'imap-mail.outlook.com'
attach_dir = ''

# sfdc credentials
sfdc_user = 'bko@fs-elliott.com'
sfdc_pass = getpass.getpass(prompt='Salesforce Password: ')
security_token = 'H225tAUMxPYmYEf7LSZr7158Q'

# log into outlook
con = imaplib.IMAP4_SSL(imap_url)
con.login(outlook_user, outlook_pass)

# log into sfdc
session_id, instance = SalesforceLogin(sfdc_user, sfdc_pass, security_token=security_token)
sf = Salesforce(instance=instance, session_id=session_id)

# remove passwords
outlook_pass = 'hi'
sfdc_pass = 'hi'

# get and save file
inbox = con.select('INBOX')
email_list = search('SUBJECT', 'Salesforce Data to Import', con)
email_list_ordered = email_list[0].split()
email_list_ordered.reverse()
new_file = get_attachment(email_list_ordered)

# convert file to use for data load
csv_df = pd.read_excel(new_file)
csv_df.rename(
    columns={
        'PartNumber': 'Part_Number_External_ID__c',
        'Stock Availability (30 Day Projection)': 'Availability_30_Day_Projection__c'
    },
    inplace=True
)
csv_tuple = csv_df.itertuples(index=False)

# initalize for Bulk API Limits
bulk_data = []
count_records = 0
count_chars = 0
count_rows = 0

# bulk upsert
for each in csv_tuple:
    if (datetime.now() - time_check).seconds > 5:
        print(f'{count_rows} rows have been processed. {round((datetime.now() - start_time).seconds/60, 2)} minutes have passed')
        time_check = datetime.now()

    # Max of 10,000 records and 10,000,000 characters per Salesforce Bulk API Limits
    if count_records >= 9000 or count_chars >= 9000000:
        sf.bulk.Product2.upsert(bulk_data, 'Part_Number_External_ID__c')
        bulk_data = []
        count_records = 0
        count_chars = 0
    each_dict = each._asdict()
    bulk_data.append(each_dict)
    count_records += 1
    count_chars += len(json.dumps(each_dict))
    count_rows += 1
sf.bulk.Product2.upsert(bulk_data, 'Part_Number_External_ID__c')

print(f'{count_rows} rows have been processed. {round((datetime.now() - start_time).seconds/60, 2)} minutes have passed')
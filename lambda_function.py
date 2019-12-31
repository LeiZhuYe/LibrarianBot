import os
import json, re, ast,logging

from slackclient import SlackClient
from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
from datetime import date
from urllib.parse import parse_qsl

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

SLACK_BOT_OAUTH = os.environ['SLACK_BOT_OAUTH']

SC = SlackClient(token=SLACK_BOT_OAUTH)
RESPONSE_TEXT = ""

def lambda_handler(event, context):
    logger.debug('lambda event:')
    logger.debug(json.dumps(event))
    
    body = {'message': 'OK'}
    resp = {'statusCode': 200}
    
    if event.get('headers').get('Content-Type') == 'application/json' and event.get('body') and event.get('resource').startswith('/HadoopIssueBotDemo'):
        data = json.loads(event['body'])
        logger.debug('lambda event body: ')
        logger.debug(json.dumps(data))
        
        if data.get('challenge'):
            body['challenge'] = data['challenge']
        elif data.get('event'):
            logger.info('slack event: ')
            logger.info(json.dumps(data['event']))
            slackevents(data['event'])
    elif event.get('headers').get('Content-Type') == 'application/x-www-form-urlencoded' and event.get('body') and event.get('resource') == '/HadoopIssueBotDemo':
        payload = json.loads(parse_qsl(event['body'])[0][1])
        logger.debug('Interactive Message: ')
        logger.debug(json.dumps(payload))
        
        if payload.get('type') == 'interactive_message':
            body = handle_interactive_msg(payload)
        elif payload.get('type') == 'dialog_submission':
            body = handle_dlg_submission(payload)
        
    
    resp['body'] = json.dumps(body)
    logger.debug(resp)
    return resp

def slackevents(message):
    if message and message.get('subtype') is None: 
        channel = message.get('channel')
        text = message.get('text').lower()
        event_ts = message.get('event_ts')
        
        logger.info('text: {}'.format(text))
        
        if ('report' in text and 'new' in text) or (('add' in text or 'insert' in text) and 'issue' in text):
            SC.api_call("chat.postMessage",channel=channel,text="",attachments=ask_new(event_ts))
        else:
            if text.startswith('<@'):
                text = text[13:]
            sql = issue_status_sql(text)
            json_issueNo = query_with_fetchall(sql)
        
            if json_issueNo is None:
                attachmentMsg = ask_new(event_ts)
            else:
                attachmentMsg = ask_update(event_ts, json_issueNo)

            SC.api_call("chat.postMessage",channel=channel,text=RESPONSE_TEXT,attachments=attachmentMsg)
            
def issue_status_sql(searchTxt):
    logger.debug('issue_status_sql(searchTxt): {}'.format(searchTxt))
    sql="SELECT * FROM issue_hadoop WHERE MATCH(DESCRIPTION_OF_THE_ISSUE,IMPACT,RECOMMENDED_CORRECTIVE_ACTION,ACTIONS_TAKEN) AGAINST('{}')".format(searchTxt)
    if 'status' in searchTxt and 'issue' in searchTxt:
        issueNo = re.findall(r'\d+', searchTxt)
        length = len(issueNo) - 1
        selectSql = "SELECT * FROM issue_hadoop WHERE "
        if length >= 0:
            sql = selectSql + "ISSUE_NO IN ("
            i = 0
            while i < length:
                sql += "{}, ".format(int(issueNo[i]))
                i += 1
            sql += "{})".format(int(issueNo[length]))
        elif 'open' in searchTxt:
            sql = selectSql + "RESOLUTION_STATUS = 'Open'"
        elif 'in progress' in searchTxt:
            sql = selectSql + "RESOLUTION_STATUS = 'In Progress'"
        elif 'close' in searchTxt:
            sql = selectSql + "RESOLUTION_STATUS = 'Closed'"
    return sql


def handle_interactive_msg(pl):
    logger.debug('Handling Interactive Message for: {}'.format(pl.get('callback_id')))
    user = ' '.join(map(str.capitalize, pl['user']['name'].split('.')))
    trigger_id = pl['trigger_id']
    
    if pl.get('callback_id', ' ').startswith('ask_new_issue'):
        body = ansNewIssuePrompt(pl)
        sendNewIssueDlg(body.pop('trigger_id'), pl.get('message_ts'))
    elif pl.get('callback_id', ' ').startswith('ask_update_issue'):
        # Check to see what the user's selection was and update the message
        selection = pl["actions"][0]["selected_options"][0]["value"]
        if selection == "ask_new":
            body = ansNewIssuePrompt(pl)
            sendNewIssueDlg(trigger_id, pl["message_ts"])
        else:
            body = {'text': '{} {}.'.format(user, selection), 'replace_original': True}
            sendUpdateDlg(trigger_id, pl["message_ts"], selection)
    return body

def handle_dlg_submission(pl):
    logger.debug('Handling Dialog Submissiong for: {}'.format(pl.get('callback_id')))
    if pl.get('callback_id', ' ').startswith('new_issue_form'):
        body = procNewIssueDlg(pl.get('submission'))
        if body == {}:
            updateMessage(pl.get('channel', dict()).get('id'), pl.get('callback_id').split('.', 1)[1], RESPONSE_TEXT)
    elif pl.get('callback_id', ' ').startswith('update_issue_form'):
        body = procUpdateIssueDlg(pl.get('state'), pl.get('submission'))
        if body == {}:
            updateMessage(pl.get('channel', dict()).get('id'), pl.get('callback_id').split('.', 1)[1], RESPONSE_TEXT)
    return body

def updateMessage(channel, ts, text, attachments=[]):
    global SC
    logger.debug('Update Message: ')
    logger.debug('Channel: {}, Msg_ts: {}, Text: {}'.format(channel, ts, text))

    SC.api_call('chat.update', channel=channel, ts=ts, text=text, attachments=attachments) 

def ansNewIssuePrompt(pl):
    acts = pl['actions']
    user = ' '.join(map(str.capitalize, pl['user']['name'].split('.')))
    trigger = pl['trigger_id']
    channel = pl['channel']['id']
    
    return {
        'text': '{}, thank you for trying to report a new issue.'.format(user),
        'replace_original': True,
        'trigger_id': trigger,
    }
    
def procNewIssueDlg(flds):
    global RESPONSE_TEXT
    logger.debug('Attempting to add new issue...')
    errors = [] #Should be a list of {name: <field_name>, error: <description>}
    logger.debug(flds)
    # description is unique key
    query = "INSERT IGNORE INTO issue_hadoop(DESCRIPTION_OF_THE_ISSUE,impact,recommended_corrective_action,action_owner,TICKET,ACTIONS_TAKEN,resolution_status,resolution_date,reporter,report_date) " \
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    args = (flds['issue_description'],flds['imapct'],flds['recommend'],flds['owner'],flds['ticket'],flds['actionTaken'],flds['status'],flds['resolutionDate'],flds['reporter'],flds['reportDate'])
 
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)
 
        cursor = conn.cursor()
        cursor.execute(query, args)
 
        if cursor.lastrowid:
            RESPONSE_TEXT = "The Issue Has Been Submitted! The New Issue No Is: {}".format(cursor.lastrowid)
        else:
            RESPONSE_TEXT = "last insert id not found"
 
        conn.commit()
    except Error as e:
        RESPONSE_TEXT = "Failed adding the issue {}".format(e)
 
    finally:
        cursor.close()
        conn.close()
    logger.debug("RESPONSE_TEXT = {}".format(RESPONSE_TEXT))
    if len(errors) == 0:
        return {}
    else:
        return {
            'errors': errors
        }

def procUpdateIssueDlg(ISSUENo, flds):
    global RESPONSE_TEXT
    logger.debug('Attempting to update ISSUE No. {}'.format(ISSUENo))
    errors = [] #Should be a list of {name: <field_name>, error: <description>}
    logger.debug(flds)
    # description is unique key
    query = "UPDATE issue_hadoop SET DESCRIPTION_OF_THE_ISSUE = %s, " \
            "IMPACT = %s, recommended_corrective_action = %s, action_owner = %s, " \
            "TICKET = %s, ACTIONS_TAKEN = %s, resolution_status = %s, resolution_date=%s, " \
            "reporter = %s, report_date = %s WHERE ISSUE_No = %s"
    args = (flds['issue_description'],flds['imapct'],flds['recommend'],flds['owner'],flds['ticket'],flds['actionTaken'],flds['status'],flds['resolutionDate'],flds['reporter'],flds['reportDate'], int(ISSUENo))
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)
 
        cursor = conn.cursor()
        cursor.execute(query, args)
        conn.commit()
        if cursor.rowcount == 1:
            RESPONSE_TEXT = "ISSUE# {} Has Been Updated!".format(ISSUENo)
        else:
            RESPONSE_TEXT = "ISSUE# {} Has Not Been Updated!".format(ISSUENo)
    except Error as e:
        RESPONSE_TEXT = "Failed updating the issue {}".format(e)
 
    finally:
        cursor.close()
        conn.close()
       
    if len(errors) == 0:
        return {}
    else:
        return {
            'errors': errors
        }

def query_with_fetchall(sql):
    global RESPONSE_TEXT
    logger.debug('query_with_fetchall sql: {}'.format(sql))
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        # returns a list of tuples describing the columns in a result set. 
        # Each tuple in the list contains values as follows:(column_name,type,...):
        desc = cursor.description
        columns = len(desc)
        counts = cursor.rowcount
        logger.debug('Total Row(s): {}'.format(counts))
        RESPONSE_TEXT = "Matched Issue(s) Found: {}\n".format(counts)
        if counts >= 1:
            issueNo_options = []
            issueNo_options.append({
                "text": "Report a New Issue", 
                "value": "ask_new"
            })
            for row in rows:
                RESPONSE_TEXT += "---------------------------ISSUE No. {}---------------------------\n".format(row[0])
                jsonStr = "'{}': '{}', ".format(desc[0][0], row[0])
                
                for col in range(columns):
                    if col >= 1 and row[col] is not None:
                        # column_name
                        RESPONSE_TEXT += "{}: ".format(desc[col][0].upper())
                        # type = 253 (VAR_STRING)
                        if (desc[col][1] == 253):
                            jsonStr += "'{}': '{}', ".format(desc[col][0], row[col].replace('\'', ''))
                        else:
                            jsonStr += "'{}': '{}', ".format(desc[col][0], row[col])
                        RESPONSE_TEXT += "{}\n".format(row[col])

                jsonStr = "{" + jsonStr[:-2] + "}"
                
                issueNo_options.append({
                    "text": "Update ISSUE No. {}".format(row[0]), 
                    "value": str(jsonStr)
                })        
            return issueNo_options
    except Error as e:
        RESPONSE_TEXT += "Failed retrieving a record {}\n".format(e)
 
    finally:
        cursor.close()
        conn.close()
    return None

def sendNewIssueDlg(trigger_id, msg_ts):
    global SC
    logger.info('New Issue Dialog Trigger ID: {}'.format(trigger_id))
    dlg = {
        "callback_id": "new_issue_form.{}".format(msg_ts),
        "title": "Report a New Issue",
        "submit_label": "Submit",
        "notify_on_cancel": False,
        "state": "insert_record",
        "elements": [
            {
                "type": "textarea",
                "label": "Description Of The Issue:",
                "name": "issue_description",
                "max_length": 600
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Impact:",
                "name": "imapct",
                "max_length": 955
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Recommended Corrective Action:",
                "name": "recommend",
                "max_length": 955
            },
            {
                "type": "text",
                "optional": True,
                "label": "Action Owner:",
                "name": "owner"                
            },
            {
                "type": "text",
                "optional": True,
                "label": "Ticket #:",
                "name": "ticket",
                "max_length": 50
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Action Taken:",
                "name": "actionTaken",
                "max_length": 955
                
            },
            {
                "type": "select",
                "optional": False,
                "label": "Resolution Status:",
                "name": "status",
                "options": [
                    {
                        "label": "Open",
                        "value": "Open"
                    },
                    {
                        "label": "In Progress",
                        "value": "In Progress"
                    },
                    {
                        "label": "Closed",
                        "value": "Closed"
                    }
                ]
            },
            {
                "type": "text",
                "optional": True,
                "label": "Resolution Date:",
                "name": "resolutionDate",
                "max_length": 10,
                "hint": "YYYY-MM-DD",
                "placeholder": str(date.today())
            },
            {
                "type": "text",
                "optional": True,
                "label": "Reporter:",
                "name": "reporter",
                "max_length": 100,
           },
           {
                "type": "text",
                "optional": True,
                "label": "Report Date:",
                "name": "reportDate",
                "max_length": 10,
                "hint": "YYYY-MM-DD",
                "value": str(date.today())
            }
        ]
    }
    logger.debug('Dialog: ')
    logger.debug(json.dumps(dlg))
    response = SC.api_call('dialog.open', trigger_id=trigger_id, dialog=dlg)
    logger.info('Dialog Response: {}'.format(response))

def sendUpdateDlg(trigger_id, msg_ts, selected_option):
    global SC
    logger.info('Update Dialog Trigger ID: {}'.format(trigger_id))
    json_data = ast.literal_eval(selected_option)
    value = json.loads(json.dumps(json_data))
    dlg = {
        "callback_id": "update_issue_form.{}".format(msg_ts),
        "title": "Update Issue No. {}".format(value['ISSUE_No']),
        "submit_label": "Update",
        "notify_on_cancel": False,
        "state": value['ISSUE_No'],
        "elements": [
            {
                "type": "textarea",
                "label": "Description Of The Issue:",
                "name": "issue_description",
                "max_length": 600,
                "value": value['DESCRIPTION_OF_THE_ISSUE']
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Impact:",
                "name": "imapct",
                "max_length": 955,
                "value": "" if 'IMPACT' not in value else value['IMPACT']
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Recommended Corrective Action:",
                "name": "recommend",
                "max_length": 955,
                "value": "" if 'RECOMMENDED_CORRECTIVE_ACTION' not in value else value['RECOMMENDED_CORRECTIVE_ACTION']
            },
            {
                "type": "text",
                "optional": True,
                "label": "Action Owner:",
                "name": "owner",
                "value": "" if 'ACTION_OWNER' not in value else value['ACTION_OWNER']
            },
            {
                "type": "text",
                "optional": True,
                "label": "Ticket #:",
                "name": "ticket",
                "max_length": 50,
                "value": "" if 'TICKET' not in value else value['TICKET']
            },
            {
                "type": "textarea",
                "optional": True,
                "label": "Action Taken:",
                "name": "actionTaken",
                "max_length": 955,
                "value": "" if 'ACTIONS_TAKEN' not in value else value['ACTIONS_TAKEN']
            },
            {
                "type": "select",
                "optional": False,
                "label": "Resolution Status:",
                "name": "status",
                "value": "" if 'RESOLUTION_STATUS' not in value else value['RESOLUTION_STATUS'],
                "options": [
                    {
                        "label": "Open",
                        "value": "Open"
                    },
                    {
                        "label": "In Progress",
                        "value": "In Progress"
                    },
                    {
                        "label": "Closed",
                        "value": "Closed"
                    }
                ]
            },
            {
                "type": "text",
                "optional": True,
                "label": "Resolution Date:",
                "name": "resolutionDate",
                "max_length": 10,
                "hint": "YYYY-MM-DD",
                "placeholder": str(date.today()),
                "value": "" if 'RESOLUTION_DATE' not in value else value['RESOLUTION_DATE']
            },
            {
                "type": "text",
                "optional": True,
                "label": "Reporter:",
                "name": "reporter",
                "max_length": 100,
                "value": "" if 'REPORTER' not in value else value['REPORTER']
           },
           {
                "type": "text",
                "optional": True,
                "label": "Report Date:",
                "name": "reportDate",
                "max_length": 10,
                "hint": "YYYY-MM-DD",
                "value": "" if 'REPORT_DATE' not in value else value['REPORT_DATE']
            }
        ]
    }
    response = SC.api_call('dialog.open', trigger_id=trigger_id, dialog=dlg)
    logger.info('Dialog Response: {}'.format(response))

def ask_new(event_ts):
    newMsg = [{
        "text": "",
        "callback_id": "ask_new_issue.{}".format(event_ts),
        "color": "good",
        "attachment_type": "default",
        "actions": [
        {
            "name": "ask",
            "text": "Report a New Issue",
            "type": "button",
            "value": "ask_new"
        }]
    }]
    return newMsg

def ask_update(event_ts, options_menu):
    updMsg = [{
        "text": "Report a new issue or select an ISSUE No. to update:",
        "callback_id": "ask_update_issue.{}".format(event_ts),
        "color": "#3AA3E3",
        "attachment_type": "default",
        "actions": [
            {
                "name": "issue_list",
                "text": "Select ...",
                "type": "select",
                "options": options_menu
            }
        ]
    }]
    return updMsg
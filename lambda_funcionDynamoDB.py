import os
import re
import json
import boto3
import logging
from urllib.parse import parse_qsl

from slackclient import SlackClient
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

#Setting up the logging capabilities and Slack client initialization.  This code should not need to be changed from application to application
logger = logging.getLogger() 
logger.setLevel(logging.DEBUG)
SLACK_BOT_OAUTH = os.environ['SLACK_BOT_OAUTH'] 
SC = SlackClient(token=SLACK_BOT_OAUTH)
##End of slack client initialization

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
        
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Librarian')

#Handling the challenge and response mechanism required by Slack and these bots.  This code should not need to be changed from application to application
def lambda_handler(event, context): 
    logger.debug('event:') 
    logger.debug(json.dumps(event)) 
    
    body = { 'message': 'OK' } 
    resp = { 'statusCode': 200 } 
    if event.get('headers').get('Content-Type') == 'application/json' and event.get('body') and event.get('resource').startswith('/testChatBot'): 
        data = json.loads(event['body']) 
        logger.debug('lambda event body:') 
        logger.debug(json.dumps(data)) 
        
        if data.get('challenge'): 
            body['challenge'] = data['challenge']
           ##End of the slack challenge initialization 
        elif data.get('event'): 
            logger.info('slack event: ')
            logger.info(json.dumps(data['event']))
            slackevents(data['event']) 
            
    resp['body'] = json.dumps(body) 
    logger.info(json.dumps(resp)) 
    return resp 

#The main section of code that accepts the user message, does X,y, and Z with it, and responds back to the Slack channel accordingly
def slackevents(message):
    if message and message.get('subtype') is None: 
        #Extracting the channel from which the user message originated. Used in the postMessage response to direct the Bot message back to the same channel
        #Just added one space to line 44 (was line 43) based on the most recent logs
        channel = message.get('channel')
        
        #Retrieving the user ID of the person who sent the message to the bot
        user = message.get('user')
        
        #Retrieving the text that the user directed to the bot
        userText = message.get('text')
        
        #When starting out user messages to your bot with @<bot_name> followed by the message, "directedTo" pulls the @<bot_name> out of the message
        ##Important when attempting to provide the user's message back to the user (will cause an infinite loop if it remains in the text)
        ###Can also be used in responding to the user if you want the bot to use their name in the response itself (example of this in below comments)
        if userText.startswith('<@'):
            directedTo=userText[2:11]
            userText=userText[13:]
        
        #remove special charaters, keep only letters and spaces
        userText = re.sub(r"[^a-zA-Z]+", ' ', userText).strip().lower()
        
        #Example of how to output a variable's contents into Cloudwatch;
        logger.info('text: {}'.format(userText))
        
        item = getDocument(userText)
        if item == 'null':
            keywords = getKeyWordList()
            # using filter() + endswith()
            # Checking for string match suffix
            searchKeywords = filter(userText.endswith, keywords)
            for keyword in searchKeywords:
                row = getDocument(keyword)
                if row != 'null':
                    postMessage(channel=channel, text=row)
                    return None
            tokens = [token for token in userText.split(" ") if token !=""]
            responseText=""
            documentName=[]
            for token in tokens:
                for item in keywords:
                    if token in item:
                        row=getDocument(item)
                        if row['DocumentName'] not in documentName:
                            documentName.append(row['DocumentName'])
                            responseText += row['LocationResponse'] + "\n \n"
            if responseText !="":
                postMessage(channel=channel, text=responseText)
            else:
                postMessage(channel=channel, text="{} not found.  Please pick up words from following list: {}".format(userText, keywords))
        else:
            postMessage(channel=channel, text="Found '{}' in '{}'.  {}".format(userText, item['DocumentName'], item['LocationResponse']))

def postMessage(channel, text, attachments=None):
    global SC
    SC.api_call('chat.postMessage', channel=channel, text=text, attachments=attachments)
    
def getKeyWordList():
    global table
    rows = table.scan(ExpressionAttributeNames={'#n': 'KeyWord'}, ProjectionExpression='#n')
    logger.debug(rows)
    items = rows.get('Items')
    keyWords = [ key['KeyWord'] for key in items ]
    logger.debug(keyWords)
    
    return keyWords

def getDocument(keyword):
    global table
    try:
        response = table.get_item(Key={'KeyWord': keyword.lower()})
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        return response.get('Item', 'null')
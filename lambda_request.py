from datetime import datetime, timezone
from datetime import timedelta
import requests
import json
import boto3


def lambda_handler(event, context):

    # Getting info about date and hour
    now = datetime.now()
    diference = timedelta(hours=-3)
    timezoneVariable = timezone(diference)
    today = now.astimezone(timezoneVariable)
    today = today.strftime(
        "%Y-%m-%d %H00")
    todayDate = today.split()[0]
    todayHour = today.split()[1]

    # Url for request
    baseUrl = f"https://apitempo.inmet.gov.br/estacao/dados/{todayDate}/{todayHour}"
    # Request
    result = requests.get(baseUrl)

    # Transforming result in json
    dataJson = result.json()
    # filter json
    jsonFiltered = [x for x in dataJson if x['UF'] == 'PE']

    dataToSend = []
    for info in jsonFiltered:
        thisDict = {
            "Data": json.dumps({
                "DC_NOME": info["DC_NOME"],
                "TEM_INS": info["TEM_INS"],
                "UMD_INS": info["UMD_INS"]
            }),
            "PartitionKey": info["DC_NOME"]
        }
        dataToSend.append(thisDict)

    client = boto3.client("kinesis", "us-east-1")
    client.put_records(
        Records=dataToSend,
        StreamName='firstDataStream')
    return {
        'statusCode': 200,
        'body': json.dumps('Send to Kinesis Stream!')
    }

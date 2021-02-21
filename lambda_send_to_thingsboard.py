import json
import base64
import requests

# Place acess token
dic = {}
dic["RECIFE"] = ""
dic["PETROLINA"] = ""
dic["ARCO VERDE"] = ""
dic["GARANHUNS"] = ""
dic["SURUBIM"] = ""
dic["CABROBÓ"] = ""
dic["CARUARU"] = ""
dic["IBIMIRIM"] = ""
dic["SERRA TALHADA"] = ""
dic["FLORESTA"] = ""
dic["PALMARES"] = ""
dic["OURICURI"] = ""
dic["SALGUEIRO"] = ""


def lambda_handler(event, context):

    records = event.get('Records')

    listToSend = []

    for record in records:

        data = record['kinesis']['data']
        msg = base64.b64decode(data)
        message = json.loads(msg)

        if(message['HEAT_INDEX'] == None):
            message['HEAT_INDEX'] = 0
            message['alert_level'] = 'No information'
        else:
            heatIndex = (message['HEAT_INDEX'] - 32) / 1.8
            if(heatIndex <= 27.0):
                message['alert_level'] = 'Normal'
            elif(27.1 <= heatIndex <= 32.0):
                message['alert_level'] = 'Caution'
            elif(32.1 <= heatIndex <= 41.0):
                message['alert_level'] = 'Extreme caution'
            elif(41.1 <= heatIndex <= 54.0):
                message['alert_level'] = 'Danger'
            elif(heatIndex >= 54.0):
                message['alert_level'] = 'Extreme danger'
            message['HEAT_INDEX'] = float(format(heatIndex, '.2f'))
        listToSend.append(message)
    print(listToSend)
    try:
        for station in listToSend:
            headers = {'content-type': 'application/json'}
            url = f'https://demo.thingsboard.io/api/v1/{dic[station["DC_NOME"]]}/telemetry'
            print(url)
            r = requests.post(url, data=json.dumps(station), headers=headers)
            print(r)
    except:
        print('error')
    return {
        'statusCode': 200,
        'body': json.dumps('Send to thingsboard!')
    }

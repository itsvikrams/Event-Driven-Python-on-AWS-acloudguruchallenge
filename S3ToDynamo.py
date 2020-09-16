import boto3
import csv

def lambda_handler(event, context):
    region='us-east-1'
    recList=[]
    try:            
        s3=boto3.client('s3')            
        dyndb = boto3.client('dynamodb', region_name=region)
        confile= s3.get_object(Bucket='pythonetlacloudguruchallenge', Key='us.csv')
        recList = confile['Body'].read().split('\n')
        firstrecord=True
        csv_reader = csv.reader(recList, delimiter=',', quotechar='"')
        print('Print Succeeded')
        for row in csv_reader:
            if (firstrecord):
                firstrecord=False
                continue
            date = row[0]
            cases = row[1].replace(',','').replace('$','') if row[1] else '-'
            deaths = row[2].replace(',','').replace('$','') if row[2] else 0
            response = dyndb.put_item(
                TableName='covidusdata',
                Item={
                'date' : {'S':str(date)},
                'cases': {'N':str(cases)},
                'deaths': {'N':str(deaths)}
                }
            )
        print('Put succeeded:')
    except Exception, e:
        print ('stopped your instances: ' + str(e))
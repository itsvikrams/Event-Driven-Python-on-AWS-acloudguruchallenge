import pandas as pd
import io
import requests
import boto3


def get_ny_data(min_date=None):
    try:
        # TODO - remove the responsibility of where the data present
        url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
        content = requests.get(url).content
        ny_data = pd.read_csv(io.StringIO(content.decode('utf-8')))
        ny_data['date'] = ny_data['date'].astype('datetime64')
        if min_date is not None:
            # filter date
            final_data = ny_data.loc[(ny_data['date'] >= min_date)]
        else:
            final_data = ny_data
        return final_data
    except Exception as ex:
        print('error while getting the data from ny times'+ex)


def get_johns_hopkins_dataset(min_date=None):
    try:
        # TODO - remove the responsibility of where the data present
        url = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'
        content = requests.get(url).content
        jh_data = pd.read_csv(io.StringIO(content.decode('utf-8')))
        jh_data['Date'] = jh_data['Date'].astype('datetime64')
        # filter only us data
        us_only = jh_data.loc[jh_data["Country/Region"] == "US"]
        if min_date is not None:
            # filter date
            final_data = us_only.loc[(us_only['Date'] >= min_date)]
        else:
            final_data = us_only
        return final_data
    except Exception as ex:
        print('error while getting the data from johns_hopkins_dataset'+ex)


def data_manipulation(min_date=None):
    ny_data = get_ny_data(min_date)
    jh_data = get_johns_hopkins_dataset(min_date)
    final_data = pd.merge(left=ny_data, right=jh_data, how='inner', left_on='date', right_on='Date')[['date', 'cases', 'deaths', 'Recovered']]

    dynamo_data = get_data_from_dynamo()
    try:
        dynamo_data['date'] = dynamo_data['date'].astype('datetime64')
    except Exception as ex:
        pass

    print(dynamo_data)
    print(final_data)
    r = remove_old_dates(dynamo_data, final_data)
    put_date_to_dynamo(r)
    # return final_data.loc[final_data - dynamo_data] # if didnt work set index and try final_data.set_index('date', inplace=True)


def remove_old_dates(dynamo_datas, final_data):
    for dynamo_data in dynamo_datas:
        index_names = final_data[(final_data['date'] >= dynamo_datas['date'])].index
        final_data.drop(index_names, inplace=True)
    return final_data


def get_data_from_dynamo():
    dyndb = get_dynamo_client()
    table = dyndb.Table('covidusdata')
    response = table.scan()
    return pd.DataFrame.from_dict(response['Items'])

def put_date_to_dynamo(datas):
    dyndb = boto3.client('dynamodb',
                           # aws_session_token=aws_session_token,
                           aws_access_key_id='AKIATOPFE6QQJGXW72WQ',
                           aws_secret_access_key='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',
                           region_name='us-east-1'
                           )
    for data in datas:
        response = dyndb.put_item(
            TableName='covidusdata',
            Item={
                'date': {'S': str(data['date'])},
                'cases': {'N': str(data['cases'])},
                'deaths': {'N': str(data['deaths'])},
                'Recovered': {'N': str(data['Recovered'])} # data.columns = map(str.lower, data.columns)
            }
        )


def get_dynamo_client():
    return boto3.resource('dynamodb',
                           # aws_session_token=aws_session_token,
                           aws_access_key_id='AKIATOPFE6QQJGXW72WQ',
                           aws_secret_access_key='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',
                           region_name='us-east-1'
                           )


def cleanup_dynamo():
    dyndb = get_dynamo_client()
    table = dyndb.Table('covidusdata')
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'date': each['date']
                }
            )

#cleanup_dynamo()

#print(data_manipulation('2020-09-09'))
print(data_manipulation())




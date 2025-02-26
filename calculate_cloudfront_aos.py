import datetime
import argparse
import botocore
import boto3
import json
import os
import math


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Calculate Amazon CloudFront AOS using Cost Explorer API.')
    parser.add_argument('--month', help='specify month (ignored if start or end date are set)')
    parser.add_argument('--year', help='specify year (ignored if start or end date are set)')
    parser.add_argument('--start-date', help='specify start date in format yyyy-mm-dd')
    parser.add_argument('--end-date', help='specify end date in format yyyy-mm-dd')
    parser.add_argument('--granularity', help='monthly, daily, hourly (only for the last 14 days)')
    parser.add_argument('--output', help='json or csv or text')

    args = parser.parse_args()

    if not('AWS_ACCESS_KEY_ID' in os.environ):
        os.write(2, b'env variable "AWS_ACCESS_KEY_ID" has to be set\n')
    if not('AWS_SECRET_ACCESS_KEY' in os.environ):
        os.write(2, b'env variable "AWS_SECRET_ACCESS_KEY" has to be set\n')
    if not('AWS_SESSION_TOKEN' in os.environ):
        os.write(2, b'env variable "AWS_SESSION_TOKEN" has to be set\n')
    if (
        not('AWS_ACCESS_KEY_ID' in os.environ)
        or not('AWS_SECRET_ACCESS_KEY' in os.environ)
        or not('AWS_SESSION_TOKEN' in os.environ)
    ):
        exit(1)

    if args.start_date and not(args.end_date):
        os.write(2, 'if start-date is set, end-date must also be set')
        exit(1)
    elif not(args.start_date) and args.end_date:
        os.wrire(2, 'if end-date is set, start-date must also be set')
        exit(1)
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        today = datetime.datetime.today()
        month = int(args.month) if args.month else today.month
        year = int(args.year) if args.year else today.year

        first_day_of_month = datetime.datetime(year, month, 1).date()
        current_day_of_month = datetime.datetime(year, month, today.day).date()

        # Cost Explorer API TimePeriod end date is exclusive, adding 1 more day to end date
        end_date = (first_day_of_month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        
        if month == today.month:
            end_date = today.date() + datetime.timedelta(days=1)

        start_date = str(first_day_of_month)
        end_date = str(end_date)

     
    output_format = args.output if args.output else 'text'

    granularity = 'MONTHLY'
    if args.granularity and args.granularity == 'daily':
        granularity = 'DAILY'
    if args.granularity and args.granularity == 'hourly':
        granularity = 'HOURLY'

    # get a session and fetch data
    try:
        session = boto3.session.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                                        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                                        aws_session_token=os.environ['AWS_SESSION_TOKEN'])
        client = session.client('ce', region_name='us-east-1')

        output = client.get_cost_and_usage(TimePeriod={'Start': start_date, 'End': end_date},
                                           Metrics=['USAGE_QUANTITY'], Granularity=granularity,
                                           GroupBy=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}], Filter={'Dimensions':
                                            {'Key': 'SERVICE', 'Values': ['Amazon CloudFront']}})
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AccessDeniedException':
            print(error.response['Error']['Message'])
        else:
            raise error
        exit(1)

    # transform the data
    period_total_data_transfer_in_Gb = []
    period_total_requests = []
    result_json = []
    for t in output['ResultsByTime']:
        period_data_transfer_in_Gb = []
        period_requests = []

        for k in t['Groups']:
            if 'DataTransfer-Out-Bytes' in k['Keys'][0]:
                period_data_transfer_in_Gb.append(float(k['Metrics']['UsageQuantity']['Amount']))
            if '-Requests-Tier' in k['Keys'][0]:
                period_requests.append(float(k['Metrics']['UsageQuantity']['Amount']))

        period_total_data_transfer_in_Gb =  period_total_data_transfer_in_Gb + period_data_transfer_in_Gb 
        period_total_requests = period_total_requests + period_requests

        sum_data_transfer_in_kb = sum(period_data_transfer_in_Gb) * 1048576
        sum_period_requests = sum(period_requests)
        aos = ''
        if sum_period_requests != 0:
            aos = round(sum_data_transfer_in_kb / sum_period_requests, 3)
        else:
            aos = sum_data_transfer_in_kb

        result_json.append({
            'time': t['TimePeriod'],
            'kb': sum_data_transfer_in_kb,
            'requests': math.floor(sum(period_requests)),
            'aos': aos,
         })

    total_data_transfer_in_kb = sum(period_total_data_transfer_in_Gb) * 1048576
    total_requests = sum(period_total_requests)


    # output data as csv, json or text
    try:
        aos = round(total_data_transfer_in_kb / total_requests, 3)
        if output_format == 'json':
            print(json.dumps({
                'message': f'Cloudfront Average Object Size (AOS) for period is: {aos}Kb', 'aos': aos,
                'data':  result_json                  
                }))
        elif output_format == 'csv':
            print(f'Cloudfront Average Object Size (AOS) for period is: {aos}Kb\n\n')
            print('start date;end date;data in kb;requests;average object size;')
            for entry in result_json:
                print(f'{entry['time']['Start']};{entry['time']['End']};{entry['kb']};{entry['requests']};{entry['aos']};')
        else:
            print(f'Cloudfront Average Object Size (AOS) for period is: {aos}Kb')

    except ZeroDivisionError:
        error_message = 'Cost explorer API returned 0 for one of the usage types (data transfer or requests), this is most likely because you run the report at the beginning of the month, please adjust the dates using --month or --year parameters'
        if output_format == 'json':
            print(json.dumps({'message': error_message, 'aos': ''}))
        else:
            print(error_message)

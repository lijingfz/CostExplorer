import datetime
import boto3
import pandas
import numpy as np

def lambda_handler(event, context):
    today = datetime.date.today()
    # start = today.replace(day=1).strftime('%Y-%m-%d')
    # end = today.strftime('%Y-%m-%d')
    start = '2023-01-01'
    end = '2023-02-01'
    print(start, end)
    ce = boto3.client('ce')
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End': end,
        },
        # Granularity='DAILY',
        Granularity='MONTHLY',
        # Filter={
        #     "Not": {"Dimensions": {"Key": "USAGE_TYPE", "Values": ["DataTransfer"]}}
        # },
        Metrics=[
            'UnblendedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
            ,
            # {
            #     'Type': 'DIMENSION',
            #     'Key': 'REGION'
            # }
            # ,
            {
                'Type': 'DIMENSION',
                'Key': 'USAGE_TYPE'
            }
        ]
    )
    return response['ResultsByTime']
    # return response


response = lambda_handler(1, 2)

# print(response)

merged_cost = pandas.DataFrame(
        index=[],
        columns=['Services','UsageType'],
    )

for index, item in enumerate(response):
    normalized_json = pandas.json_normalize(item['Groups'])
    split_keys = pandas.DataFrame(
            normalized_json['Keys'].tolist(),
            columns=['Services','UsageType']
        )
    cost = pandas.concat(
            [split_keys, normalized_json['Metrics.UnblendedCost.Amount']],
            axis=1
        )
    renamed_cost = cost.rename(
            columns={'Metrics.UnblendedCost.Amount': item['TimePeriod']['Start']}
        )
    print(renamed_cost)
    merged_cost = pandas.merge(merged_cost, renamed_cost, on=['Services','UsageType'], how='right')

# merged_cost.to_csv('/Users/lijing/Desktop/我的工作/360/ce-services.csv', index=True)


def lambda_handler_region(event, context):
    today = datetime.date.today()
    # start = today.replace(day=1).strftime('%Y-%m-%d')
    # end = today.strftime('%Y-%m-%d')
    start = '2023-01-01'
    end = '2023-02-01'
    print(start, end)
    ce = boto3.client('ce')
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End': end,
        },
        # Granularity='DAILY',
        Granularity='MONTHLY',
        Metrics=[
            'UnblendedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'REGION'
            }
            ,
            {
                'Type': 'DIMENSION',
                'Key': 'USAGE_TYPE'
            }
        ]
    )
    return response['ResultsByTime']

response_region = lambda_handler_region(1, 2)

merged_cost_region = pandas.DataFrame(
        index=[],
        columns=['Region','UsageType'],
    )

for index, item in enumerate(response_region):
    normalized_json_region = pandas.json_normalize(item['Groups'])
    split_keys_region = pandas.DataFrame(
            normalized_json_region['Keys'].tolist(),
            columns=['Region','UsageType']
        )
    cost_region = pandas.concat(
            [split_keys_region, normalized_json_region['Metrics.UnblendedCost.Amount']],
            axis=1
        )
    renamed_cost_region = cost_region.rename(
            columns={'Metrics.UnblendedCost.Amount': item['TimePeriod']['Start']}
        )
    print(renamed_cost_region)
    merged_cost_region = pandas.merge(merged_cost_region, renamed_cost_region, on=['Region','UsageType'], how='right')

# merged_cost_region.to_csv('/Users/lijing/Desktop/我的工作/360/ce-region.csv', index=True)

MyRegion = pandas.merge(merged_cost,merged_cost_region.drop(['2023-01-01'],axis=1), on=['UsageType'], how='left')


# MyRegion.to_csv('/Users/lijing/Desktop/我的工作/360/add-region.csv', index=True)

MyRegion['Services'] = np.where((MyRegion['UsageType'].str.contains('DataTransfer-Out-Bytes')) |
                                (MyRegion['UsageType'].str.contains('DataTransfer-In-Bytes')) |
                                (MyRegion['UsageType'].str.contains('AWS-In-Bytes')) |
                                (MyRegion['UsageType'].str.contains('AWS-Out-Bytes')) |
                                (MyRegion['UsageType'].str.contains('DataXfer-In')) |
                                (MyRegion['UsageType'].str.contains('DataXfer-Out')) |
                                (MyRegion['UsageType'].str.contains('CloudFront-In-Bytes')) |
                                (MyRegion['UsageType'].str.contains('CloudFront-Out-Bytes')) |
                                (MyRegion['UsageType'].str.contains('DataTransfer-Regional-Bytes'))
                                , 'DataTransfer', MyRegion['Services'])

MyRegion.to_csv('/tmp/add-region.csv', index=True)

# df = MyRegion
#
# bill_pivot=pandas.pivot_table(df, index=['Services'], values=['2023-01-01'], aggfunc=np.sum)
#
# # sorted_df_pivot = bill_pivot.sort_values(by=['2023-01-01'], ascending=False)
#
# print(bill_pivot)

df = pandas.read_csv('/tmp/add-region.csv')
df.head()
bill_pivot=pandas.pivot_table(df, index=['Services'], values=['2023-01-01'], aggfunc=np.sum)
sorted_df_pivot = bill_pivot.sort_values(by=['2023-01-01'], ascending=False)
print(bill_pivot)






# Jingamz@
# 20230211

import datetime
import boto3
import pandas
import numpy as np

def condf(DimCol, UsageType):
    return pandas.DataFrame(
        index=[],
        columns=[DimCol, UsageType],
    )


def getcedetail(aws_client, startdate, enddate, Dimension1, Dimension2):
    start = startdate
    end = enddate
    DimKey1 = Dimension1
    DimKey2 = Dimension2
    ce = aws_client
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End': end,
        },
        Granularity='MONTHLY',
        Metrics=[
            'UnblendedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': DimKey1
            },
            {
                'Type': 'DIMENSION',
                'Key': DimKey2
            }
        ]
    )
    return response['ResultsByTime']


def GenCeReport(QueryOutput, merged_cost, dimkey1):
    for index, item in enumerate(QueryOutput):
        normalized_json = pandas.json_normalize(item['Groups'])
        split_keys = pandas.DataFrame(
            normalized_json['Keys'].tolist(),
            columns=[dimkey1, 'UsageType']
        )
        cost = pandas.concat(
            [split_keys, normalized_json['Metrics.UnblendedCost.Amount']],
            axis=1
        )
        renamed_cost = cost.rename(
            columns={'Metrics.UnblendedCost.Amount': item['TimePeriod']['Start']}
        )
        merged_cost = pandas.merge(merged_cost, renamed_cost, on=[dimkey1, 'UsageType'], how='right')

    # merged_cost.to_csv('/Users/lijing/Desktop/我的工作/360/ce-services.csv', index=True)

    return merged_cost


if __name__ == "__main__":
    # 指定AWS configure profile
    session = boto3.Session(profile_name='jingamz')
    aws_client = session.client('ce')
    start='2023-01-01'
    end='2023-02-01'
    merged_cost_service = condf('Services', 'UsageType')
    response_service = getcedetail(aws_client, start, end, 'SERVICE', 'USAGE_TYPE')
    cost_service = GenCeReport(response_service, merged_cost_service, 'Services')
    cost_service.to_csv('/Users/lijing/Desktop/cost_service.csv', index=True)
    merged_cost_region = condf('Region', 'UsageType')
    response_region = getcedetail(aws_client, start, end, 'REGION', 'USAGE_TYPE')
    cost_region = GenCeReport(response_region, merged_cost_region, 'Region')
    cost_region.to_csv('/Users/lijing/Desktop/cost_region.csv', index=True)

    MyRegion = pandas.merge(cost_service, cost_region.drop([start], axis=1), on=['UsageType'],
                             how='left')

    # MyRegion = pandas.merge(cost_service, cost_region, on=['UsageType'],
    #                         how='left')
    # print(len(MyRegion))
    # print(len(cost_service))
    # print(len(cost_region))
    print(MyRegion)

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
    MyRegion['Services'] = np.where((MyRegion['Services'].str.contains('EC2 - Other')), 'Amazon Elastic Compute Cloud - Compute', MyRegion['Services'])
    MyRegion.to_csv('/Users/lijing/Desktop/add-region.csv', index=True)
    df = pandas.read_csv('/Users/lijing/Desktop/add-region.csv')
    df.head()
    bill_pivot = pandas.pivot_table(df, index=['Services','Region'], values=[start], aggfunc=np.sum, margins=True)
    # bill_pivot = pandas.pivot_table(df, index=['Services', 'Region'], values=[start], aggfunc=np.sum)
    # bill_pivot.sort_values(by=start, ascending=False, inplace=True)
    print(bill_pivot)
    bill_pivot.to_csv('/Users/lijing/Desktop/result.csv')
    df1 = pandas.read_csv('/Users/lijing/Desktop/result.csv')
    print(df1.to_json(orient='records'))

# Jingamz@
# 20230211
# 20230228 --  调整数据集 -- 数据聚合正常
# 20230301 --  生成Json 文件

import datetime
import boto3
import pandas
import numpy as np
import json


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


MyStr2 = '''{{ "Keys": [ "{metric}","{region}"],"Metrics": {{"UnblendedCost": {{"Amount": "{Amount}","Unit": "USD"}} }} }}'''

if __name__ == "__main__":

    # 指定AWS configure profile
    session = boto3.Session(profile_name='test')
    aws_client = session.client('ce')
    # 指定查询范围，注意，[start,end)
    start = '2023-01-01'
    end = '2023-02-01'
    # 构造DF
    merged_cost_service = condf('Services', 'UsageType')
    # 以 services 和 UsageType 为维度出数据。
    response_service = getcedetail(aws_client, start, end, 'SERVICE', 'USAGE_TYPE')
    # 生成DF
    cost_service = GenCeReport(response_service, merged_cost_service, 'Services')

    merged_cost_region = condf('Region', 'UsageType')
    response_region = getcedetail(aws_client, start, end, 'REGION', 'USAGE_TYPE')
    cost_region_filter = GenCeReport(response_region, merged_cost_region, 'Region')
    # 360 filter NoRegion
    cost_region = cost_region_filter.query("Region != 'NoRegion'")

    # 以 UsageType 为轴进行合并
    MyRegion = pandas.merge(cost_service, cost_region.drop([start], axis=1), on=['UsageType'],
                            how='left')

    # 合并生成DataTransfer 类型的数据
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
    # 合并 'EC2 - Other' 到 'Amazon Elastic Compute Cloud - Compute'
    MyRegion['Services'] = np.where((MyRegion['Services'].str.contains('EC2 - Other')),
                                    'Amazon Elastic Compute Cloud - Compute', MyRegion['Services'])
    MyRegion.to_csv('/tmp/add-region.csv', index=True)
    df = pandas.read_csv('/tmp/add-region.csv')
    df.head()
    bill_pivot = pandas.pivot_table(df, index=['Services', 'Region'], values=[start], aggfunc=np.sum, margins=True)

    bill_pivot.to_csv('/tmp/result.csv')
    df1 = pandas.read_csv('/tmp/result.csv')
    usage = df1.rename(columns={start: 'Usage'})
    list1 = []

    for row in usage.itertuples():
        dict_row = {
            "Keys": [
                row.Services,
                row.Region
            ],
            "Metrics": {
                "UnblendedCost": {
                    "Amount": row.Usage,
                    "Unit": "USD"
                }
            }
        }
        list1.append(dict_row)

    dict_result = {
        "TimePeriod": {
            "Start": start,
            "End": end
        },
        "Total": {},
        "Groups": list1
    }

    # 注意这里需要替换放置输出的文件。
    jsonFile = open('/Users/lijing/Desktop/CE20230301.json', 'w')
    jsonFile.write(json.dumps(dict_result, indent=4))
    jsonFile.close()

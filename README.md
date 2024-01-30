# rds_dashboard

## Metric
- docs : https://docs.aws.amazon.com/ko_kr/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html
- path : ./pi_to_cw/metric/*.json
    ```json
    [
        {
            "Metric": "db.Transactions.active_transactions.avg"
        },
        {
            "Metric": "db.load.avg",
            "GroupBy": {
                "Group": "db.wait_event"
            }
        }
    ]
    ```

## Run
- env : python3.8
```bash
(venv) [ec2-user@ip-10-10-101-168 rds_dashboard]$ pip install -r requirements.txt
(venv) [ec2-user@ip-10-10-101-168 rds_dashboard]$ cd pi_to_cw
(venv) [ec2-user@ip-10-10-101-168 pi_to_cw]$ python main.py
Processing metric.json: 2 metrics Complete!
(venv) [ec2-user@ip-10-10-101-168 pi_to_cw]$
```

## Result - CloudWatch Custom Metric
![Alt text](./img/1.png)
![Alt text](./img/4.png)
![Alt text](./img/2.png)
![Alt text](./img/3.png)

## Refer
- Performance Insights counter metrics : https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html
- Performance Insight Boto3 Doc : https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/pi.html

- SQL statistics for Aurora MySQL : https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.UsingDashboard.AnalyzeDBLoad.AdditionalMetrics.MySQL.html
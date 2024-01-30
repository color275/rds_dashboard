# rds_dashboard

## 메트릭 경로
./metric/*.json

## 실행
- 환경 : python3.8
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


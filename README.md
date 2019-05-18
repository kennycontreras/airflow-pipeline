# Airflow Pipeline

ETL created using Airflow. This project load data from S3 buckets and write the data into staging, fact and dimension tables. Using PostgresOperator and PythonOperator we are able to create Fact and Dimension tables for a star-schema.

This project runs data quality check into dimension and fact tables checking  null values and empty tables.

## Graph View 

#### Main Dag

![alt text](https://i.imgur.com/qTqTi7V.png "Main Dag")

Schedule:

```python
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 11, 1),
    'end_date:': datetime(2018, 11, 30),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}
```

Beside default parameters above, this DAG runs hourly with max 1 active run at the same time.

`start_date` context is used to load data from S3 (CSV files). All hooks are created with the finality being flexibles. This means that you can define is you want to delete or just append the data into dimension and fact tables. You can use different connections id too. Just make sure that the Hook fit your purpouse

#### Subdag

![alt text](https://i.imgur.com/rGwWFld.png "Subdag")  

Subdag created to write all dimension tables. 
The entire subdag use `LocalExecutor()` method to runs all taks at the same time. 

#### Development
Want to contribute? Great! please feel free to open issues and push.

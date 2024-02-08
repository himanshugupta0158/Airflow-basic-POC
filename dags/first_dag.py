try:
    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    import pandas as pd

    print("All Dag modules are OK")
except Exception as e:
    print("Error {}".format(e))


def test_func1(*args, **kwargs):
    x = kwargs.get("name", "Did not get the KEY")
    print("Hello World : {}".format(x))
    return "Hello World " + x


def first_function_execute(**context):
    print("First Function Executes ", "-" * 6)
    context["ti"].xcom_push(key="mykey", value="first_function_execute says Hello")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [
        {"name": "Himanshu", "title": "Software Developer"},
        {"name": "Pratik", "title": "Software Developer"},
        {"name": "Rahul", "title": "Data Engineer"},
    ]
    df = pd.DataFrame(data=data)
    print("@" * 66)
    print(df.head())
    print("@" * 66)

    print(
        "I am in second_function_execute got value : {} from Function 1".format(
            instance
        ),
    )


# */2 * * * * Execute every 2 min
with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retires": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 1, 1),
    },
    catchup=False,
) as f:
    test_func1 = PythonOperator(
        task_id="test_func1",
        python_callable=test_func1,
        op_kwargs={"name": "Himanshu"},
    )

    # "provide_context=True" is used for below both function b/c 2nd one is depended on first
    # like sending data from 1st and 2nd one will try to get it

    first_function_execute = PythonOperator(
        task_id="first_function_execution",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Himanshu"},
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execution",
        python_callable=second_function_execute,
        provide_context=True,
    )

# below is used to show what kind of related 1st and 2nd function have
first_function_execute >> second_function_execute

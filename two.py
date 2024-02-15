from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def my_python_function():
    # Your Python logic goes here
    print("Hello from the PythonOperator!")
    raise Exception("explicity failing the task")

def send_email_on_failure(context, **kwargs):
    industry = kwargs.get('industry', 'default_industry')
    stack = kwargs.get('stack', 'default_stack')
    print(f"industry: {industry}, stack: {stack}")

# Define your DAG
dag = DAG(
    'example_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
)

# Create a task using PythonOperator and pass parameters to the callback function
task_python_operator = PythonOperator(
    task_id='python_task',
    python_callable=my_python_function,
    on_failure_callback=send_email_on_failure,
    op_kwargs={'industry': 'tec_tkd', 'stack': 'A'},  # Pass parameters here
    provide_context=True,  # This allows passing context to the callback
    dag=dag,
)

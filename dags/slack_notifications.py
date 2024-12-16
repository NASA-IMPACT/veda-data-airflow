from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.variable import Variable


def slack_alert(context, circle, status):
    slack_conn_id = "slack_connection_id"
    ti = context.get("task_instance")
    pocs = ti.dag_run.conf.get("pocs", [])
    vars = Variable.get("aws_dags_variables", deserialize_json=True)
    sm2a_base_url = f'https://{vars.get("SM2A_BASE_URL", "localhost:8080")}'
    slack_msg = """
            :{circle}: Task {status}. 
            *Task*: {task}
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 

            """.format(
        status=status,
        circle=circle,
        task=ti.task_id,
        dag=ti.dag_id,
        exec_date=context.get("data_interval_start"),
        log_url=ti.log_url.replace("http://localhost:8080", sm2a_base_url),
    )
    if pocs:
        pocs_mentions = " ".join([f"<@{poc}>" for poc in pocs])
        slack_msg = f"{slack_msg}\n*points of contact*: {pocs_mentions}"
    print(f"{context=}")
    _alert = SlackWebhookOperator(
        task_id="slack_test",
        slack_webhook_conn_id=slack_conn_id,
        message=slack_msg,
        username="airflow",
    )
    return _alert.execute(context=context)


def slack_fail_alert(context):
    return slack_alert(context=context, circle="red_circle", status="Failed")


def slack_success_alert(context):
    return slack_alert(context=context, circle="large_green_circle", status="Succeeded")


def slack_warning_alert(context):
    return slack_alert(context=context, circle="large_yellow_circle", status="Warning")

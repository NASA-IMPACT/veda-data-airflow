def slack_alert(context, circle, status):
    slack_conn_id = "slack_connection_id"
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    pocs = (dag_run.conf or {}).get("pocs", []) if dag_run else []
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
        log_url=ti.log_url,
    )
    if pocs:
        pocs_mentions = " ".join([f"<@{poc}>" for poc in pocs])
        slack_msg = f"{slack_msg}\n*points of contact*: {pocs_mentions}"
    try:
        # Only trigger slack notification
        # when the connection is defined and library is installed
        from airflow.providers.slack.operators.slack_webhook import (
            SlackWebhookOperator,
        )

        slack_test = SlackWebhookOperator(
            task_id="slack_test",
            slack_webhook_conn_id=slack_conn_id,
            message=slack_msg,
            username="airflow",
        )
        return slack_test.execute(context=context)
    except Exception as ex:
        return f"Error: {ex}"


def slack_fail_alert(context):
    return slack_alert(context=context, circle="red_circle", status="Failed")


def slack_success_alert(context):
    return slack_alert(context=context, circle="large_green_circle", status="Succeeded")


def slack_warning_alert(context):
    return slack_alert(context=context, circle="large_yellow_circle", status="Warning")

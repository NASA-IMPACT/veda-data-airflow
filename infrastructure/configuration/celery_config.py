import os

from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

# From here https://github.com/apache/airflow/issues/16163
# DEFAULT_CELERY_CONFIG['task_acks_late'] = False

# Must exceed the runtime of the longest task. If SQS makes a message visible
# again while its task is still running, the message is redelivered and a second
# worker tries to start the same task instance. On Airflow 3 the Task Execution
# API rejects that second start with `invalid_state`, the Celery executor reports
# the task as failed, and the scheduler then kills the healthy original run --
# the task fails despite never having errored.
#
# NOTE: this alone is not sufficient. Because `predefined_queues` is set below,
# kombu does not create the queue, and the SQS queue's own VisibilityTimeout
# attribute governs redelivery. That attribute must be raised to match (it is
# created at the AWS default of 30s by the sm2a module, which does not set
# aws_sqs_queue.visibility_timeout_seconds).
VISIBILITY_TIMEOUT = 21600  # 6h; SQS allows up to 43200

CELERY_CONFIG = {
    **DEFAULT_CELERY_CONFIG,
    "broker_transport_options": {
        **DEFAULT_CELERY_CONFIG["broker_transport_options"],
        "visibility_timeout": VISIBILITY_TIMEOUT,
        "predefined_queues": {
            # Gotcha: kombu.transport.SQS.UndefinedQueueException
            # Queue with name 'default' must be defined in 'predefined_queues'
            "default": {
                "url": os.getenv(
                    "X_AIRFLOW_SQS_CELERY_BROKER_PREDEFINED_QUEUE_URL",
                    "sqs://user:pass@celery-broker:9324/",
                )
            },
            "gpu_queue": {
                "url": os.getenv(
                    "X_AIRFLOW_SQS_CELERY_BROKER_GPU_QUEUE_URL",
                    "sqs://user:pass@celery-broker:9324/",
                )
            },
        },
    },
    "polling_interval": 1.0,
    # SQS broker is incompatible with remote control commands
    "worker_enable_remote_control": False,
}

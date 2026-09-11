import os

from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

# From here https://github.com/apache/airflow/issues/16163
# DEFAULT_CELERY_CONFIG['task_acks_late'] = False

# Must exceed the longest task runtime, or SQS redelivers mid-run and the duplicate
# delivery fails the task on Airflow 3. The queue's own VisibilityTimeout attribute
# must match, since predefined_queues means kombu does not create the queue.
VISIBILITY_TIMEOUT = 1800  # 30m; SQS allows up to 43200

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

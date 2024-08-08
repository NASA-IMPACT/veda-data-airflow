import argparse
import logging
import sys
import time
from contextlib import contextmanager
from typing import List

import botocore.session
from airflow.models import DagModel, TaskInstance
from airflow.settings import Session
from airflow.utils.state import State
from sqlalchemy import func

logging.basicConfig(level=logging.INFO, stream=sys.stdout)


@contextmanager
def session_scope(_session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield _session
    finally:
        _session.close()


def get_unique_hostnames_for_states(states: List[str]) -> List[str]:
    """
    Returns the list of unique hostnames where tasks are in one of {states}

    See below for a list of possible states for a Task Instance
    https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#task-instances
    """
    with session_scope(Session) as _session:
        unique_hostnames_query = _session.query(
            TaskInstance.hostname.distinct()
        ).filter(TaskInstance.state.in_(states))
        hostnames = [result[0] for result in unique_hostnames_query]
        return hostnames


def get_pending_tasks_count(states: List[str]):
    """
    Returns the number of tasks in a 'queued' state.
    """
    with session_scope(Session) as _session:
        pending_tasks_count = (
            _session.query(func.count(TaskInstance.task_id))
            .filter(TaskInstance.state.in_(states))
            .scalar()
        )
        return pending_tasks_count


def get_tasks_info(ecs_client, cluster_name, service_name):
    # List running tasks for the specified ECS service
    response = ecs_client.list_tasks(
        cluster=cluster_name, serviceName=service_name, desiredStatus="RUNNING"
    )

    # Extract and return the running task ARNs
    running_task_arns = response.get("taskArns", [])

    tasks_info = (
        ecs_client.describe_tasks(cluster=cluster_name, tasks=running_task_arns)
        if running_task_arns
        else {"tasks": []}
    )
    # Extract and return task information, including hostnames
    task_info_list = []
    hostname = None
    for task_info in tasks_info["tasks"]:
        task_arn = task_info["taskArn"]
        for detail in task_info["attachments"][0]["details"]:
            if detail["name"] == "privateDnsName":
                hostname = detail["value"]
                continue
        task_info_list.append({"task_arn": task_arn, "task_hostname": hostname})

    return task_info_list


def scale_up_ecs_service(ecs_client, cluster_name, service_name, max_desired_count):
    # Scale up the ECS service by updating the desired count
    response = ecs_client.describe_services(
        cluster=cluster_name, services=[service_name]
    )
    current_desired_count = response["services"][0]["desiredCount"]

    # Calculate the new desired count after scale-up
    new_desired_count = min(current_desired_count + 1, max_desired_count)
    if new_desired_count == max_desired_count:
        logging.info("We reached the max needed tasks")
        return

    # Scale Up the ECS service
    ecs_client.update_service(
        cluster=cluster_name, service=service_name, desiredCount=new_desired_count
    )

    print(f"ECS service {service_name} scaled up to {new_desired_count} tasks.")


def scale_down_ecs_service(
    ecs_client, cluster_name, service_name, task_state, min_desired_count
):
    tasks_to_kill = set()
    tasks_to_not_kill = set()
    # Get tasks info
    tasks_info = get_tasks_info(
        ecs_client=ecs_client, cluster_name=cluster_name, service_name=service_name
    )
    hosts = get_unique_hostnames_for_states(states=task_state)
    pending_tasks = get_pending_tasks_count(states=[State.QUEUED, State.SCHEDULED])
    if pending_tasks:
        print(f"Nothing to scale down since we have {pending_tasks} tasks in the queue")
        return
    for task_info in tasks_info:
        task_arn, task_hostname = task_info["task_arn"], task_info["task_hostname"]
        tasks_to_kill.add(task_arn)
        for host in hosts:
            if host == task_hostname:
                tasks_to_not_kill.add(task_arn)
    tasks_to_kill = tasks_to_kill.difference(tasks_to_not_kill)
    if len(tasks_to_kill) == 0:
        print("No tasks to kill")
        return
    # Scale down the ECS service by updating the desired count
    response = ecs_client.describe_services(
        cluster=cluster_name, services=[service_name]
    )
    current_desired_count = response["services"][0]["desiredCount"]

    # Calculate the new desired count after scale-down
    new_desired_count = max(
        min_desired_count, current_desired_count - len(tasks_to_kill)
    )

    if new_desired_count == current_desired_count:
        logging.info("Nothing to do here")
        return

    # Terminate specified tasks
    # And keep desired count
    tasks_to_kill = list(tasks_to_kill)[: len(tasks_to_kill) - new_desired_count]

    logging.info(
        f"ECS service {service_name} scaled down to {current_desired_count - len(tasks_to_kill)} tasks."
    )
    for task_to_kill in tasks_to_kill:
        print(f"Terminating task: {task_to_kill}")
        ecs_client.stop_task(cluster=cluster_name, task=task_to_kill)
    # Scale down the ECS service
    ecs_client.update_service(
        cluster=cluster_name,
        service=service_name,
        desiredCount=current_desired_count - len(tasks_to_kill),
    )


def get_task_count_where_state(states: List[str]) -> int:
    """
    Returns the number of tasks in one of {states}

    See below for a list of possible states for a Task Instance
    https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#task-instances
    """
    with session_scope(Session) as session:
        tasks_query = (
            session.query(
                TaskInstance.dag_id,
                func.count("*").label("count"),
            )
            .filter(TaskInstance.state.in_(states))
            .group_by(TaskInstance.dag_id)
            .subquery()
        )
        count = (
            session.query(func.sum(tasks_query.c.count))
            .join(DagModel, DagModel.dag_id == tasks_query.c.dag_id)
            .filter(
                DagModel.is_active.is_(True),
                DagModel.is_paused.is_(False),
            )
            .scalar()
        )
        if count is None:
            return 0
        return int(count)


def get_capacity_provider_reservation(
    current_task_count: int,
    current_worker_count: int,
    desired_tasks_per_instance: int = 5,
) -> int:
    """
    CapacityProviderReservation = M / N * 100

    M is the number of instances you need.
    N is the number of instances already up and running.

    If M and N are both zero, meaning no instances and no running tasks, then
    CapacityProviderReservation = 100. If M > 0 and N = 0, meaning no instances and no
    running tasks, but at least one required task, then CapacityProviderReservation = 200.

    The return value unit is a percentage. Scale airflow workers by applying this metric
    in a target tracking scaling policy with a target value of 100.

    Source:
    https://aws.amazon.com/blogs/containers/deep-dive-on-amazon-ecs-cluster-auto-scaling/
    """
    m = current_task_count / desired_tasks_per_instance
    n = current_worker_count
    if m == 0 and n == 0:
        return 100
    elif m > 0 and n == 0:
        return 200
    return int(m / n * 100)


# Publish a custom metric for worker scaling
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/publishingMetrics.html
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster-name",
        type=str,
        required=True,
        help="Cluster name used as metric dimension",
    )
    parser.add_argument(
        "--period",
        type=int,
        default=60,
        help="The interval (in seconds) to call the put_metric_data API",
    )
    parser.add_argument(
        "--desired-count",
        type=int,
        default=0,
        help="The desired workers",
    )
    parser.add_argument(
        "--region-name", type=str, required=True, help="AWS region name"
    )
    parser.add_argument(
        "--worker-service-name",
        type=str,
        required=True,
        help="The name of the airflow worker ECS service.",
    )
    args = parser.parse_args()
    logging.info("Arguments parsed successfully")

    session = botocore.session.get_session()
    cloudwatch = session.create_client("cloudwatch", region_name=args.region_name)
    ecs = session.create_client("ecs", region_name=args.region_name)
    task_count_pointer = 0
    while True:

        task_count = get_task_count_where_state(
            states=[State.QUEUED, State.RUNNING, State.UP_FOR_RETRY]
        )
        logging.info(f"NumberOfActiveRunningTasks: {task_count}")

        worker_service = ecs.describe_services(
            cluster=args.cluster_name, services=[args.worker_service_name]
        )["services"][0]
        worker_count = worker_service["pendingCount"] + worker_service["runningCount"]
        logging.info(f"NumberOfWorkers: {worker_count}")

        metric_value = get_capacity_provider_reservation(
            current_task_count=task_count,
            current_worker_count=worker_count,
            desired_tasks_per_instance=10,
        )
        if metric_value > 100:
            logging.info(f"We are scaling up {metric_value}")
            scale_up_ecs_service(
                ecs_client=ecs,
                cluster_name=args.cluster_name,
                service_name=args.worker_service_name,
                max_desired_count=args.desired_count,
            )

        elif metric_value < 100:
            scale_down_ecs_service(
                ecs_client=ecs,
                cluster_name=args.cluster_name,
                service_name=args.worker_service_name,
                task_state=[State.RUNNING],
                min_desired_count=1,
            )

        logging.info(f"Sleeping for {args.period} seconds")
        time.sleep(args.period)

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task
from veda_data_pipeline.helpers.veda_gibs_wmts2stac_update_pipeline import gibs_wmts2stac_update_task_group, wmts2stac_task_group, VedaGibsWMTSConfig

def get_ingest_gibswmts2stac_dag(id: str, event: VedaGibsWMTSConfig) -> DAG:
    """
    A wrapper function that creates the veda_gibs_wmts2stac_with_update dags for specific collection
    - VedaGibsWMTSConfig is the expected dataclass.
    :param id: Id for the DAG. should be unique
    : param event: A config dict 
    """
    collection_config = event["collection_config"]
    collection_id = collection_config["id"]
    gibs_url = event["gibs_url"]
    schedule=event["schedule"] or "0 0 * * *"
    dag_doc_md = f"""
        ## This DAG handles creation of STAC Collection from (GIBS) WMTS. If a schedule is provided along with Gibs url in event: VedaGibsWMTSConfig, it sets a scheduler to check and update the STAC.
        ### How does it update:
        - For the frequency set by schedule in VedaGibsWMTSConfig, the DAG checks if the source wmts collection which is indexed as STAC collection
        is avaialble for the latest available date via. {gibs_url}
        - If available, it overrides the {collection_id} collection with the updated temporal extent into the STAC.
        #### Note
        - This DAG uses the following configuration json to ingest to STAC<br>
        - TODO: validation of the collection_config with respect to STAC extension for WMTS.
        ```json
        {collection_config}
        ```
        """
    dag_args = {
        "start_date": pendulum.today("UTC").add(days=-1),
        "catchup": False,
        "doc_md": dag_doc_md,
        "tags": ["collection", "WMTS", "GIBS", "STAC", "NRT", "worldview"],
    }

    @dag(
        dag_id=id,
        schedule=schedule,
        render_template_as_native_obj=True,
        **dag_args
    )
    def veda_gibs_wmts2stac_with_update(collection_config: dict, collection_id: str, gibs_url: str):
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        @task.branch
        def is_gibs(gibs_url: str) -> str:
            """
            This task branches between two task groups based on whether gibs_url is provided.
            - If gibs_url is provided: use gibs_wmts2stac_update_task_group for NRT updates
            - If no gibs_url: use wmts2stac_task_group for simple collection ingestion
            :param gibs_url: The GIBS URL string
            :return: Task group ID to execute
            """
            if gibs_url:
                return 'gibs_wmts2stac_update_task_group'
            else:
                return 'wmts2stac_task_group'

        # Only instantiate the task group that will actually execute
        if gibs_url:
            gibs_update_group = gibs_wmts2stac_update_task_group(
                        collection=collection_config,
                        gibs_url=gibs_url,
                        collection_id=collection_id
                    )
            task_choice = is_gibs(gibs_url)
            start >> task_choice >> gibs_update_group >> end
        else:
            wmts2stac_group = wmts2stac_task_group(collection=collection_config)
            start >> task_choice >> wmts2stac_group >> end
            task_choice = is_gibs(gibs_url)

    return veda_gibs_wmts2stac_with_update(collection_config=collection_config, collection_id=collection_id, gibs_url=gibs_url)

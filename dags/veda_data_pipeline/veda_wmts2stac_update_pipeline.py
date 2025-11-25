import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task
from veda_data_pipeline.helpers.veda_wmts2stac_update_pipeline import gibs_wmts2stac_update_task_group, wmts2stac_task_group, validate_collection_task, VedaWMTS2STACConfig

def get_ingest_wmts2stac_dag(id: str, event: VedaWMTS2STACConfig) -> DAG:
    """
    A wrapper function that creates the veda_gibs_wmts2stac_with_update dag for specific collection
    - VedaWMTS2STACConfig is the expected dataclass.
    :param id: Id for the DAG. should be unique
    : param event: A config dict 
    """
    collection_config: VedaWMTS2STACConfig = event.get("collection_config", {})
    if not collection_config:
        raise ValueError("Missing required field 'collection_config' in event")

    gibs_url: str = event.get("gibs_url", "")
    schedule = event.get("schedule") if gibs_url else None
    dag_doc_md = f"""
        ## This DAG handles creation of STAC Collection from WMTS.
        If a schedule is provided along with Gibs url in event: VedaWMTS2STACConfig, it sets a scheduler to check and update the STAC based on GIBS metadata.
        Else, The collection_config is used to ingest the Gibs.
        ### How does update task group work:
        - For the frequency set by schedule in VedaWMTS2STACConfig, the DAG checks if the source wmts collection is avaialble for the latest available date via. {gibs_url}
        - If available, it overrides the collection with the updated temporal extent into the STAC.
        #### Note
        - This DAG uses the following configuration json to ingest to STAC<br>
        ```json
        {collection_config}
        ```
        """

    dag_args = {
        "start_date": pendulum.today("UTC").add(days=-1),
        "catchup": False,
        "doc_md": dag_doc_md,
        "tags": ["collection", "WMTS", "GIBS", "STAC", "NRT", "worldview"],
        "max_active_runs": 1,
        "default_args": {
            "retries": 0,  # Don't retry failed tasks
        }
    }

    @dag(
        dag_id=id,
        schedule=schedule,
        render_template_as_native_obj=True,
        **dag_args
    )
    def veda_gibs_wmts2stac_with_update(collection_config: dict, gibs_url: str):
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(
            task_id="end",
            trigger_rule=TriggerRule.ALL_DONE
        )

        @task
        def extract_collection_id(collection_config: dict) -> str:
            """
            Extracts and validates the collection ID from the collection config.
            :param collection_config: The collection configuration dictionary
            :return: The collection ID
            :raises ValueError: If collection ID is missing
            """
            collection_id = collection_config.get("id", "")
            if not collection_id:
                raise ValueError("Missing required field 'id' in collection_config")
            return collection_id

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

        validated_config = validate_collection_task(collection_config)
        collection_id = extract_collection_id(validated_config)

        gibs_update_group = gibs_wmts2stac_update_task_group(
            collection=validated_config,
            gibs_url=gibs_url if gibs_url else "",
            collection_id=collection_id
        )
        wmts2stac_group = wmts2stac_task_group(collection=validated_config)

        task_choice = is_gibs(gibs_url)
        start >> validated_config >> task_choice
        task_choice >> [gibs_update_group, wmts2stac_group] >> end
        validated_config >> end

    return veda_gibs_wmts2stac_with_update(collection_config=collection_config, gibs_url=gibs_url)

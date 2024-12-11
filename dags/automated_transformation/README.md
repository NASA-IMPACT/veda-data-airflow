## Information about the folder
This folder contains `automation DAG`. It contains the DAG which is essential for transforming the given dataset into COGs.
The folder consists of two files-:
- `automation_dag.py` - It contains the DAG architecture.
- `transformation_pipeline.py` - It contains multiple functions to achieve the transformation of a given netCDF file to a COG. The file fetches checks whether the `transformation plugins` exist on `S3 bucket`, fetches the function and uses it for transformation. The statistics for the `netCDF` and `COG` files are also calculated and stored in a `JSON` file and pushed to `S3` along with the `transformed COG`.
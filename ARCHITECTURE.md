# Architecture

## Architecture Diagram

The following architecture diagram shows the **data transformation/ingestion pipeline**. Data can be discovered from CMR or from AWS S3. If the data needs to be converted into a cloud optimized geotiff, it's done in the cogification step. Then, it's uploaded to the official VEDA S3 bucket (if needed) and published to the STAC database and API.
![image](veda-data_ingest_pipeline.png)


## The architecture as step functions

The architecture defined above has been implemented as step functions (+ other resources in AWS) and the pictures below show how they look in AWS console step function graph view.

### Discovery

![image](https://user-images.githubusercontent.com/7830949/171733787-f088b7a1-3741-491e-afc1-90e8de95185f.png)

### Cogification

![image](https://user-images.githubusercontent.com/7830949/171733963-7eabfb61-c5cc-4610-8f16-40ad3e998f8e.png)

### Ingest and Publish

![image](https://user-images.githubusercontent.com/7830949/171734056-85b194b9-659c-4a57-814c-f91ffc37fdd2.png)

### Create data pipelines with Azure Data Factory

> A simple data pipeline of type "Copy Activity" to transform from CSV to Azure SQL table

##### Some key step

1. Create Blob Storage within an Account Storage

![Screenshot](./upload-blob-csv.png)

2. Create Linked Service and Datasets

![Screenshot](./create-linkedservice-and-dataset-1.png)
![Screenshot](./create-linkedservice-and-dataset-2.png)

3. Create Source & Sink Datasets

![Screenshot](./create-dataset.png)

4. Add Activity to Pipeline and Import Mapping

![Screenshot](./add-copy-activity-and-check-mapping.png)
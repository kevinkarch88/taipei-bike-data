from google.cloud import dataproc_v1
import google.auth


def submit_dataproc_job(request):
    project_id = "taipei-bike-data-project"
    region = "us-central1"
    cluster_name = "my-cluster"

    endpoint = f"{region}-dataproc.googleapis.com:443" # doesn't work with just 'us-central'

    # Get credentials and initialize the Dataproc client with the correct endpoint
    credentials, project = google.auth.default()
    client = dataproc_v1.JobControllerClient(
        credentials=credentials,
        client_options={"api_endpoint": endpoint}
    )

    # pyspark job is in a bucket, replace bucket name and project name
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://bucket-name/main.py",
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"],
            "args": ["--project=project-name"]
        },
    }

    # url is created for a POST
    result = client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    return f"Submitted job: {result.result().reference.job_id}"

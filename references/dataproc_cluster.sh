#export PROJECT=data-prod
#export BUCKET_NAME=i18n-subreddit-clustering
#export CLUSTER=i18n-dsm
#export REGION=us-west1-b

# enable/ make sure dataproc API is available here:
# https://console.developers.google.com/apis/api/dataproc.googleapis.com/overview?project=212906482731

# video with debugging tips
# https://www.youtube.com/watch?v=5OYT2SSMGo8&ab_channel=DecisionForest

# Command from console:
gcloud beta dataproc clusters create cluster-i18n-dsm --region us-central1 --zone us-central1-a --master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10 --optional-components JUPYTER --scopes 'https://www.googleapis.com/auth/cloud-platform' --project data-science-prod-218515


# try older version ENABLE OPTIONAL COMPONENTS & COMPONENT GATEWAY `--enable-component-gateway`
#  OTHERWISE WE WON'T BE ABLE TO SEE JUPYTER NOTEBOOKS
gcloud beta dataproc clusters create cluster-i18n-dsm-ubuntu15 --enable-component-gateway --region us-central1 --zone us-central1-c --master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 1.5-ubuntu18 --optional-components ANACONDA,JUPYTER,FLINK --scopes 'https://www.googleapis.com/auth/cloud-platform' --project data-science-prod-218515



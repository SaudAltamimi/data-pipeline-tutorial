## How to deploy on AWS?

1. Install `awscli`

2. run `aws configure` 
    * AWS Access Key ID : 
    * AWS Secret Access Key : 
    * Default region name: `us-west-2`
    * Default output format : `json`
    
3. **copy all the necessary files to an s3 bucket**
      
    > `emr_bootstrap.sh`
    > `etl.py`

    * Ex: `aws s3 cp <filename> s3://<bucket_name>`
    
    
4. **Run EMR create script with the etl job**

```
aws emr create-cluster --name "Spark cluster with step" \
    --release-label emr-5.30.1 \
    --applications Name=Spark \
    --log-uri s3://dendsparktutorial/logs/ \
    --ec2-attributes KeyName=emr-key \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --bootstrap-actions Path=s3://dendsparktutorial/emr_bootstrap.sh \
    --steps Type=Spark,Name="Spark program",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://dendsparktutorial/src/etl.py] \
    --use-default-roles \
    --auto-terminate
```

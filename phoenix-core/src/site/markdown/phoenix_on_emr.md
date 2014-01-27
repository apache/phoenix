# Phoenix on Amazon EMR

Follow these steps to deploy HBase with Phoenix on Amazon's Elastic MapReduce (EMR).

### 1. Amazon EMR Configuration

1. Create a free/paid [EMR account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) 
2. [Download](http://aws.amazon.com/developertools/2264) the latest CLI from and follow the setup instructions [here](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-cli-install.html).

_Note: Step 2 is a multi-step process in which you would install ruby, gem, create credentials file and create a S3 bucket._

### 2. Deploy HBase with Phoenix on EMR using Web Console

##### Using Web Console

Go to _Elastic MapReduce > Create Cluster_ and follow the steps listed below:

![EMR Create Cluster Page](http://phoenix-bin.github.io/client/images/EMR2.png)

1. Type your cluster name
2. Set _AMI version_ to _3.0.1_
3. From _Additional Application_ drop down, select HBase and add.
4. In _Core_ text box, enter the number of HBase region server(s) you want configured for your cluster
5. Add a _custom action bootstrap_ from dropdown and specify S3 location: ``s3://beta.elasticmapreduce/bootstrap-actions/phoenix/install-phoenix-bootstrap.sh`` and click add.
6. Click _Create Cluster_

##### Using CLI

Instead of using _Web Console_, you may use following _CLI_ command to deploy _HBase_ with _Phoenix_:

 ```
./elastic-mapreduce --create --instance-type c1.xlarge --name 
PHOENIX_2.2_install --ami-version 3.0.1 --hbase --alive --bootstrap-action 
"s3://beta.elasticmapreduce/bootstrap-actions/phoenix/install-phoenix-bootstrap.sh"
```

### 3. Usage

_SSH_ to the _EMR Master_ and CD to _/home/hadoop/hbase/lib/phoenix/bin_

Create test data: ``./performance localhost 1000000 ``

SQL CLI: ``./sqlline localhost``

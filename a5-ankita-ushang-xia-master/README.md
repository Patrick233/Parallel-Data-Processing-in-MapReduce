# Assignment 5: Flight Data Prediction

**Prerequisite: Set HADOOP HOME in `Makefile`**

After cloning the repo, edit and put your hadoop distribution path at line 1 of `Makefile` in the `HADOOP_HOME` variable. 

### Setting up the input:

The input can be set from the `inputFile`. The input needs to be one line per prediction as follows:
```
 2001,2,1,BOS,LAX
 2001,3,1,DEN,LAX
 2001,7,1,JFK,DAL
 2001,9,11,DEN,DCA
 ```
The values are of the form:
`Year` : 4 digits only, 
`Month` : If single digit month please enter 9 and not 09,
`Day`: If single digit day please enter 1 and not 01,
`SRC` : Source airport,
`DEST`: Desination airport

You can run this Assignment either on AWS or local. Please follow the steps given below in order.

### AWS Execution:

Steps to be followed after setting `HADOOP_HOME`

1. Setup `AWS CLI` if you already haven't

    a.  Install:
    
    Click [here](http://docs.aws.amazon.com/cli/latest/userguide/installing.html) to see install instructions. 

    b.  Configure:
    
    Click [here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) to see configure instructions.
    
2. Make sure your AWS User has a `AWS Access Key ID` , a `AWS Secret Access Key` and HAS the following bare minimum rules added:
    ```
    EMR_AutoScaling_DefaultRole
    EMR_DefaultRole
    EMR_EC2_DefaultRole
    ```

3. Configure `Makefile`: Makefile options are as follows

    ```
    aws.emr.release=emr-5.9.0       //  The release number for AWS EMR to be used. DO NOT CHANGE !
    aws.region=us-east-2            //  Select the region you put in Step 1b
    aws.bucket.name=mr-a5-input     //  The input folder (bucket) name on S3 where the corpus is
    aws.jar.bucket=mr-a5-jar        //  The bucket where the jar is pushed. Create manually or use make command.
    aws.output=mr-a5-output         //  The bucket where output will be generated. Please DO NOT create this in S3 Manually.
                                    //  Output bucket SHOULD NOT be present in S3 and should have globally unique name.
    aws.subnet.id=subnet-1dcd8c66   //  The subnet id from EC2 with subnet keyword
    aws.log.dir=mr-a5-logs          //  The log bucket on s3
    aws.num.nodes=4                 //  No. of nodes: Switch between 2 and 4 to test this assignment
    aws.instance.type=m4.2xlarge    //  Type of AWS Instance. DO NOT CHANGE !
    ...
    cluster.name=A5ClusterNodes4    //  The unique name you want to give this cluster.
    ```
    Make sure all the buckets you create are in the same region as `aws.region`.
    
    If data is not already loaded to S3. You can do it as follows:
    
    `make upload-input-aws`
    
    This will also create the bucket for the input.

4. Finally after all the `Makefile` configuration is done run this command to start execution:

    `make cloud`

    This will build the `jar` push it to the `S3 bucket` and launch the `EMR` instance as mentioned in the configuration.
    At this point you should wait and check `EMR Console` on your `AWS` Account to check if this worked and monitor the cluster.

5. Once the execution finishes and if you wish to check the CSV output generated:

    `make csv`

6. Once the output is complete to run again you need to do some cleanup/post-processing:

    `make delete-output-aws`

7. If you wish to check the logs:

    `make download-logs`
    
### Local Execution:

Steps to be followed after setting `HADOOP_HOME`


**Prepare the data if not already loaded**

`make setup` : This should ideally be run for the first time in order to load data into HDFS memory.

**Run `make` :**

```make ```

Where `k` is the k to be passed for the K-neighborhood and

This runs the program and generates the output.

**Post Processing / Data Cleanup options:**

`make clean` : This should be run after the entire run. This is to clear the output folder from HDFS. This NEEDS to be 
done if you are running the code again.


### Checking the output:

The report for both the cases can be generated manually using :

`make report`

While the report is automatically generated for Local Version you will have to run this command explicitly for AWS Version.
The various reports generated are shown below:

`report.Rmd` gives the R Markdown report of the serial and parallel runtime analysis

`report.html` is the HTML version of `report.Rmd`

`report.pdf` is the PDF version of `report.Rmd`

`/output_csv` stores the CSV files of the result. The result has airline and airport delays for each month in the entire data set.


### Model Accuracy Check:

In order to check the accuracy of our model for the report we have another `jar` called `accuractCheck.jar` in our repo. 
This is for internal/report uses only. In the similar context files `FlightValidation.java` and `ValidationDriver.java` 
can be neglected. So is the `src/META-INF-2` folder.

HADOOP_HOME = /Users/admin/Downloads/hadoop-2.8.1
MY_CLASSPATH = lib/hadoop-common-2.8.1.jar:lib/hadoop-mapreduce-client-common-2.8.1.jar:lib/hadoop-mapreduce-client-core-2.8.1.jar:out:.
JAR_NAME = a4FlightAnalysis.jar

# AWS EMR Execution
aws.emr.release=emr-5.9.0
aws.region=us-east-2
aws.bucket.name=mr-a4-input
aws.jar.bucket=mr-a4-jar
aws.output=mr-a4-output
aws.subnet.id=subnet-1dcd8c66
aws.log.dir=mr-a4-logs
aws.num.nodes=4
aws.instance.type=m4.2xlarge

# Other Variables
cluster.name=A4ClusterNodes4
local.logs=logs

all: build run

build: compile jar

compile:
	javac -cp $(MY_CLASSPATH) -d output src/*.java

jar:
	cp -r src/META-INF/MANIFEST.MF output
	cd output; jar cvmf MANIFEST.MF $(JAR_NAME) *
	mv output/$(JAR_NAME) .

run:
	$(HADOOP_HOME)/bin/hadoop jar $(JAR_NAME) input output
	Rscript -e 'rmarkdown::render("report.Rmd")'

clean:
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r -f output

gzip:
	gzip input/*

gunzip:
	gunzip input/*

setup:
	$(HADOOP_HOME)/bin/hdfs dfs -put input input


report:
	Rscript -e 'rmarkdown::render("report.Rmd")'


# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}
	aws s3 mb s3://${aws.jar.bucket}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync input s3://${aws.bucket.name}/

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp $(JAR_NAME) s3://${aws.jar.bucket}

# Main EMR launch.
cloud: build upload-app-aws
	aws emr create-cluster \
		--name "$(cluster.name)" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/","s3://${aws.output}/","$(k)"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.jar.bucket}/${JAR_NAME}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.output}/ --recursive --include "*"

# Download logs from S3
download-logs:
	mkdir ${local.logs}
	aws s3 sync s3://${aws.log.dir} ${local.logs}

# Get csv from S3 output
csv:
	aws s3 sync s3://${aws.output}/phase2output output_csv
	cat output_csv/part-r-* > output_csv/result.csv




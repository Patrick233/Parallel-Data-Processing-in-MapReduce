all: build run

build: compile jar

MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:out.

compile:
	javac -cp $(MY_CLASSPATH) src/*.java

jar:
	cd src; jar cvf  NeighborhoodScore.jar  *.class

run:
	cd src; hadoop jar NeighborhoodScore.jar NeighborhoodScore /randomBooks /output7 /output8

clean:
	hdfs dfs -rm -r /output*

setup:
	hdfs dfs -mkdir /randomBooks
	hdfs dfs -put input/big-corpus/* /randomBooks

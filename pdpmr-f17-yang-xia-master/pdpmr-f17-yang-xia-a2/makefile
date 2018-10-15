HADOOP_HOME = /opt/hadoop-2.8.1
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:out:.


all: build run

build: compile jar


compile:
   javac -cp $(MY_CLASSPATH) -d out src/*.java

jar:
   jar cvf  NeighborhoodScoreHadoop.jar *.class

run:
   hadoop jar NeighborhoodScoreHadoop.jar NeighborhoodScore test output7 output8

clean:
	hdfs dfs -rm -r output*

setup:
	hdfs dfs -mkdir randomBooks
	hdfs dfs -put input/big-corpus/* randomBooks
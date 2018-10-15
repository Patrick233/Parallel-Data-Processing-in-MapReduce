HADOOP_VERSION=2.8.1
SCALA_HOME = /Users/Patrizio/Downloads/scala-2.11.8
MY_CLASSPATH=${HADOOP_HOME}/share/hadoop/common/hadoop-common-${HADOOP_VERSION}.jar:${SPARK_HOME}/jars/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:out:.
SPARK_HOME=//Users/Patrizio/Desktop/jars/spark-2.2.0-bin-hadoop2.7
CLASSNAME=songsCount

PROJECT_BASE=src/main/scala
INPUT_FOLDER=input
OUTPUT_FOLDER=output
JAR_NAME=MusicFedility.jar
MAIN_CLASS=songCount

all: build run report

build:
	${SCALA_HOME}/bin/scalac -cp ${MY_CLASSPATH} -d ${JAR_NAME} ${PROJECT_BASE}/*.scala

run:
	$(SPARK_HOME)/bin/spark-submit --class $(CLASSNAME) $(JAR_NAME)

report:
	Rscript -e "rmarkdown::render('Report.Rmd')"

clean:
	rm -rf out

gzip:
	-gzip input/MillionSongSubset/*

gunzip:
	-gunzip input/MillionSongSubset/*


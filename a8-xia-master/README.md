

# To run the code:
* make : build, run and report.<br>
* make build : compiles the file and builds the jar.<br>
* make run : runs the files.<br>
* make report : generates the report. <br>
* Change SPARK_HOME to your local SPARK_HOME. <br>
* Change SCALA_HOME to your local SCALA_HOME, make sure it’s in version 2.11.8 <br>
* Change ClassPath if needed to point to location of jars folder of spark on your machine. <br>
* Unzip the input files using:<br>make gunzip, ignored if already unzipped.<br>
* To remove previously created folders by the code, do:<br>make clean<br>
* If the report is not rendering:<br>
in macOs - export RSTUDIO_PANDOC=/Applications/RStudio.app/Contents/MacOS/pandoc/
in linux - export RSTUDIO_PANDOC=/usr/lib/rstudio/bin/pandoc

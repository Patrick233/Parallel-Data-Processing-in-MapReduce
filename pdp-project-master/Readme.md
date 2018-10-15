1. install sbt
take macOS for example: 
Enter the following line at a Terminal prompt:
	/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" 
	brew install sbt 
2. query file should be specified as `QUERY_FILE` at makefile line 2
3. output file should be specified as `OUTPUT_FILE` at makefile line 5
4. run "make predict" to making predictions

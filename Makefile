all:
	hdfs dfs -rm /hw4Input/*
	hadoop com.sun.tools.javac.Main AffineGap.java	
	jar cf AffineGap.jar AffineGap*class
	hadoop fs -rm -r /hw4Output/

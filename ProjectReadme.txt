READ_ME:

* DATASET: https://webscope.sandbox.yahoo.com/catalog.php?datatype=r 
  Download R2 dataset and store it in a folder

RUNNING IN ECLIPSE

* After initial set up in Eclipse, create three new java classes, namely:  
	Similarity
 	Prediction 
 	Recommendation

* Copy and paste the code in the respective classes.

*PREPOCESSING AND SIMILARITY:
  Run the Similarity.java file-
  Right click on java file>Run As > Run Configurations>
  Arguments: "train_0.txt" "similarityList_out"

*PREDICT:
  Rut the Prediction.java file-
  Right click on java file>Run As > Run Configurations
  > Arguments: "test_0.txt" "similarityList_out" "test_0" "accuracy_out"

*RECOMMEND:
  Rut the Recommend.java file-
  Right click on java file >Run As > Run Configurations
  > Arguments: "test_0.txt" "similarityList_out""recommend_out"

RUNNING IN CLUSTER
*Download the input files into the local system of cloudera

*open the terminal and conncet to DSBA cluster using the following 
	ssh <username>@dsba-hadoop.uncc.edu

*Create a folder to place all the files under /users/<username> 
	mkdir /users/<username>/<folderName>

*open another terminal to copy files from local system to cluster by using the following scp command
	scp <filename> <local path where file is kept> <username>@dsba-hadoop.uncc.edu:<path of the cluster>

*now open the terminal from which you have connected to the cluster

*copy the input files from cluster to hdfs
	hadoop fs -put <filepath in the cluster>/filename <input path in hdfs>

*Make a directory for the class files.
	$mkdir recommender_classes

*Then, run the following commands for the execution
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <java file>-d recommender_classes -Xlint
	jar -cvf <jar file name> -C recommender_classes/.
	hadoop jar recom.jar org.myorg.<java filename> <input filepath> <output filepath>

*Move the output files on the cluster to hadoop and local machine.



   
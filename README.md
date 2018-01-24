# Spark-based Framework for Big GeoSpatial Vector Data Analytics

This repository is developping a Spark-based framework for big geospatial vector data analytics
using GeoSpark.

## Example
 * edu.gmu.stc.vector.examples.GeoSparkExample


## Notes
 * Maven pom file dependence: the difference between `provider` and `compile`(default) of the scope value
 * Compile jars: `mvn clean install -DskipTests=true`
 * Run Spark-shell: `spark-shell --master yarn --deploy-mode client 
   --jars /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar --num-executors 9 --driver-memory 12g 
   --executor-memory 10g --executor-cores 12`
 * Submit Spark-jobs: `spark-submit --master yarn --deploy-mode client --num-executors 9 
   --driver-memory 12g --executor-memory 10g --executor-cores 12 --class 
   edu.gmu.stc.vector.sparkshell.OverlapPerformanceTest /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar 
   /user/root/data0119/ Impervious_Surface_2015_DC Soil_Type_by_Slope_DC 24 performanceTest_1`
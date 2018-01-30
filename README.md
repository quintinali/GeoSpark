# Spark-based Framework for Big GeoSpatial Vector Data Analytics

This repository is developping a Spark-based framework for big geospatial vector data analytics
using GeoSpark.

## Example
 * edu.gmu.stc.vector.examples.GeoSparkExample


## Notes
 * Maven pom file dependence: the difference between `provider` and `compile`(default) of the scope value
 * Compile jars: `mvn clean install -DskipTests=true`
 * Connect PostgreSQL: ` psql -h 10.192.21.133 -p 5432 -U postgres -W`
 * Run Spark-shell: `spark-shell --master yarn --deploy-mode client 
   --jars /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar --num-executors 9 --driver-memory 12g 
   --executor-memory 10g --executor-cores 12`
 * Submit Spark-jobs: 
    - GeoSpark: `spark-submit --master yarn --deploy-mode client --num-executors 9 
   --driver-memory 12g --executor-memory 10g --executor-cores 12 --class 
   edu.gmu.stc.vector.sparkshell.OverlapPerformanceTest /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar 
   /user/root/data0119/ Impervious_Surface_2015_DC Soil_Type_by_Slope_DC 24 performanceTest_1`
    - STCSpark_Build_Index: `spark-shell --master yarn --deploy-mode client --num-executors 9 --driver-memory 12g --executor-memory 10g --executor-cores 12 --class edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar BuildIndex /home/fei/GeoSpark/config/conf.xml`
    - STCSpark_Overlap: `spark-shell --master yarn --deploy-mode client --num-executors 9 --driver-memory 12g --executor-memory 10g --executor-cores 12 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar OverlapTest /home/fei/GeoSpark/config/conf.xml 20`
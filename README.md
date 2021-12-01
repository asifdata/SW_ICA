## Loading Sweden Weather data into Hive table using Spark and Scala ##

1. **Assuming the input data is present in HDFS location**

2. Config files helps for dynamic Columns mapping and to keep the mapping of input files

3. **Build the jar using maven**

mvn clean install

5. **Upload the jar file built to the cluster driver node location**

6. **Spark job submission for Pressure and Temperature Analysis**

spark-submit --class com.sw.ica.utility.EntryPointInitializer /root/conifg.json --master yarn --deploy-mode cluster --executor memory 1G --num-executors 5 <path to jar>

7. **Git Code Check-in**

cd "local repository path"
git init
git add .
git commit -m First commit
git remote add origin "https://github.com/asifdata/SW_ICA"
git remote -v
git push origin master -f

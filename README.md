# Standalone file format benchmarks

How to run 

```bash
java -cp /home/atr/jars/filebench-1.0.jar:/home/atr/crail-deployment/hadoop-2.7.3/etc/hadoop:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/common/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/common/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/yarn/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/yarn/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/mapreduce/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/mapreduce/*:/home/atr/crail-deployment/hadoop//contrib/capacity-scheduler/*.jar:/home/atr/crail-deployment/apache-hive-2.3.0-bin/lib/*:/home/atr/crail-deployment/spark/jars/*:/home/atr/crail-deployment/spark/extra-jars/* com.github.animeshtrivedi.FileBench.Main $@

# -XX:-TieredCompilation 
#-XX:ReservedCodeCacheSize=2048m 
```

```bash
ORC=0
for i in "$@" ; do 
	if [[ $i == "orcread" ]]; then 
	 ORC=1
	fi 
done 

if [ $ORC -eq 1 ]; then 
	ORCCLASSPATH=/home/atr/crail-deployment/apache-hive-2.3.0-bin/lib/*
else 
	ORCCLASSPATH=""
fi 

java -cp /home/atr/jars/filebench-1.0.jar:/home/atr/crail-deployment/hadoop-2.7.3/etc/hadoop:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/common/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/common/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/hdfs/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/yarn/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/yarn/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/mapreduce/lib/*:/home/atr/crail-deployment/hadoop-2.7.3/share/hadoop/mapreduce/*:/home/atr/crail-deployment/hadoop//contrib/capacity-scheduler/*.jar:$ORCCLASSPATH:/home/atr/crail-deployment/spark/jars/*:/home/atr/crail-deployment/spark/extra-jars/* com.github.animeshtrivedi.FileBench.Main $@

```
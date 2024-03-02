
### Java
export JAVA_HOME="${HOME}/bin/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

### Spark
export SPARK_HOME="${HOME}/bin/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

### Pyspark
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
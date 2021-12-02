#!/bin/bash
spark-submit\
 --master spark://alon-System-Product-Name:7077\
  --class basicelastic.MovingON\
 /home/alon/hive2elastic/target/hive2elastic-1.0-SNAPSHOT-jar-with-dependencies.jar  



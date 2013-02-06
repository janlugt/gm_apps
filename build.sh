#!/bin/bash

# Set the location of Green-Marl
GM_HOME=/Users/janlugt/workspace/Green-Marl

# Generate java sources with Green-Marl
$GM_HOME/bin/gm_comp -t=giraph -o=src/ $GM_HOME/apps/src/avg_teen_cnt.gm
$GM_HOME/bin/gm_comp -t=giraph -o=src/ $GM_HOME/apps/src/conduct.gm
$GM_HOME/bin/gm_comp -t=giraph -o=src/ $GM_HOME/apps/src/hop_dist.gm
$GM_HOME/bin/gm_comp -t=giraph -o=src/ $GM_HOME/apps/src/pagerank.gm
$GM_HOME/bin/gm_comp -t=giraph -o=src/ $GM_HOME/apps/src/sssp.gm

# Clean bin dir
rm -rf bin/*

# Compile java sources
javac src/*.java -d bin/ -cp lib/giraph-0.2-SNAPSHOT-for-hadoop-1.0.2-jar-with-dependencies.jar:lib/hadoop-core-1.0.3.jar
javac src/manual/*.java -d bin/ -cp lib/giraph-0.2-SNAPSHOT-for-hadoop-1.0.2-jar-with-dependencies.jar:lib/hadoop-core-1.0.3.jar

# Extract Giraph to the bin directory so it's added to the jar
cd bin
jar -xf ../lib/giraph-0.2-SNAPSHOT-for-hadoop-1.0.2-jar-with-dependencies.jar
cd ..

# Package the bin dir into a jar
jar cf dist/gm_apps.jar -C bin/ .

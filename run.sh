#!/bin/bash
# Скрипт объединяющий разные этапы в один процесс

echo "Stage PreFilter"
if hadoop fs -test -e prefilter
then
	echo "Remove exists directory prefilter"
	hadoop fs -rm -R prefilter 
	if [ $? -ne 0 ]
	then
		echo "Error delete prefilter"
		exit 1
	fi
fi
spark-submit --py-files common.py filter.py  incoming/*.csv prefilter 
if [ $? -eq 0 ] 
then
	echo "Stage PreFilter OK"
else
	echo "Stage PreFilter FAILED"
	exit 1
fi

echo "Stage copy data from prefilter to staging"
if hadoop fs -test -e staging
then
	echo "Remove exists directory staging"
	hadoop fs -rm -R staging 
	if [ $? -ne 0 ]
	then
		echo "Error delete staging"
		exit 1
	fi
fi
hadoop fs -mkdir staging 
if [ $? -ne 0 ]
then
	echo "Error make directory staging"
	exit 1
fi
hadoop fs -cp prefilter/part-* staging/ 
if [ $? -eq 0 ] 
then
	echo "Stage copy data from prefilter to staging OK"
else
	echo "Stage copy data from prefilter to staging FAILED"
	exit 1
fi

echo "Stage Archive"
if hadoop fs -test -e archive
then
	echo "Remove exists directory archive"
	hadoop fs -rm -R archive 
	if [ $? -ne 0 ]
	then
		echo "Error delete archive"
		exit 1
	fi
fi
spark-submit --jars MultipleOutputByKey.jar --py-files common.py archive.py staging/part-* archive 
if [ $? -eq 0 ] 
then
	echo "Stage Archive OK"
else
	echo "Stage Archive FAILED"
	exit 1
fi

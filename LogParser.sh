#usr/bin/sh

##################################
# Shell script to
#  Copy from nfs to hdfs
#  Run python streaming 
##################################

#Variables
basedir=/root/InstaCartLogProcessor  #Change basedir to Application root dir
hdfs_input_dir=/user/sbalasubramani/InstaCart/Input #Change hdfs folder location
nfs_input_file=$basedir/scripting_challenge_input_file.txt
hdfs_output_dir=/user/sbalasubramani/InstaCart/Output #Change hdfs folder location
nfs_output_dir=$basedir/Output
mapper_file=$basedir/mapper.py
logfile=$basedir/ParserLog_`date +"%m%d%Y-%H:%M:%S"`.log

echo "Start Parsing - $(date +"%m%d%Y-%H:%M:%S")" >> $logfile

echo "Step1 - Copy file from nfs to hdfs" >> $logfile
hadoop fs -copyFromLocal $nfs_input_file $hdfs_input_dir

echo "Step 2 - Remove output hdfs dir if it exists" >> $logfile
hadoop fs -rmr $hdfs_output_dir

echo "Step 3 - Run Python Streaming " >> $logfile
##Run Streaming
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar\
	   -mapper $mapper_file \
	   -input $hdfs_input_dir/scripting_challenge_input_file.txt \
	   -output $hdfs_output_dir

echo "Step 4 - File count check" >> $logfile
#File counts check
nfs_file_count=`cat $nfs_input_file | wc -l`
echo "nfs file count : $nfs_file_count" >> $logfile

hdfs_file_count=`hadoop fs -cat $hdfs_output_dir/* | wc -l`
echo "hdfs file count is : $hdfs_file_count" >> $logfile

if [[ $nfs_file_count -ne $hdfs_file_count ]]; then echo "File Counts donot match" >> $logfile; fi

echo "Step5 - Copy file from hdfs to nfs"
rm -rf $nfs_output_dir/*
hadoop fs -get $hdfs_output_dir/* $nfs_output_dir

echo -e "Order_id\tOrder_date\tUser_Id\tAvg_item_price\tStart_page_url\tError_msg" > $nfs_output_dir/OutPutFile.txt
cat $nfs_output_dir/part-00000 >> $nfs_output_dir/OutPutFile.txt

echo "End Parsing - $(date +"%m%d%Y-%H:%M:%S") " >> $logfile

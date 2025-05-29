

JOBS --> Stage --> task1, task2
     --> Stage --> task3, task4


Driver --> executor1 : core1, core2
       --> executor2 : core3, core4


each core can perform local count operation
then 1 core will collect local count from other cores and calculate global count, 
to calculate local count data shuffling operation is conducted.



Driver : 
1. heart
2. maintain info executors
3. analyzes, distribute & schedule the work


Executor : 
1. execute the code
2. respond to driver with the execution status.


######################################
Transformation

Transformation Types :
1.Narrow Transformation : one partition contribute to only one partition
2.Wide Transformation : one partition contribute to many partitions

##########################################

Actions : 


To trigger the execution we need to call an Action.


This basically executes the plan created by Transformation.

Actions are of three types:
1. view data in console
2. collect data to native language
3. write data to output data sources
######################################################
Spark Lazy Evaluation

Spark will wait till last moment to execute the graph of computation.

This allow spark to optimize, plan and use the resources properly for execution.
#########################################################################################
Spark Session:

The driver process is known as spark session.
It is the entry point for spark execution.
The spark session instance executes the code in the cluster.
And the relation is one to one , ie for 1 spark application we will have 1 spark session instance.


#########################################################
Structured API : DataFrames

1. Dataframe is the most common structured api, represented like a table
2. rows, columns
3. dataframe has schema, which is the metadata for the columns
4. data in dataframes are in partitions
5. dataframes are immutable


######################################
Execution Plan

1. Logical planning
2. physical planning

Logical planning :

Unresolved logical plan --> catalog check --> resolved logical plan --> catalyst optimizer --> optimized logical plan

Physical planning: 
optimized logical plan --> physical plan 1, 2, 3--> cost model --> best physical plan --> cluster execution
########################################################################################################################




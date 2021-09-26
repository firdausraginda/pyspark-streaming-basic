# Pyspark Notes

### Spark Streaming API
spark streaming API handles following things:
- automatic looping between micro-batches
- batch start and end position management
- intermediate state management
- combining result to the previous batch
- fault tolerance and restart management

### Spark Streaming API (DStream) vs Structured Streaming API
| DStream | Structured Streaming API |
| ------ | ------ |
| **RDD** based streaming API | **Dataframe** based streaming API |
| Lacks spark SQL engine optimization | SQL engine optimization |
| No support for **even time semantics** | Support **event time semantics** |
| No future upgrades and enhancements expected | Expected further ehancements and new features

### Typical Structure of a Spark Streaming Application
Spark streaming application consist of 3 steps:
- Read streaming source - input dataframe
- Process and transform input dataframe - output dataframe
- write the output - streaming sink

### Spark Streaming Flow
- Spark driver will take the *read stream*, *transform*, & *write stream* code, and submit those to the spark SQL engine 
- The SQL engine will analyze the code, optimize it, and compiles it to generate an execution plan. This happen in run-time
- Once the execution plan generated, spark will start a **background thread** to execute it
- The background thread will trigger one **spark job** to: **1)** *read the data from the source and crate an input streaming dataframe*, **2)** *process the input dataframe as per defined logic to generate the output dataframe*, **3)** *write the output dataframe to the given sink*. Those 3 steps considered as one **micro-batch** 
- After finish the 3 steps above, background thread will look for the new inputs from the given source. If haven't found any new inputs yet, background job will wait. Once the new input found, the background thread will clean the input dataframe and load it with the new data. And this new input will be processed for the 2nd micro-batch

### Streaming Trigger
- **Unspecified**: new micro-batch is triggered as soon as the current micro-batch finishes, without any time limit. However, the micro-batch will wait for the new input data
- **Time Interval**: has time limits for each micro-batch, can be done using `.trigger(processingTime="1 minute")`. If the current trigger finish less than the time limit, then the next micro-batch will wait for the remaining time, and only trigger when the time limit from the current micro-batch is over. If the current micro-batch takes more than the time limit, then the next micro-batch will start immediately as soon as the current one ends. This time interval gives us option to collect some input data and process it together, rather than processing each record individually
- **One Time**: Will create one and only one micro-batch and then the background process will stop. This works when we want to run spark cluster in cloud then shut down the cluster right after. So in this case, we'll be scheduling job using third party schedular, and the stream processing job acts as a batch job.
- **Continous**: To achieve millisecond latencies. This works when we want to get things in less than a second.

### Spark Streaming Sources
- **Socket source**: allows to read text data from a socket connection. This source is not designed for production usage, but for testing purpose only
- **Rate source**: designed for testing and branchmarking spark cluster. It's a dummy data source which generates a configurable number of key/value pair per second
- **File source**: allows to read data from file with **json** or **parquet** or other format. In deal with file source, we can set the maximum files per trigger using `.option("maxFilesPerTrigger", 1)`. The processed source files in directory will negatively impact the micro-batch, hence needs to be cleaned, using `.option("cleanSource", "delete")`. 

### Spark Jobs
- The spark jobs will start from the **0** job id, which inferring the schema, while the actual micro-batches start from job id **1**
- The **submitted** time between job id 0 to 1, and job id 1 to 2 can be less than the **time limit interval** (if specified), this because spark still trying to align the trigger time to a round off time. But from job id 3 onwards, the duration should be more stable, follows the time limit interval

### Spark Output Modes
- **Append** (insert only): Each micro-batch will **write new recods only**, this works when we don't want to update any previous output
- **Update** (upsert): Will write either the **new records**, or the **old records that wants to be updated**. This works best when we want to implement upsert operation
- **Complete** (overwrite): Will overwrite the complete results: **the old & new records**. So we will always get the entire results

### Fault Tolerance & Restarts
The stream processing application should run forever, and will stop only because these following reasons: **failure** or **maintenance**. 

The stream processing application must be able to handle the stop & restart gracefully. **Restart gracefully** here means restart with **excatly once** feature, this means:
- do not miss any input records
- do not create duplicate output records

Spark structured streaming maintains the state of the micro-batches in the checkpoint location. **Checkpoint location** mainly consist of 
- **read position**: represents the start & end of the date range which is processed by the current micro-batch
- **state information**: the intermediate data for the micro-batch

To be able to restart the application **exactly-once**, needs to follow these requirements:
- restart with the same checkpoint
- use a replayable source: use data source that allows us to re-read the incomplete micro-batch data
- make sure the application logic produces the same results when given the same input data
- the application should be able to identify the duplicates, and take action upon it. Either to ignore it, or update the older copy of the same record
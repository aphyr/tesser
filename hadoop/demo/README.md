# tesser.hadoop.demo

A simple project that runs a character-counting fold. Take a look at
project.clj and src/tesser/hadoop/demo/core.clj for the code, since the
packaging requirements for a Parkour project are a little tricky to get right.
This project.clj is set up for a Hadoop 2.0 Cloudera 4 cluster; you'll probably
want to choose the Hadoop client version appropriate for your environment.

## Usage

Before working with Tesser and Hadoop, make sure that `hadoop jar whatever.jar`
works correctly. You'll probably need to authenticate by running `kinit`.

We need some text files in HDFS to read, and a temporary directory in HDFS to write results to. First, we'll make sure that directory is empty:

```
hadoop fs -rm -r /tmp/kingsbury/demo
```

Then, we'll package the demo project as a fat jar:

```
lein do clean, uberjar
```

Then we'll run that jar in Hadoop against our temp dir and input files:

```
hadoop jar \
  target/tesser-hadoop-demo-0.1.0-SNAPSHOT-standalone.jar \
  hdfs:/tmp/kingsbury/demo \
  hdfs:/data/foo/0_1/part-*
```

Tesser automatically generates a short random job name and submits it to the
cluster. Stderr looks like:

```
15/01/08 15:40:48 INFO parkour.graph: Launching job audience-analysis-262[1/1]
15/01/08 15:40:48 WARN mapred.JobClient: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.
15/01/08 15:40:48 INFO hdfs.DFSClient: Created HDFS_DELEGATION_TOKEN token 193994 for kingsbury on ha-hdfs:dev
15/01/08 15:40:48 INFO security.TokenCache: Got dt for hdfs://dev; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:dev, Ident: (HDFS_DELEGATION_TOKEN token 193994 for kingsbury)
15/01/08 15:40:48 INFO input.FileInputFormat: Total input paths to process : 722
15/01/08 15:40:48 INFO lzo.GPLNativeCodeLoader: Loaded native gpl library
15/01/08 15:40:48 INFO lzo.LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev d0f5a10f99f1b2af4f6610447052c5a67b8b1cc7]
15/01/08 15:46:16 INFO parkour.graph: Job audience-analysis-262[1/1] succeeded
15/01/08 15:46:16 INFO hdfs.DFSClient: Created HDFS_DELEGATION_TOKEN token 193997 for kingsbury on ha-hdfs:dev
15/01/08 15:46:16 INFO security.TokenCache: Got dt for hdfs://dev; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:dev, Ident: (HDFS_DELEGATION_TOKEN token 193997 for kingsbury)
15/01/08 15:46:16 INFO input.FileInputFormat: Total input paths to process : 1
```

And stdout: a frequency distribution of characters in the input text.

```
([\space 1946238846]
 [\0 1797333149]
 [\1 1325564417]
 [\4 1249022283]
 [\8 1123651791]
 [\2 1122908302]
 [\3 1120085668]
 [\e 1102461872]
 [\5 1102221964]
 [\7 1097686896]
 [\6 1091183208]
 [\9 1072278238]
 [\: 789290251]
 [\n 709239137]
 [\, 690404976]
 [\a 690187809]
 [\t 680228177]
 [\i 664018328]
 [\l 602011267]
 [\- 519792746]
 [\s 515397081]
 [\c 453300270]
 [\_ 430756037]
 ...
```

## License

Copyright © 2015 Kyle Kingsbury

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

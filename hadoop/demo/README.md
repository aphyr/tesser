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
  hdfs:/data/foo/2014.09.30_10.28a/0_1/part-0000*
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
 [\o 418699474]
 [\. 405119626]
 [\r 403810901]
 [\" 399935776]
 [\d 361554456]
 [\p 355709259]
 [\u 287261598]
 [\m 262743552]
 [\g 253047244]
 [\} 212606496]
 [\{ 212606496]
 [\f 151244048]
 [\b 109072312]
 [\y 97829141]
 [\[ 97153768]
 [\] 97153720]
 [\w 89347501]
 [\h 78798317]
 [\k 38598971]
 [\v 37026465]
 [\x 22149637]
 [\) 12401561]
 [\( 12401552]
 [\z 7231806]
 [\j 6745911]
 [\q 5299506]
 [\+ 669563]
 [\ا 430627]
 [\# 401887]
 [\tab 401618]
 [\/ 368009]
 [\ل 285537]
 [\é 230055]
 [\ي 225045]
 [\省 222633]
 [\市 221187]
 [\ر 173232]
 [\ã 170165]
 [\ة 164241]
 [\ب 149914]
 [\= 142856]
 [\울 140087]
 [\á 139910]
 [\서 138030]
 [\م 137708]
 [\و 113287]
 [\ó 110128]
 [\د 108566]
 [\í 108094]
 [\ن 101646]
 [\北 88766]
 [\도 85109]
 [\; 80354]
 [\о 64007]
 [\ك 63692]
 [\경 62782]
 [\ق 60389]
 [\е 53982]
 [\県 53599]
 [\东 53372]
 [\南 51630]
 [\기 50271]
 [\新 50218]
 [\' 48162]
 [\江 47323]
 [\` 45347]
 [\京 44828]
 [\广 44436]
 [\س 42704]
 [\ح 42630]
 [\с 42240]
 [\台 41861]
 [\ش 41690]
 [\河 39495]
 [\ร 37975]
 [\أ 37001]
 [\山 36404]
```

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

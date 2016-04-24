Hive JSON Schema Generator
=============================

## Overview

This is a first-pass prototype that generates a Hive schema from JSON files (one JSON object per line).

There are two other tools that generate Hive schemas from JSON:

* https://github.com/hortonworks/hive-json - Owen O'Malley of Hortonworks - c. 2013
* https://github.com/quux00/hive-json-schema - Michael Peterson - c. 2013. I added many of my features in a fork:
https://github.com/amesar/hive-json.

When processing deeply nested and complex JSON such as Twitter tweets and [Github Archive](https://www.githubarchive.org/) formats, both of these tools had hiccups. Since I had previously done work with JSON schema discovery, I wanted to give Hive schema generation a shot. 

Though I addressed some the hiccups of the other tools, there are my hiccups too which need to be looked at. Inferring schema from deeply nested and dirty real-world JSON feeds at scale is not an easy task.

### Features

* Expects JSON files to have one JSON object per line
* Escapes Hive reserved keywords with back ticks
* Escapes JSON attributes that start with underscore since Hive doesn't allow such column names 
* Escapes JSON attribute names that only contain numeric characters (yep, tweets have them!)
* Can specify table name
* Can specify SERDE and LOCATION
* Rudimentary handling of polympormism. When an attribute's value appears as both Int and Bigint (Long) in the data feed, its type becomes a Bigint. If we find Int and String, we generate a Union.
* Empty arrays and null values appear as comments in the DDL
* Handles .gz files

### SerDe 
You need to make a choice as to which JSON SerDe to use:

  * Hcatlog SerDe - org.apache.hcatalog.data.JsonSerDe. Comes with the Hive distribution.
  * https://github.com/rcongiu/Hive-JSON-Serde 
  * https://github.com/cloudera/cdh-twitter-example. There is a SerDe baked into this example that is quite robust.

All had problems processing complex JSON files. It seems the Cloudera SerDe had the least issues.

## Sample JSON and Schemas

Sample JSON feeds and schemas:

* [tweets.json](json/tweets.json) - [tweets.ddl](schemas/tweets.ddl)
* [tweets_v1.json](json/tweets_v1.json) - [tweets_v1.ddl](schemas/tweets_v1.ddl)

Tweet DDL Snippet
```
CREATE EXTERNAL TABLE tweets (
  createdAt string,
  currentUserRetweetId tinyint,
  id bigint,
  userMentionEntities  array <
     struct <
      name: string,
      screenName: string,
      id: bigint,
      start: int,
      `end`: int
    >
  >,
  mediaEntities  array <
     struct <
      id: bigint,
      url: string,
      mediaURL: string,
      mediaURLHttps: string,
      expandedURL: string,
      displayURL: string,
      sizes: struct <
        `0`: struct <
          width: int,
          height: int,
          resize: int
        >,

  -- coordinates null,
  -- hashtags  array <>,
  ...
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/tables/tweets'
```

## Run
```
java -cp target/hive-json-schema-gen-1.0-SNAPSHOT.jar \
  org.amm.hiveschema.HiveJsonSchemaDriver \
  --table tweets \
  --location /tables/tweets \
  --serde com.cloudera.hive.serde.JSONSerDe \
  --isExternalTable \
  --escapeReservedKeywords \
  json/tweets.json
```

Convenience script:
```
build-schema.sh tweets /tables/tweets data/tweets.json

build-schema.sh tweets_v1 /tables/tweets_v1 data/tweets_v1.json
```

## Build 

```
mvn package
```

## Options 

* --table:  table name in schema. Required.
* --isExternalTable: Create external table.
* --location: HDFS path.
* --serde: Desired SerDe class path.
* --reservedKeywordsFile: Override default Hive reserved keyword list is in src/main/resources/reservedKeywords.txt.
* --escapeReservedKeywords: Escape Hive reserved keywords with a back tick. Default is not to escape.
* --output: Override output file name (table name plus ".ddl")


## Feature Details


### Reserved Keywords
JSON
```
  "user" : {
```

Schema
```
    `user`: struct <
```

### Hive identifiers cannot start with underscore
JSON
```
  "_name" : "hello"
```

Schema
```
    `_name`: string
```


### Numeric JSON attribute names

JSON
```
  "sizes" : {
    "0" : {
      "width" : 150,
      "height" : 150,
      "resize" : 101
    },
```

Schema
```
  sizes: struct <
    `0`: struct <
      width: int,
      height: int,
      resize: int
    >,
```


### Hive identifiers cannot start with underscore
JSON
```
  "_name" : "hello"
```

Schema
```
  `_name`: string
```

### Numeric JSON attribute names

JSON
```
  "sizes" : {
    "0" : {
      "width" : 150,
      "height" : 150,
      "resize" : 101
    },
```

Schema
```
  sizes: struct <
    `0`: struct <
      width: int,
      height: int,
      resize: int
    >,
```

### Polymorphism
JSON
```
  { "retweet_count" : "100+", }
  { "retweet_count" : 1, }

```

Schema
```
  retweet_count uniontype<string,int>,
```

In Twitter API 1 the retweet_count attribute was deliberately polymorphic - its value could be either an int or string! In API 1.1 it is fortunately only an int. 

From [https://dev.twitter.com/overview/api/tweets](https://dev.twitter.com/overview/api/tweets): 
> Number of times this Tweet has been retweeted. This field is no longer capped at 99 and will not turn into a String for "100+"

## TODO

* Read in reserved keyword file as a resource
* Option to lexically sort DDL attributes
* Pluggable polymorphism handlers
* Unit tests 

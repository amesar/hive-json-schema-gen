
table=$1
location=$2
file=$3

serde=org.apache.hcatalog.data.JsonSerDe
serde=org.openx.data.jsonserde.JsonSerDe
serde=com.cloudera.hive.serde.JSONSerDe
OPTS="$OPTS --serde $serde"

OPTS="$OPTS --isExternalTable"
OPTS="$OPTS --escapeReservedKeywords"

JAR=target/hive-json-schema-gen-1.0-SNAPSHOT.jar
PGM=org.amm.hiveschema.HiveJsonSchemaDriver
java -cp $JAR $PGM -t $table -l $location $OPTS $file 

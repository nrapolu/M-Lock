rm wal.jar
cd target/classes
jar cf wal.jar ham/wal/*.class ham/wal/regionobserver/*.class
cp wal.jar ../../.
scp wal.jar carpediem.cs.purdue.edu:tmp/ham/.
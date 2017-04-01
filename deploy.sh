rm app.tar*
tar cvf app.tar run-app.sh target/word-count-test-java-1.0.jar pom.xml ./SGDvoc.txt
gzip app.tar
docker ps |grep hadoop-master |awk '{print $1}' |xargs -I % docker cp ./app.tar.gz %:/root

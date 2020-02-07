

### Compile

    ./gradlew clean build

### Run

Either:

     ./gradlew :pxf-service:bootRun

Or:

     java -jar pxf-service/build/libs/pxf-service-<VERSION>.jar

### Dependency tree

    ./gradlew <project-name>:dependencies

For example:

    ./gradlew pxf-api:dependencies

### Custom log configuration file path

    java -jar -Dlogging.config="file:///<PXF_CONF>/pxf/conf/pxf-log4j2.xml" pxf-service/build/libs/pxf-service-<VERSION>.jar

# Atlas-Custom-Entities

1. Create custom Type in Atlas with name ResearchPaperAccessDataset, RecommendationResults and Special process of type ResearchPaperMachineLearning using below curl command
 Simple/LDAP/AD Authentication:
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_ResearchPaperDataSet.json
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_RecommendationResults.json
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_process_ML.json

 Kerberos Authentication: 
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' --negotiate -u: 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_ResearchPaperDataSet.json
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' --negotiate -u: 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_RecommendationResults.json
   curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' --negotiate -u: 'http://<AtlasMetaDataServer_Host>:21000/api/atlas/types' -d @atlas_type_process_ML.json


2. After creating the above types, we will push Entities and Process related information using CustomHookKafka.java
  2.1: Compile CustomHookKafka.java as below from the host where you have Atlas Client package installed.
       $JAVA_HOME/bin/javac -cp .:`hadoop classpath`:/usr/hdp/<Version>/atlas/server/webapp/atlas/WEB-INF/lib/*: CustomHookKafka.java
  2.2: Create atlas-application.properties, you may copy from /etc/hive/conf/atlas-application.properties to dir where Java code is present and replace  atlas.hook.hive to atlas.hook.custom
  2.3 Run Java code after compilation
       $JAVA_HOME/bin/java -cp .:`hadoop classpath`:/usr/hdp/<Version>/atlas/server/webapp/atlas/WEB-INF/lib/*: CustomHookKafka

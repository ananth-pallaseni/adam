@setlocal
set JAVA_HOME=C:\Program Files\Java\jdk1.7.0_13
set M2_HOME=C:\Program Files (x86)\apache-maven-3.1.1
set M2=%M2_HOME%\bin
set PATH=%M2%;%PATH%
set MAVEN_OPTS=-Xmx1024m -XX:MaxPermSize=1024m
mvn.bat -X clean package 2>&1 | C:\NTUtil\BinU\tee.exe Build.log
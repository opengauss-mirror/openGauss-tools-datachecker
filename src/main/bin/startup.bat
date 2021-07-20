@echo off
@if not "%ECHO%" == ""  echo %ECHO%
@if "%OS%" == "Windows_NT"  setlocal

set ENV_PATH=.\
if "%OS%" == "Windows_NT" set ENV_PATH=%~dp0%

set conf=%ENV_PATH%\..\conf
set gauss_conf=%conf%\gauss.properties
set logback_configurationFile=%conf%\logback.xml
set classpath=%conf%\..\lib\*;%conf%
set JAVA_OPTS= -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dapplication.codeset=UTF-8 -Dfile.encoding=UTF-8 -Xms128m -Xmx512m -XX:PermSize=128m -XX:+HeapDumpOnOutOfMemoryError -DappName=DataChecker -Dlogback.configurationFile="%logback_configurationFile%" -Dgauss.conf="%gauss_conf%"

set COMMAND= java %JAVA_OPTS% -classpath "%classpath%" com.gauss.GaussLauncher
echo %COMMAND%
java %JAVA_OPTS% -classpath "%classpath%" com.gauss.GaussLauncher
cmd /k
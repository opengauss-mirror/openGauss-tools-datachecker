1. # openGauss-tools-datachecker

   #### Introduction

   DataChecker is a tool written in Java for checking data consistency between two databases. After migrating a large amount of data from one database to another, you need to check whether the migrated data is accurate and complete. In this case, you can use DataChecker to check whether the data in the two databases is consistent.

   #### Verification Principle

   Perform a FULL JOIN on the checksum values of corresponding tables across both source and target databases to generate validation results. For example, if you want to verify table A in the MySQL database and table A in the openGauss database, you can use the hash function (MD5) to calculate the hash value of each record in table A in the two databases, and then perform a FULL JOIN on the hash values to obtain the verification result.

   #### Implementation Architecture

   The implementation framework of the tool consists of four parts:

   1. Preparer: calculates the checksum value of the table in the openGauss database and inserts the value into the new table checksumB.

   2. Extractor: calculates the checksum value of the table in the Oracle, MySQL, or PostgreSQL database and extracts the value.

   3. Applier: inserts the checksum value extracted from the Extractor into the new table checksumA in the openGauss database.

   4. Performs a FULL JOIN on checksumA and checksumB to obtain the verification results and search for the specific mismatched records.

   #### Environment Requirements

   ##### OS

   Java-based development with BAT and shell scripts, compatible with Windows and Linux.

   JDK 1.6.25 or later is recommended.

   ##### Database

   The source database supports MySQL and Oracle and will support PostgreSQL in the future.

   The target database supports only openGauss.

   #### Installation

   ```
   ① Download the source code.
   git clone git@gitee.com:opengauss/openGauss-tools-datachecker.git
   ② Access the root directory.
   cd openGauss-tools-datachecker
   ③ Build the project.
   mvn clean install -Dmaven.test.skip -Denv=release
   ```

   #### Instruction

   1. Once the build is complete, the target folder is generated in the root directory. `DataChecker-1.0.0-RELEASE.tar.gz` in the target folder is the required package.

   2. Decompress `DataChecker-1.0.0-RELEASE.tar.gz`.

   3. Modifying `/conf/ gauss.properties`.

      | Parameter                         | Description                                                    | Default Value             |
      | --------------------------------- | ------------------------------------------------------------ | ------------------- |
      | gauss.database.source.username    | Specifies the username for accessing the source database.                                            | None                 |
      | gauss.database.source.password    | Specifies the password for accessing the source database.                                              | None                 |
      | gauss.database.source.type        | Specifies the type of the source database.                                              | Mysql               |
      | gauss.database.source.url         | Specifies the URL for connecting to the source database. The URL must comply with certain format requirements.                         | None                 |
      | gauss.database.source.encode      | Specifies the encoding format of the source database.                                          | UTF-8               |
      | gauss.database.target.username    | The username for accessing the destination database.                                          | None                 |
      | gauss.database.target.password    | Specifies the password for accessing the target database.                                            | None                 |
      | gauss.database.target.type        | Specifies the type of the target database.                                            | OPGS (openGauss)|
      | gauss.database.target.url         | Specifies the URL for connecting to the target database. The URL must comply with certain format requirements.                       | None                 |
      | gauss.database.target.encode      | Specifies the encoding format of the target database.                                            | UTF-8               |
      | gauss.table.onceCrawNum           | Specifies the maximum number of records processed by Extractor or Applier in each batch.                     | 1000                |
      | gauss.table.tpsLimit              | Specifies the limit on transactions per second (TPS). The value 0 indicates that TPS is not limited.                                        | 0                   |
      | gauss.table.white                 | Whitelist. Specifies the table to be verified. The format of the value is schema.tablename. Multiple table names can be separated by commas (,). To verify all tables in a schema, you only need to enter the schema name.| None                 |
      | gauss.table.black                 | Blacklist. Specifies the blacklist that lists the tables to be ignored. The format of the value is the same as that specified by gauss.table.white.               | None                 |
      | gauss.table.concurrent.enable     | Specifies whether to enable parallel processing for multiple tables. If the value is false, serial processing is required.       | true                |
      | gauss.table.concurrent.size       | Specifies the number of tables that can be concurrently processed.                                          | 5                   |
      | gauss.table.query_dop             | Specifies the query_dop parameter in openGauss, which is used to control the number of concurrent threads to be executed.        |                     |
      | gauss.extractor.concurrent.global | Extractor adopts the global thread pool mode. If the value is true, all Extractor tasks use a group of thread pools. The thread pool size is specified by concurrent.size.| false               |
      | gauss.extractor.concurrent.size   | Specifies the number of threads that can be concurrently processed. This parameter takes effect only after concurrent.enable is enabled.| 30                  |
      | gauss.applier.concurrent.enable   | Specifies whether parallel processing is enabled for Applier.                                     | true                |
      | gauss.applier.concurrent.global   | Applier adopts the global thread pool mode. If the value is true, all Applier tasks use a group of thread pools. The thread pool size is specified by concurrent.size.| false               |
      | gauss.applier.concurrent.size     | Specifies the number of threads that can be concurrently processed. This parameter takes effect only after concurrent.enable is enabled.| 30                  |
      | gauss.stat.print.interval         | Specifies the frequency of printing statistical information. If the value is 5, statistical information is printed once after five rounds of Extractor and Applier operations are complete.| 5                   |

   4. Start up and stop.

      ```
      #Linux:
      sh bin/startup.sh (start up)
      sh bin/stop.sh (stop)
      
      #Windows:
      Double-click bin/startup.bat (start up).
      Close the window directly (stop).
      ```

      #### Log Description

      The log structure is as follows:

      ```
      /logs
      	summary/    
      		summary.log
      	gauss/      
      		table.log 
      	${table}/           
              table.log
              extractor.log
              applier.log
              check.log   
           
      The `table.log` file in the `gauss` directory records all logs in the entire verification process.
      The `summary.log` file in the `summary` directory records the names of all tables whose verification results are incorrect. That is, the data in the two tables is inconsistent.
      `${table}` indicates the name of each table. The `table.log` file records the verification process of the table, the `extractor.log` file records the checksum extraction process, and the `applier.log` file records the checksum insertion process. The `check.log` file records the data of a specific row that fails the verification. If the `check.log` file does not exist, the verification result is correct.
      ```

   

Contributions

   1. Fork this repository.
   2. Create a Feat_xxx branch.
   3. Commit code.
   4. Create a pull request (PR).

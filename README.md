1. # openGauss-tools-datachecker

   #### 介绍

   DataChecker是一个用Java语言编写的检验两个数据库间数据一致性的工具，一般情况下用于数据迁移完成之后的正确性校验。在将大量数据从一个数据库迁移至另一个数据库后，往往需要确定迁移过去的数据是否正确、完整。DataChecker就是用来检验两边数据库中的数据是否一致的工具。

   #### 校验原理

   简单地说，就是对两边数据库对应的表的checksum值做full join，得出校验结果。举个例子，假设要校验mysql端的表A以及openGauss端的表A，先用哈希函数（MD5）分别算出两端的表A里每一条记录的哈希值，再对两边的哈希值做full join，得出校验结果。

   #### 实现架构

   整个工具的实现框架分为4个部分：

   1.Preparer：计算出openGauss端的表的checksum值，并插入一个新建的表checksumB。

   2.Extractor：计算出Oracle/Mysql/Postgresql端的表的checksum值, 并提取出来。

   3.Applier：将Extractor中提取出来的checksum值，插入openGauss端的一个新建的表checksumA。

   4.让checksumA和checksumB做full join操作，得出校验结果并反向查找出具体的不匹配记录。

   #### 环境要求

   ##### 操作系统

   纯java开发，有bat和shell脚本，windows/linux均可支持。

   jdk建议使用1.6.25以上的版本。

   ##### 数据库

   源库支持mysql、Oracle，后续将增加对Postgresql的支持。

   目标库仅支持openGauss数据库。

   #### 安装教程

   ```
   ① 下载源码：
   git clone git@gitee.com:opengauss/openGauss-tools-datachecker.git
   ② 进入根目录
   cd openGauss-tools-datachecker
   ③ 编译
   mvn clean install -Dmaven.test.skip -Denv=release
   ```

   #### 使用说明

   1. 编译后根目录下会出现target文件夹，target文件夹中的```DataChecker-1.0.0-RELEASE.tar.gz```即为所需要的包。

   2. 解压```DataChecker-1.0.0-RELEASE.tar.gz```

   3. 修改配置文件```/conf/ gauss.properties ```

      | 参数名字                          | 参数说明                                                     | 默认值              |
      | --------------------------------- | ------------------------------------------------------------ | ------------------- |
      | gauss.database.source.username    | 源数据库的用户名                                             | 无                  |
      | gauss.database.source.password    | 源数据库的密码                                               | 无                  |
      | gauss.database.source.type        | 源数据库的类型                                               | Mysql               |
      | gauss.database.source.url         | 源数据库的连接url，须遵循一定的格式                          | 无                  |
      | gauss.database.source.encode      | 源数据库的编码格式                                           | UTF-8               |
      | gauss.database.target.username    | 目标数据库的用户名                                           | 无                  |
      | gauss.database.target.password    | 目标数据库的密码                                             | 无                  |
      | gauss.database.target.type        | 目标数据库的类型                                             | OPGS（即openGauss） |
      | gauss.database.target.url         | 目标数据库的连接url，须遵循一定的格式                        | 无                  |
      | gauss.database.target.encode      | 目标数据库的编码                                             | UTF-8               |
      | gauss.table.onceCrawNum           | extractor/applier每个批次最多处理记录数                      | 1000                |
      | gauss.table.tpsLimit              | tps限制，0代表不限制                                         | 0                   |
      | gauss.table.white                 | 白名单。定义需要进行校验的表:   格式为schema.tablename组成，多个表可加逗号分隔。如想要校验一个schema下面的所有表，则填schema的名字即可 | 无                  |
      | gauss.table.black                 | 黑名单，需要忽略的表。格式同gauss.table.white                | 无                  |
      | gauss.table.concurrent.enable     | 多张表之前是否开启并行处理，如果false代表需要串行处理        | true                |
      | gauss.table.concurrent.size       | 允许并行处理的表数                                           | 5                   |
      | gauss.table.query_dop             | opengauss中的query_dop参数，用于控制执行的并行线程数         |                     |
      | gauss.extractor.concurrent.global | extractor是启用全局线程池模式,如果true代表所有extractor任务都使用一组线程池,线程池大小由concurrent.size控制 | false               |
      | gauss.extractor.concurrent.size   | 允许并行处理的线程数，需要先开启concurrent.enable该参数才会生效 | 30                  |
      | gauss.applier.concurrent.enable   | applier是否开启并行处理                                      | true                |
      | gauss.applier.concurrent.global   | applier是启用全局线程池模式,如果true代表所有applier任务都使用一组线程池,线程池大小由concurrent.size控制 | false               |
      | gauss.applier.concurrent.size     | 允许并行处理的线程数，需要先开启concurrent.enable该参数才会生效 | 30                  |
      | gauss.stat.print.interval         | 统计信息打印频率.    频率为5，代表，完成5轮extract/applier后，打印一次统计信息 | 5                   |

   4. 启动及停止

      ```
      #Linux下:
      sh bin/startup.sh（启动）
      sh bin/stop.sh (停止)
      
      #windows下:
      双击bin/startup.bat(启动)
      直接关掉窗口（停止）
      ```

      #### 日志说明

      日志结构为： 

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
           
      gauss目录下的table.log为整个校验过程的总日志。
      summary目录下的summary.log记录了所有校验结果为不正确的表名（即两边表的数据不一致）。
      ${table} 为各个表的表名，其下的table.log为该表的校验过程总日志，extractor.log为checksum提取过程的总日志，applier.log为插入checksum过程中的总日志。check.log记录了校验失败的具体某一行的数据。如没有出现check.log，则表示校验结果为正确。
      ```

   

参与贡献

1. Fork 本仓库
   2. 新建 Feat_xxx 分支
   3. 提交代码
   4. 新建 Pull Request

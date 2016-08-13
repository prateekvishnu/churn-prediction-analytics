churn-prediction-analytics
--------------------------

This repository is about identifying potential churning subscribers by analysing their call behavior
patterns and correlating the patterns with already churned subscribers. 
It employs Supervised machine learning and data science approach for developing this system. 
We used CDRs as the source for our training and test dataset.

Pre-requisites
--------------
Following is the list of softwares required for accomplishing the tasks.....
1. Apache Maven
2. Eclipse
3. Git
4. Hadoop binary for Windows.
5. Spark

How to install Hadoop Binaries for Windows?
---------------

If you don't have a local Hadoop install on Windows you can download winutils.exe and then: 

1. Create a Hadoop home directory
2. Place winutils.exe in a bin directory under that Hadoop home directory   
3. Set system variable HADOOP_HOME = C:\Users\<username>\Hadoop    
(File location C:\Users\<username>\Hadoop\bin\winutils.exe)
4. Restart Eclipse.

DOWNLOAD LINK: http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe

How to install?
---------------
1. Clone the repo on your system.
2. Open the command line at the repo root folder:
    $> maven eclipse:eclipse
    $> maven compile
3. Open the project inside eclipse and start coding and running the application from there.



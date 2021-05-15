REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic simple_invoices
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic loyalty_notifications
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic invoices_flat
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic invoices_agg
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic market_trades
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic sensor_topic
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic logins
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic impressions
REM %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic clicks
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic rates
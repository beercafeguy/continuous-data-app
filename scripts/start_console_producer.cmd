REM %KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9093 --topic market_trades
REM %KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9093 --topic sensor_topic --property "parse.key=true" --property "key.separator=:"
%KAFKA_HOME%\bin\windows\kafka-console-producer.bat --broker-list localhost:9093 --topic logins
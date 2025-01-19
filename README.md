# 0mqLoggerTest
personal debugging tool for capturing logs using zmq.PUSH to forward logs to central collector that can print out and write to a sqlite.db

currently this is an mvp to test how well i can make this work. i would like to convert this to rust or c++ for better deployability one day

i want to write a formal contract/unprotocol for this soonish, which will be found here

right now i am getting ~500msg/s. this is slow and could be improved
things that should be done to improve performance 
- Batch inserts instead of individual inserts
- Use write-ahead logging (WAL) mode for SQLite
- Reduce transaction frequency
- Buffer messages before processing

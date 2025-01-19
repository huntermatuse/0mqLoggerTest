import zmq
import json
import logging
from datetime import datetime, timezone, UTC
import signal
import sys
import sqlite3
from pathlib import Path
from typing import Optional
from contextlib import contextmanager


class DatabaseManager:
    SCHEMA = '''
    CREATE TABLE IF NOT EXISTS Events(
        Timestamp INTEGER NOT NULL,
        Loglevel TEXT NOT NULL COLLATE NOCASE,
        Source TEXT NOT NULL COLLATE NOCASE,
        Category TEXT COLLATE NOCASE,
        Message TEXT NOT NULL COLLATE NOCASE
    );
    
    CREATE INDEX IF NOT EXISTS Category_index ON Events (Category COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS Loglevel_index ON Events (Loglevel COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS Source_index ON Events (Source COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS Timestamp_index ON Events (Timestamp);
    '''
    
    MAX_SIZE_BYTES = 15 * 1024 * 1024  # 15MB in bytes
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize the database with the schema"""
        with self.get_connection() as conn:
            conn.executescript(self.SCHEMA)
    
    def _get_db_size(self) -> int:
        """Get the current size of the database file in bytes"""
        return Path(self.db_path).stat().st_size
    
    def _remove_oldest_entries(self, conn: sqlite3.Connection, num_entries: int = 1000):
        """Remove the oldest entries from the database"""
        conn.execute('''
            DELETE FROM Events 
            WHERE Timestamp IN (
                SELECT Timestamp 
                FROM Events 
                ORDER BY Timestamp ASC 
                LIMIT ?
            )
        ''', (num_entries,))
        conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def insert_event(self, timestamp: int, loglevel: str, source: str, 
                    category: Optional[str], message: str):
        """Insert a new event into the database with size management"""
        with self.get_connection() as conn:
            current_size = self._get_db_size()
            
            if current_size >= (self.MAX_SIZE_BYTES * 0.9):  # 90% of limit
                self._remove_oldest_entries(conn)
                
                conn.execute("VACUUM")
                
                if self._get_db_size() >= self.MAX_SIZE_BYTES:
                    raise sqlite3.Error("Database size limit reached and cleanup unsuccessful")
            
            conn.execute('''
                INSERT INTO Events (Timestamp, Loglevel, Source, Category, Message)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, loglevel, source, category, message))
            conn.commit()

class LogCollector:
    def __init__(self, port: int = 18044, db_path: str = "logs.db", quiet: bool = False):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.running = True
        self.port = port
        self.quiet = quiet
        
        self.db = DatabaseManager(db_path)
        
        self._setup_logging()
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self):
        """Setup basic logging configuration"""
        level = logging.WARNING if self.quiet else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logging.warning("Shutdown signal received, stopping collector...")
        self.running = False

    def start(self):
        """Start the log collector"""
        try:
            self.socket.bind(f"tcp://*:{self.port}")
            logging.info(f"Log collector started on port {self.port}")
            
            while self.running:
                try:
                    # Use poll() to allow checking self.running periodically
                    if self.socket.poll(timeout=1000):
                        message = self.socket.recv_string()
                        self._process_message(message)
                except zmq.ZMQError as e:
                    logging.error(f"ZMQ Error receiving message: {e}")
                    continue
                
        except Exception as e:
            logging.error(f"Failed to start log collector: {e}")
            raise
        finally:
            self.cleanup()

    def _process_message(self, message: str):
        """Process and store received log message"""
        try:
            data = json.loads(message)
            dt = datetime.fromisoformat(data.get('timestamp', datetime.now(UTC).isoformat()))
            if dt.tzinfo is None:  
                dt = dt.replace(tzinfo=timezone.utc)
            else:  
                dt = dt.astimezone(timezone.utc)
            timestamp = int(dt.timestamp())
            loglevel = data.get('level', 'INFO').upper()
            source = data.get('source', 'UNKNOWN')
            category = data.get('category')
            content = data.get('message', message)
            
        except json.JSONDecodeError:
            timestamp = int(datetime.now(UTC).timestamp())
            loglevel = 'INFO'
            source = 'UNKNOWN'
            category = None
            content = message
        
        try:
            self.db.insert_event(timestamp, loglevel, source, category, content)
            logging.info(f"Stored event: {content[:100]}...")
        except sqlite3.Error as e:
            if "Database size limit reached" in str(e):
                logging.warning("Database size limit reached. Some older logs will be removed.")
            else:
                logging.error(f"Database error: {e}")


    def cleanup(self):
        """Cleanup resources"""
        logging.info("Cleaning up resources...")
        if hasattr(self, 'socket'):
            self.socket.close()
        if hasattr(self, 'context'):
            self.context.term()

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ZMQ Log Collector with SQLite storage')
    parser.add_argument('--port', type=int, default=18044,
                      help='Port to listen on (default: 18044)')
    parser.add_argument('--db-path', type=str, default="logs.db",
                      help='SQLite database path (default: logs.db)')
    parser.add_argument('--quiet', action='store_true',
                      help='Suppress INFO level log messages')
    
    args = parser.parse_args()
    
    collector = LogCollector(port=args.port, db_path=args.db_path, quiet=args.quiet)
    try:
        collector.start()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, shutting down...")
    finally:
        collector.cleanup()

if __name__ == "__main__":
    main()

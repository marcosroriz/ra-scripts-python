import datetime as dt
import time
import traceback
from sqlalchemy import text

class ExecutionLogger:
    def __init__(self, engine, script_name):
        self.engine = engine
        self.script_name = script_name
        self.start_time = None

    def __enter__(self):
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.start_perf = time.perf_counter()
        print(f"[{self.script_name}] Execution started at {self.start_time}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_perf = time.perf_counter()
        
        # Calculate duration in ms
        duration_ms = int((end_perf - self.start_perf) * 1000)
        
        status_complete = True
        exception_text = None

        if exc_type:
            # Check if it's a clean exit
            if isinstance(exc_val, SystemExit) and exc_val.code == 0:
                 status_complete = True
                 print(f"[{self.script_name}] Execution completed successfully (SystemExit 0).")
            else:
                status_complete = False
                # Format the exception
                exception_text = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
                print(f"[{self.script_name}] Execution failed: {exc_val}")
        else:
            print(f"[{self.script_name}] Execution completed successfully.")

        # SQL Insertion
        insert_sql = text("""
            INSERT INTO public.script_execution_log 
            (executed_at, script_name, status_complete, exception_text, execution_time_ms)
            VALUES (:executed_at, :script_name, :status_complete, :exception_text, :execution_time_ms)
        """)

        try:
            with self.engine.connect() as conn:
                conn.execute(insert_sql, {
                    "executed_at": self.start_time,
                    "script_name": self.script_name,
                    "status_complete": status_complete,
                    "exception_text": exception_text,
                    "execution_time_ms": duration_ms
                })
                conn.commit()
                print(f"[{self.script_name}] Log saved to database.")
        except Exception as e:
            print(f"[{self.script_name}] Failed to save log to database: {e}")
            
        return False

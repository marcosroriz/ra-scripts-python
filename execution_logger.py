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
        self.start_time = dt.datetime.now(dt.timezone.utc) # Use UTC for generic 'executed_at' if needed, or local
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
        # We need to construct the INSERT statement.
        # Table: public.script_execution_log
        # Columns: executed_at, script_name, status_complete, exception_text, execution_time_ms
        
        insert_sql = text("""
            INSERT INTO public.script_execution_log 
            (executed_at, script_name, status_complete, exception_text, execution_time_ms)
            VALUES (:executed_at, :script_name, :status_complete, :exception_text, :execution_time_ms)
        """)

        try:
            # Create a new connection to ensure we commit this log even if the main transaction failed (though engine.connect() usually starts new)
            # Depending on engine config, we might want to ensure auto-commit or explicit commit.
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
            # We don't want to suppress the original exception if there was one, 
            # but we also don't want to raise a DB log error if the script succeeded.
            # If the script failed, 'exc_type' is set, and it will be re-raised automatically by python context manager unless we return True.
            # We return False (default) so original exception propagates.

        # Return False to propagate exception if any occurred in the block
        return False

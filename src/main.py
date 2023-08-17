import sqlalchemy as sa
from datetime import datetime, timedelta
import pytz

timezone = pytz.UTC

def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
    conn = engine.connect()
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


def ingest_data(conn: sa.Connection, timestamp: str, detection_type: str):
    conn.execute(
        sa.text(
            "INSERT INTO detections (time, type) VALUES (:timestamp, :detection_type)"
        ).bindparams(
            timestamp=timestamp, 
            detection_type=detection_type
        )
    )

def format_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def convert_string(dtstr):
    time = datetime.strptime(dtstr, "%Y-%m-%dT%H:%M:%S")
    return timezone.localize(time)

def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    query = """
        WITH interval_calculations AS (
            SELECT type,
                   time,
                   time - LAG(time) OVER (PARTITION BY type ORDER BY time) AS time_diff
            FROM detections
        )
        SELECT type,
               MIN(time) AS start_time,
               MAX(time) AS end_time
        FROM (
            SELECT type,
                   time,
                   SUM(CASE WHEN time_diff IS NULL OR time_diff > INTERVAL '1 minute' THEN 1 ELSE 0 END) OVER (PARTITION BY type ORDER BY time) AS interval_group
            FROM interval_calculations
        ) subquery
        GROUP BY type, interval_group
        ORDER BY start_time
    """

    result = conn.execute(sa.text(query))
    aggregate_results = {
        "people": [],
        "vehicles": [],
    }

    for row in result:
        detection_type, start_time, end_time = row
        interval = (format_time(start_time), format_time(end_time))
        
        if detection_type in ["pedestrian", "bicycle"]:
            category = "people"
        else:
            category = "vehicles"

        # Combine intervals that are less than 1 minute apart in the same category
        if aggregate_results[category]:
            last_end_time = convert_string(aggregate_results[category][-1][1])
            if start_time - last_end_time <= timedelta(minutes=1):
                aggregate_results[category][-1] = (aggregate_results[category][-1][0], format_time(end_time))
            else:
                aggregate_results[category].append(interval)
        else:
            aggregate_results[category].append(interval)

    return aggregate_results
    
def main():
    conn = database_connection()

    # Simulate real-time detections every 30 seconds
    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "bicycle"),
        ("2023-08-10T18:37:30", "pedestrian"),
        ("2023-08-10T18:37:30", "bicycle"),
        ("2023-08-10T18:37:30", "car"),
        ("2023-08-10T18:37:30", "truck"),
        ("2023-08-10T18:38:00", "van"),
        ("2023-08-10T18:38:30", "truck"),
        ("2023-08-10T18:39:00", "car"),
    ]
    
    # add alert
    consecutive_count = 0
    for timestamp, detection_type in detections:
        ingest_data(conn, timestamp, detection_type)
        
        if detection_type in ["pedestrian", "bicycle"]:
            consecutive_count += 1
            if consecutive_count >= 5:
                print(f"ALERT: Unusual activity - person detected for a long time.")
        else:
            consecutive_count = 0

    aggregate_results = aggregate_detections(conn)
    print(aggregate_results)

if __name__ == "__main__":
    main()

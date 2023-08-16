import sqlalchemy as sa
import datetime
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
    db_ingest_table = sa.table("detections",
                              sa.column("id"),
                              sa.column("time"),
                              sa.column("type"))
    conn.execute(
        sa.insert(db_ingest_table).values(
            {
                "time": timestamp,
                "type": detection_type,
            }
        )
    )
    conn.commit()
    # print(f'Data ingested at {timestamp}, of type {detection_type}')


def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    result = conn.execute(
        sa.text("SELECT * FROM detections")
    )
    data = result.all()
    # print(f'Results for aggregating detections {data}, {len(data)}')
    aggregated_data = {}
    for row in data:
        _, time, detection_type = row.tuple()
        # print(f'Data row {time}, {detection_type}')
        type_aggregated_value = aggregated_data.get(detection_type, [])
        
        if len(type_aggregated_value) == 0:
            type_aggregated_value.append((time.strftime("%Y-%m-%dT%H:%M:%S"), time.strftime("%Y-%m-%dT%H:%M:%S")))
        else:
            last_aggregated_start_time, last_aggregated_end_time = type_aggregated_value[-1]
            last_aggregated_end_time_offset_unaware = datetime.datetime.strptime(last_aggregated_end_time, "%Y-%m-%dT%H:%M:%S")
            last_aggregated_end_time_offset_aware = timezone.localize(last_aggregated_end_time_offset_unaware)
            if time - last_aggregated_end_time_offset_aware <= datetime.timedelta(minutes=1):
                last_aggregated_end_time = time.strftime("%Y-%m-%dT%H:%M:%S")
                type_aggregated_value[-1] = (last_aggregated_start_time, last_aggregated_end_time)
            else:
                type_aggregated_value.append((time.strftime("%Y-%m-%dT%H:%M:%S"), time.strftime("%Y-%m-%dT%H:%M:%S")))

        aggregated_data[detection_type] = type_aggregated_value
    return aggregated_data

def clean_data(conn: sa.Connection):
    db_delete_table = sa.table("detections",
                              sa.column("id"),
                              sa.column("time"),
                              sa.column("type"))
    result = conn.execute(
        sa.delete(db_delete_table).returning()
    )
    conn.commit()
    print(f'Deleted existing data {result}')
    
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
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]
    # clean up the detections table
    clean_data(conn)
    
    for timestamp, detection_type in detections:
        ingest_data(conn, timestamp, detection_type)

    aggregate_results = aggregate_detections(conn)
    print(aggregate_results)

if __name__ == "__main__":
    main()

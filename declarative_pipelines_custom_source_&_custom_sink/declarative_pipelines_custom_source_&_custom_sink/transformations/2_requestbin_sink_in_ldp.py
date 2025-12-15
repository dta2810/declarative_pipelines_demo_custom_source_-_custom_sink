
import dlt
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
from datetime import datetime
import requests

from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamWriter,
    WriterCommitMessage,
)


# Define your RequestBin endpoint
my_requestbin = "https://your-requestbin-url"  # Replace with your actual RequestBin URL

class RequestBinDataSource(DataSource):
    def __init__(self, options):
        super().__init__(options)
        self.options = options

    @classmethod
    def name(cls):
        return "requestbin_sink"

    def schema(self):
        return (
            "timestamp timestamp, icao24 string, callsign string, country string, "
            "longitude double, latitude double, altitude_m double, velocity_ms double, "
            "vertical_rate double, on_ground boolean"
        )

    def streamWriter(self, schema, overwrite):
        return RequestBinStreamWriter(self.options)

class SimpleCommitMessage(WriterCommitMessage):
    def __init__(self, partition_id, count, success_count):
        self.partition_id = partition_id
        self.count = count
        self.success_count = success_count


class RequestBinStreamWriter(DataSourceStreamWriter):
    def __init__(self, options):
        self.options = options
        self.requestbin_url = options.get("requestbin_url", my_requestbin)
        self.max_records = int(options.get("max_records_per_request", "100"))

    def write(self, iterator):
        from pyspark import TaskContext
        from datetime import datetime
        import requests

        context = TaskContext.get()
        partition_id = context.partitionId()
        total_cnt = 0
        total_success = 0
        batch_data = []

        def send_batch(records):
            nonlocal total_success
            if not records:
                return
            payload = {
                "batch_id": f"batch_{partition_id}_{datetime.now().timestamp()}",
                "partition_id": partition_id,
                "record_count": len(records),
                "records": records,
            }
            response = requests.post(
                self.requestbin_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if 200 <= response.status_code < 300:
                total_success += len(records)
            elif response.status_code == 413:
                # too large even at this batch size; log and drop for demo purposes
                print(f"Warning: HTTP 413 for partition {partition_id}, {len(records)} records")
            else:
                raise RuntimeError(f"RequestBin HTTP {response.status_code}")

        for row in iterator:
            total_cnt += 1
            row_dict = {
                "timestamp": row[0].isoformat() if row[0] else None,
                "icao24": row[1],
                "callsign": row[2],
                "country": row[3],
                "longitude": row[4],
                "latitude": row[5],
                "altitude_m": row[6],
                "velocity_ms": row[7],
                "vertical_rate": row[8],
                "on_ground": row[9],
                "partition_id": partition_id,
                "batch_time": datetime.now().isoformat(),
            }
            batch_data.append(row_dict)
            if len(batch_data) >= self.max_records:
                send_batch(batch_data)
                batch_data = []

        if batch_data:
            send_batch(batch_data)

        return SimpleCommitMessage(
            partition_id=partition_id,
            count=total_cnt,
            success_count=total_success,
        )


    def commit(self, messages, batchId):
        total_rows = sum(m.count for m in messages)
        success_rows = sum(m.success_count for m in messages)
        print(f"Batch {batchId} committed: {success_rows}/{total_rows} rows successfully sent to RequestBin")

    def abort(self, messages, batchId):
        print(f"Batch {batchId} aborted")

spark.dataSource.register(RequestBinDataSource)

dlt.create_sink(
    name="requestbin_opensky_sink",
    format="requestbin_sink",
     options={
        "requestbin_url": my_requestbin,
        "max_records_per_request": "50"  # cap for demo purposes
    },
)

@dlt.append_flow(name="flow_to_requestbin", target="requestbin_opensky_sink")
def stream_to_requestbin():
    # return dlt.read_stream("opensky_flights").repartition(4)
    return dlt.read_stream("opensky_flights").limit(100).repartition(4)      # For demos, cap with .limit(100) and use .repartition(4) to show parallel writes.



## RESULTS -> https://pipedream.com/

# Apache NiFi configuration

This folder contains configuration files and templates for Apache NiFi
used in the Netflix analytics stack.  When NiFi starts via
`docker-compose`, it looks for custom extensions and templates in this
directory (mounted into the container at `/opt/nifi/nifi-current/extensions`).

You can design your own data flow by importing a template (`flow_template.xml`) via
the NiFi UI (available at `http://localhost:8080` when running locally).
The provided template offers a basic starting point; it implements the following
high‑level steps:

1. **Read** the `netflix_revenue_updated.csv` file from a local or mounted
   directory using the *GetFile* processor.
2. **Split** the CSV into individual records (e.g., using *SplitText* or
   *ConvertRecord*) and transform field names as needed.
3. **Publish** each record to a Kafka topic (`netflix`) using *PublishKafka*.
4. Optionally write a copy of the processed data to another destination
   (e.g., HDFS, S3 or a relational database).

The `flow_template.xml` included here is intentionally minimal and serves
as a starting point.  Import it into NiFi and customise it according to
your environment (e.g., update file paths or Kafka settings).  The
template is mounted into the NiFi container at `/opt/nifi/nifi-current/extensions` by
the `docker-compose.yml` file, so it is easy to version‑control your flows.


### Configuration example

```yaml
datasets:
  batch:
    - name: test_dataset
      format: csv
      landing_path: "abfss://landing@jbrcursosparkucm.dfs.core.windows.net/test_data"
      bronze_path: "abfss://bronze@jbrcursosparkucm.dfs.core.windows.net/test_data"
      partition_columns: ["year", "month", "day"]
      datasource: "test_source"
      dataset: "test_dataset"
      mode: "planned"
      output_format: "delta"
      merge_schema: true
      incremental: false
      options:
        cloudFiles.format: "csv"
        cloudFiles.inferColumnTypes: "true"
        cloudFiles.schemaEvolutionMode: "addNewColumns"

  streaming:
    - name: test_streaming_dataset
      format: avro
      topic_pattern: "test-topic"
      key_subject: "key-subject"
      value_subject: "value-subject"
      bronze_path: "abfss://bronze@jbrcursosparkucm.dfs.core.windows.net/test_data"
      datasource: "test_source"
      dataset: "test_streaming_dataset"
      mode: "continuous"
      trigger_interval: "1 minute"
      output_format: "delta"
      merge_schema: true
      kafka:
        bootstrap.servers: "your_kafka_bootstrap_servers"
        security.protocol: "SASL_SSL"
        sasl.mechanism: "PLAIN"
        sasl.username: "your_kafka_username"
        sasl.password: "your_kafka_password"
        subscribe: "your_topic"
      schema_registry:
        url: "https://your-schema-registry-url"
        username: "your-schema-registry-username"
        password: "your-schema-registry-password"

```



### Configuration example

```yaml
datasets:
  batch:
    - name: dataset1
      format: csv
      schema: schema_dataset1.json
      landing_path: abfss://<container>@<account>.dfs.core.windows.net/landing/dataset1/
      bronze_path: abfss://<container>@<account>.dfs.core.windows.net/bronze/
      partition_columns: ["column1", "column2"]
      datasource: my_source
      dataset: dataset1
      incremental: true
      mode: planned
      output_format: delta
      merge_schema: true
      options:
        cloudFiles.format: csv
        cloudFiles.inferColumnTypes: true
        cloudFiles.schemaEvolutionMode: addNewColumns
    - name: dataset2
      format: avro
      schema: schema_dataset2.avsc
      landing_path: abfss://<container>@<account>.dfs.core.windows.net/landing/dataset2/
      bronze_path: abfss://<container>@<account>.dfs.core.windows.net/bronze/
      partition_columns: ["column3", "column4"]
      datasource: my_source
      dataset: dataset2
      incremental: false
      mode: planned
      output_format: delta
      merge_schema: false
      options:
        cloudFiles.format: avro
        cloudFiles.inferColumnTypes: true
        cloudFiles.schemaEvolutionMode: addNewColumns
  streaming:
    - name: dataset1
      source_type: kafka
      format: avro
      key_subject: key
      value_subject: value
      topic_pattern: topic1
      bronze_path: abfss://<container>@<account>.dfs.core.windows.net/bronze/
      partition_columns: ["column1", "column2"]
      datasource: my_source
      dataset: dataset1
      mode: continuous
      trigger_interval: "1 minute"
      output_format: delta
      merge_schema: true
      kafka:
        bootstrap_servers: "your_kafka_bootstrap_servers"
        startingOffsets: "latest"
        security_protocol: "SASL_SSL"
        sasl_mechanism: "PLAIN"
        sasl_jaas_config: "org.apache.kafka.common.security.plain.PlainLoginModule required username='your_username' password='your_password';"
      schema_registry:
        url: "http://your-schema-registry-url"
        username: "your-username"
        password: "your-password"
    - name: dataset2
      source_type: kafka
      format: json
      bronze_path: abfss://<container>@<account>.dfs.core.windows.net/bronze/
      partition_columns: ["column1", "column2"]
      datasource: my_source
      dataset: dataset2
      mode: planned
      output_format: delta
      merge_schema: false
      options:
        cloudFiles.format: json
        cloudFiles.inferColumnTypes: true
        cloudFiles.schemaEvolutionMode: addNewColumns
        cloudFiles.schemaHints: clicked_items string, ordered_products.element.promotion_info string, fulfillment_days string
```

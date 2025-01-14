# Jdbc

> Source plugin : Jdbc [Flink]

## Description

Read data through jdbc

## Options

| name                  | type   | required | default value |
|-----------------------|--------| -------- | ------------- |
| driver                | string | yes      | -             |
| url                   | string | yes      | -             |
| username              | string | yes      | -             |
| password              | string | no       | -             |
| query                 | string | yes      | -             |
| fetch_size            | int    | no       | -             |
| partition_column      | string | no       | -             |
| partition_upper_bound | long   | no       | -             |
| partition_lower_bound | long   | no       | -             |
| common-options        | string | no       | -             |
| parallelism           | int    | no       | -             |

### driver [string]

Driver name, such as `com.mysql.cj.jdbc.Driver` for MySQL.

Warn: for license compliance, you have to provide MySQL JDBC driver yourself, e.g. copy `mysql-connector-java-xxx.jar` to `$FLINK_HOME/lib` for Standalone.

### url [string]

The URL of the JDBC connection. Such as: `jdbc:mysql://localhost:3306/test`

### username [string]

username

### password [string]

password

### query [string]

Query statement

### fetch_size [int]

fetch size

### parallelism [int]

The parallelism of an individual operator, for JdbcSource.

### partition_column [string]

The column name for parallelism's partition, only support numeric type.

### partition_upper_bound [long]

The partition_column max value for scan, if not set SeaTunnel will query database get max value.

### partition_lower_bound [long]

The partition_column min value for scan, if not set SeaTunnel will query database get min value.

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
JdbcSource {
    driver = com.mysql.jdbc.Driver
    url = "jdbc:mysql://localhost/test"
    username = root
    query = "select * from test"
}
```

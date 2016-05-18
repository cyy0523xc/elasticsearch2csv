# elasticsearch2csv

Export data from ElasticSearch to CSV file. 

从ElasticSearch导出数据到csv文件

## Example

```sh
python es2csv.py --index-name=indexname --file-name=/tmp/filename.csv --doc-type=typename
```

## Help


```sh
python es2csv.py --help
```

## Todos

- [x] 分页导入
- [ ] 支持复杂字段，例如经纬度等

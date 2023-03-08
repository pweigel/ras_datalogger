## README.md ##
A simple datalogging tool for RAS 

Here is an example PV configuration for storing values as the pvServer receives them:
```yaml
"device:get_pressure" : {
  "value_dtype": "f8",
  "time_dtype": "f8",
  "scan_type": "continuous",
  "sample_rate": ,
}
```

Alternatively, you can directly poll the PV with pyEPICS with some rate:
```yaml
"device:get_pressure" : {
  "value_dtype": "f8",
  "time_dtype": "f8",
  "scan_type": "sample",
  "sample_rate": 0.5,
}
```

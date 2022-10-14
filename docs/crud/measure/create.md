# Create Measure


To create a measure schema. use **create -f [file|dir|-]**

**-f [file|dir|-]** option specify the source of measure schema: pass file or dir if it is from file/directory or pass - if it is from command line<br>
By default, banyandb CLI connects to the server at the address 127.0.0.1 with port 17913. You can change a different IP address or Port by using the **-a** option.


For example:
```
    bydbctl measure create -a 127.0.0.1:17913 -f - \
metadata:
  name: service_cpm_minute  
  group: sw_metric
```


## API Reference

[MeasureService v1](../../api-reference.md#measureservice)
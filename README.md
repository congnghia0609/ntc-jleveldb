# ntc-jleveldb
ntc-jleveldb is a module swapper java leveldb

## Usage
```java
NLSerializer nls = new NLSerializer();
String dbPath = "./db";
LDBSingleConnection conn = LDBSingleConnection.getInstance(dbPath);


String key = "nghiatc";
String value = "handsome";
// Put Data
conn.put(key, value);
// Get Data
String rs1 = conn.get(key);
Assert.assertEquals(value, rs1);
String rs2 = nls.deserializeString(conn.getByte(nls.serializeString(key)));
Assert.assertEquals(value, rs2);
// Delete Data
conn.delete(key);
String rs3 = conn.get(key);
Assert.assertEquals(null, rs3);


conn.close();
```

## License
This code is under the [Apache License v2](https://www.apache.org/licenses/LICENSE-2.0).  
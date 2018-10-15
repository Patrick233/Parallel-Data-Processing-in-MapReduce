# A4 Review ankita-ushang-xia
Reviewer:  Navya-Shabbir-Yu

---

## Code

* Missing structure in code:

It is a good practive to organize multiple java files into function
specific sub-packages. And one have one entry point java file. This
structure is clear and easy to understand.
Eg:

```
.
+-- org.neu.pdpmr
|   +-- drivers
|       +-- Driver1.java
|   +-- mappers
|       +-- Mapper1.java
|   +-- reducers
|   +-- util
|   +-- Main.java
```

### ActiveAirportAirlines.java

Two classes are embeded in a wrapper class. Unnecessary use of inner
public static classes. Use namespaces instead.

## Overall job:

Suggestion for saving 2 passes at whole corpus:
- One job that will spit cleaned flight records. And send flight delays
with frequency.
- The same job can also create side-effect by storing flight count in
memory for carrier and destinations as there are very few of them. Then
in the cleanup stage of the job emit these records in the same stream
but with a secondary sort key. This way in the second reduce only job
you just check the first record has a secondary key say "-1", if it is
there that means following records are from top N airlines or destinations.

## AverageMonthlyDelayMapper.java

```
@Override
protected void setup(Context context) throws IOException, InterruptedException {
   //Read the previous file and call the helper function to get top 5
   readFile("/part-r-00000",topAirlines,context);
   readFile("/part-r-00001",topAirports,context);
}
```
This is not the most reliable way to read the part files. Either loop through all
the parts available in the directory or generate only one merged file
using hdfs util.

## ActivePartitioner
```
public int getPartition(Text text, IntWritable intWritable, int i) {
    if(text.toString().substring(0,2).equals("AL")) return 0;
    return 1;
}
```

Too many conversions. A better way is the have a secondary key and
create a writable class for that key.

### Consider replacing

```
Iterator it = airlineCount.entrySet().iterator();
while(it.hasNext()){
    Map.Entry pair = (Map.Entry)it.next();
    context.write(new Text(String.valueOf("AL_"+pair.getKey())),new IntWritable((int)pair.getValue()));
}
```
with
```
for(Map.Entry pair: airlineCount.entrySet()){
    context.write(new Text(String.valueOf("AL_"+pair.getKey())),new IntWritable((int)pair.getValue()));
}
```
or Java 8
```
airlineCount.foreach((k,v) -> {
    try{
        context.write(new Text(String.valueOf("AL_"+k)),new IntWritable((int)v));
    } catch (Exception e){}
});
```
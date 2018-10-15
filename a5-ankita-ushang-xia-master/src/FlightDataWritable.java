import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is a custom Writable class to store all the data for a single flight
 * @author Ankita,Ushang,Xia
 */
public class FlightDataWritable implements WritableComparable {

    Text carrier;
    Text src;
    Text dest;
    LongWritable deptTime;
    LongWritable arrivalTime;
    DoubleWritable delay;
    IntWritable month;

    public FlightDataWritable(){
        carrier = new Text();
        src = new Text();
        dest = new Text();
        deptTime = new LongWritable();
        arrivalTime = new LongWritable();
        delay = new DoubleWritable();
        month = new IntWritable();
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public DoubleWritable getDelay() {
        return delay;
    }

    public void setDelay(DoubleWritable delay) {
        this.delay = delay;
    }

    public Text getCarrier() {
        return carrier;
    }

    public void setCarrier(Text carrier) {
        this.carrier = carrier;
    }

    public Text getSrc() {
        return src;
    }

    public void setSrc(Text src) {
        this.src = src;
    }

    public Text getDest() {
        return dest;
    }

    public void setDest(Text dest) {
        this.dest = dest;
    }

    public LongWritable getDeptTime() {
        return deptTime;
    }

    public void setDeptTime(LongWritable deptTime) {
        this.deptTime = deptTime;
    }

    public LongWritable getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(LongWritable arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.carrier.write(dataOutput);
        this.src.write(dataOutput);
        this.dest.write(dataOutput);
        this.deptTime.write(dataOutput);
        this.arrivalTime.write(dataOutput);
        this.delay.write(dataOutput);
        this.month.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(this.carrier == null)        this.carrier = new Text();
        if(this.src == null)            this.src = new Text();
        if(this.dest == null)           this.dest = new Text();
        if(this.deptTime == null)       this.deptTime = new LongWritable();
        if(this.arrivalTime == null)    this.arrivalTime = new LongWritable();
        if(this.delay == null)          this.delay = new DoubleWritable();
        if(this.month == null)          this.month = new IntWritable();
        this.carrier.readFields(dataInput);
        this.src.readFields(dataInput);
        this.dest.readFields(dataInput);
        this.deptTime.readFields(dataInput);
        this.arrivalTime.readFields(dataInput);
        this.delay.readFields(dataInput);
        this.month.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
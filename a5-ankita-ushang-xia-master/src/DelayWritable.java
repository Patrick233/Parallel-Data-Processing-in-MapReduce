import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is a custom Writable class to store number of flights and  delays
 * @author Ankita,Ushang,Xia
 */
public class DelayWritable implements WritableComparable {

    IntWritable flights;
    DoubleWritable delay;

    public DelayWritable(){
        flights = new IntWritable();
        delay = new DoubleWritable();
    }

    public DelayWritable(IntWritable flights, DoubleWritable delay){
        this.flights = flights;
        this.delay = delay;
    }

    public IntWritable getFlights() {
        return flights;
    }

    public void setFlights(IntWritable flights) {
        this.flights = flights;
    }

    public DoubleWritable getDelay() {
        return delay;
    }

    public void setDelay(DoubleWritable normalizedDelay) {
        this.delay = normalizedDelay;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.flights.write(dataOutput);
        this.delay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(this.flights == null)    this.flights = new IntWritable();
        if(this.delay == null)    this.delay = new DoubleWritable();

        this.flights.readFields(dataInput);
        this.delay.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is a custom Writable class to store flights and normalized delay for a month
 * @author Ankita,Ushang,Xia
 */
public class FlightDataWritable implements WritableComparable {

    IntWritable flights;
    DoubleWritable normalizedDelay;

    public FlightDataWritable(){
        flights = new IntWritable();
        normalizedDelay = new DoubleWritable();
    }

    public FlightDataWritable(IntWritable flights, DoubleWritable normalizedDelay){
        this.flights = flights;
        this.normalizedDelay = normalizedDelay;
    }

    public IntWritable getFlights() {
        return flights;
    }

    public void setFlights(IntWritable flights) {
        this.flights = flights;
    }

    public DoubleWritable getNormalizedDelay() {
        return normalizedDelay;
    }

    public void setNormalizedDelay(DoubleWritable normalizedDelay) {
        this.normalizedDelay = normalizedDelay;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.flights.write(dataOutput);
        this.normalizedDelay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(this.flights == null)    this.flights = new IntWritable();
        if(this.normalizedDelay == null)    this.normalizedDelay = new DoubleWritable();

        this.flights.readFields(dataInput);
        this.normalizedDelay.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}

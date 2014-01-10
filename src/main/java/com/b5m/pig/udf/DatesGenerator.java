package com.b5m.pig.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Generator of dates in string format <code>YYYYMMDD</code>.
 * @author Paolo D'Apice
 */
public class DatesGenerator extends LoadFunc {

    private final static Log log = LogFactory.getLog(DatesGenerator.class);

    private final String date;
    private final int count;

    private List<LocalDate> dates = null;
    private Iterator<LocalDate> iterator = null;
    private String location = null;

    private final TupleFactory tupleFactory = TupleFactory.getInstance();
    private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd");

    /**
     * Single date generator.
     */
    public DatesGenerator(String date) {
        this(date, "1");
    }

    /**
     * Date list generator.
     * Will generate <code>count</code> dates starting from <code>date</code>.
     */
    public DatesGenerator(String date, String count) {
        this.date = date;
        this.count = Integer.parseInt(count);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        this.location = location;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        if (log.isDebugEnabled()) log.debug("date=" + date + ", count=" + count);

        dates = new ArrayList<LocalDate>();

        LocalDate last = dtf.parseDateTime(date).toLocalDate();
        for (int i = 0; i < count; i++) {
            dates.add(last.minusDays(i));
        }
        if (log.isDebugEnabled()) log.debug("dates: " + dates);

        iterator = dates.iterator();
        if (log.isDebugEnabled()) log.debug("iterator on " + dates.size() + " dates");
    }

    private static class DummyInputSplit extends InputSplit implements Writable {

        private String location;

        // used through reflection by Hadoop
        @SuppressWarnings("unused")
        public DummyInputSplit() {}

        public DummyInputSplit(String location) {
            this.location = location;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[] { location };
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 10000000;
        }

        @Override
        public boolean equals(Object other) {
            return other == this;
        }

        @Override
        public int hashCode() {
            return location.hashCode();
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            location = input.readUTF();
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            arg0.writeUTF(location);
        }
    }

    private static class DummyRecordReader extends RecordReader<Object, Object> {
        @Override
        public void close() throws IOException {}

        @Override
        public Object getCurrentKey() throws IOException, InterruptedException {
            return "dummyKey";
        }

        @Override
        public Object getCurrentValue() throws IOException, InterruptedException {
            return "dummyValue";
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.5f;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return true;
        }
    }

    private static class DummyInputFormat extends InputFormat<Object, Object> {

        private final String location;

        private DummyInputFormat(String location) {
            this.location = location;
        }

        @Override
        public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
            return Arrays.<InputSplit>asList(new DummyInputSplit(location));
        }

        @Override
        public RecordReader<Object, Object> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
            return new DummyRecordReader();
        }

    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new DummyInputFormat(location);
    }

    @Override
    public Tuple getNext() throws IOException {
        if (!iterator.hasNext()) {
            if (log.isDebugEnabled()) log.debug("no more dates");
            return null;
        }

        LocalDate date = iterator.next();
        String string = date.toString(dtf);

        if (log.isDebugEnabled()) log.debug("date string: " + string);

        return tupleFactory.newTuple(Arrays.asList(string));
    }

}


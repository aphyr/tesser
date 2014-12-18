package tesser.hadoop_support;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;
import clojure.lang.IFn;

// Mutable container for reading and writing Fressian data structures using
// Hadoop. Has a pair of static IFns for reading and writing data, which will
// be filled in by Fressian code later.  readFieldsFn is invoked with this and
// the input, and is expected to clobber this's state. writeFn is invoked with
// this and an output, and is expected to serialize this's state to that
// output.
//
// Ugly, ugly hack.
//
// Totally not thread safe, but what's life without a little danger?
public class FressianWritable implements Writable {
  public static IFn readFieldsFn;
  public static IFn writeFn;

  public Object state;

  public FressianWritable() {
  }

  public void readFields(DataInput in) {
    readFieldsFn.invoke(this, in);
  }

  public void write(DataOutput out) {
    writeFn.invoke(this, out);
  }
}

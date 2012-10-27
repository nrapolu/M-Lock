package ham.wal;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.*;

/**                                                                                                            
 * A binary comparator which lexicographically compares against the specified                                  
 * byte array using {@link org.apache.hadoop.hbase.util.Bytes#compareTo(byte[], byte[])}.                      
 */
public class MyBinaryComparator extends WritableByteArrayComparable {

  /** Nullary constructor for Writable, do not use */
  public MyBinaryComparator() { }
  
  /**                                                                                                          
   * Constructor                                                                                               
   * @param value value                                                                                        
   */
  public MyBinaryComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte [] value, int offset, int length) {
  	System.out.println("Value sent by region: " + Bytes.toLong(value, offset, length));
  	System.out.println("Our value: " + Bytes.toLong(this.getValue(), 0, this.getValue().length));
    return Bytes.compareTo(this.getValue(), 0, this.getValue().length, value, offset, length);
  }
}


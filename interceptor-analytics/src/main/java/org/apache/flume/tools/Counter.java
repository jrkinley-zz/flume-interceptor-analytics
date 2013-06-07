package org.apache.flume.tools;

/**
 * An item and its count. Implements the {@link Comparable} interface for sorting by count.
 */
public class Counter implements Comparable<Counter> {
  private String item;
  private long count;

  public Counter(String item, long count) {
    this.item = item;
    this.count = count;
  }

  /**
   * @return the item
   */
  public String getItem() {
    return item;
  }

  /**
   * @return the count
   */
  public long getCount() {
    return count;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(Counter other) {
    return (this.count == other.count) ? 0 : (this.count < other.count) ? -1 : 1;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return item + "=" + count;
  }
}

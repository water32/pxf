package org.greenplum.pxf.api.model;

import com.google.common.collect.ImmutableSet;
import org.greenplum.pxf.api.examples.DemoFragmentMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataSplitSegmentIteratorTest {

    List<DataSplit> splitList;

    @BeforeEach
    void setup() {
        splitList = Arrays.asList(
                new DataSplit("foo", new DemoFragmentMetadata("foo_path")),
                new DataSplit("bar", new DemoFragmentMetadata("bar_path")),
                new DataSplit("foobar", new DemoFragmentMetadata("foobar_path"))
        );
    }

    @Test
    void testEmptyIterator() {
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, 1, Collections.emptyIterator());

        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void whenThereIsOneSegmentItProcessesAllTheSplits() {
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, 1, splitList);

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.hasNext()).isEqualTo(true); // make sure hasNext is idempotent
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.hasNext()).isEqualTo(true); // make sure hasNext is idempotent
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.hasNext()).isEqualTo(true); // make sure hasNext is idempotent
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThat(iterator.hasNext()).isEqualTo(false); // make sure hasNext is idempotent
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testNextCallWithoutCallingHasNext() {
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, 1, splitList);

        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void twoSegments() {
        int totalSegments = 2;
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(1, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void setOfTwoSegments() {
        Set<Integer> segmentIds = ImmutableSet.of(0, 1);
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(segmentIds, 2, splitList);

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void threeSegments() {
        int totalSegments = 3;
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(1, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(2, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void setOfTwoSegmentsWithTotalOfThreeSegments() {
        int totalSegments = 3;
        Set<Integer> segmentIds = ImmutableSet.of(1, 2);
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(segmentIds, totalSegments, splitList);

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        segmentIds = ImmutableSet.of(0);
        iterator = new DataSplitSegmentIterator<>(segmentIds, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testWhenThereAreMoreSegmentsThanSplits() {
        int totalSegments = 6;
        Iterator<DataSplit> iterator = new DataSplitSegmentIterator<>(0, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(1, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(2, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(3, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(4, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(2));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

        iterator = new DataSplitSegmentIterator<>(5, totalSegments, splitList.iterator());

        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(0));
        assertThat(iterator.hasNext()).isEqualTo(true);
        assertThat(iterator.next()).isEqualTo(splitList.get(1));
        assertThat(iterator.hasNext()).isEqualTo(false);
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

}
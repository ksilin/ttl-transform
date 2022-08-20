package com.example;

import java.util.Objects;

public class StoreCleanerStats {

    private Long processedTotal = 0L;
    private Long evictedTotal = 0L;
    private Long skippedTotal = 0L;
    private Long abortedTotal = 0L;

    public Long getProcessedTotal() {
        return processedTotal;
    }

    public void incrementProcessedTotal(Integer processedIncrement) {
        this.processedTotal = processedTotal + processedIncrement;
    }

    public Long getEvictedTotal() {
        return evictedTotal;
    }

    public void incrementEvictedTotal(Integer evictedIncrement) {
        this.evictedTotal = evictedTotal + evictedIncrement;
    }

    public Long getSkippedTotal() {
        return skippedTotal;
    }

    public void incrementSkippedTotal(Integer skippedIncrement) {
        this.skippedTotal = skippedTotal + skippedIncrement;
    }

    @Override
    public String toString() {
        return "StoreCleanerStats{" +
                "processedTotal=" + processedTotal +
                ", evictedTotal=" + evictedTotal +
                ", skippedTotal=" + skippedTotal +
                ", abortedTotal=" + abortedTotal +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreCleanerStats that = (StoreCleanerStats) o;
        return processedTotal.equals(that.processedTotal) && evictedTotal.equals(that.evictedTotal) && skippedTotal.equals(that.skippedTotal) && abortedTotal.equals(that.abortedTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processedTotal, evictedTotal, skippedTotal, abortedTotal);
    }

    public long getAbortedTotal() {
        return abortedTotal;
    }

    public void incrementAbortedTotal() {
        this.abortedTotal = abortedTotal + 1;
    }
}

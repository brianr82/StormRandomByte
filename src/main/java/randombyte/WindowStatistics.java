package randombyte;

class WindowStatistics {
    public long windowStartTime;
    public long windowEndTime;
    public int count;
    public float throughput;

    public WindowStatistics(
            long windowStartTime,
            long windowEndTime,
            int count
            )


    {
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
        this.count = count;


    }

    public String getString() {
        return "" + windowStartTime + ','
                +	windowEndTime + ','
                +	count;

    }

    public static String getOrder() {
        return "windowStartTime(ns),windowEndTime(ns),count";
    }
}
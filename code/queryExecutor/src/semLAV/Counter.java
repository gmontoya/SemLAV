package semLAV;

public class Counter {

    private int c = 0;
    public Counter() {
        this.c = 0;
    }

    public void increase() {
        this.c++;
    }

    public int getValue() {
        return this.c;
    }

    public void reset() {

        this.c = 0;
    }
}

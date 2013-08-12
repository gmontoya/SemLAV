package semLAV;

/**
 *
 * @author gonzalez-l
 */
public class Timer {

  private long startTime = 0;
  private long endTime   = 0;
  private long elapsedTime = 0;

  public void start(){

	this.elapsedTime = 0;
    this.startTime = System.currentTimeMillis();
  }

  public void stop() {
    this.endTime   = System.currentTimeMillis();
	this.elapsedTime += this.endTime - this.startTime;
  }

  public void resume() {
	this.startTime = System.currentTimeMillis();
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public long getTotalTime() {
    return this.elapsedTime;
  }
}

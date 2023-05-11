/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;

public class InsertJobTask implements CatalogRetry<Void> {

  private Catalog imageStore;
  private String jobId;
  private String taskId;

  public InsertJobTask(Catalog imageStore, String taskId, String jobId) {
    this.imageStore = imageStore;
    this.taskId = taskId;
    this.jobId = jobId;
  }

  @Override
  public Void run() {
    imageStore.insertJobTask(taskId, jobId);
    return null;
  }
}

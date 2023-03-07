/* (C)2020 */
package saps.catalog.core.retry.catalog;

import java.util.Date;
import java.util.List;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsUserJob;

public class AddNewUserJob implements CatalogRetry<SapsUserJob> {

  private Catalog imageStore;
  private String jobId;
  private String lowerLeftLatitude;
  private String lowerLeftLongitude;
  private String upperRightLatitude;
  private String upperRightLongitude;
  private String userEmail;
  private String jobLabel;
  private Date startDate;
  private Date endDate;
  private int priority;
  private List<String> taskIds;

  public AddNewUserJob(Catalog imageStore,
      String jobId,
      String lowerLeftLatitude,
      String lowerLeftLongitude,
      String upperRightLatitude,
      String upperRightLongitude,
      String userEmail,
      String jobLabel,
      Date startDate,
      Date endDate,
      int priority,
      List<String> tasksIds) {
    this.imageStore = imageStore;
    this.userEmail = userEmail;
    this.jobId = jobId;
    this.lowerLeftLatitude = lowerLeftLatitude;
    this.lowerLeftLongitude = lowerLeftLongitude;
    this.upperRightLatitude = upperRightLatitude;
    this.upperRightLongitude = upperRightLongitude;
    this.jobLabel = jobLabel;
    this.startDate = startDate;
    this.endDate = endDate;
    this.priority = priority;
    this.taskIds = tasksIds;
  }

  @Override
  public SapsUserJob run() {
    return imageStore.addJob(
        jobId,
        lowerLeftLatitude,
        lowerLeftLongitude,
        upperRightLatitude,
        upperRightLongitude,
        userEmail,
        jobLabel,
        startDate,
        endDate,
        priority,
        taskIds);
  }
}

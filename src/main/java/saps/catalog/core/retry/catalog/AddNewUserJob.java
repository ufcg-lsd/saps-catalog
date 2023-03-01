/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;

public class AddNewUserJob implements CatalogRetry<SapsUserJob> {

  private Catalog imageStore;
  private String userEmail;
  private String userName;
  private String userPass;
  private boolean isEnable;
  private boolean userNotify;
  private boolean adminRole;

  public AddNewUserJob(
    Catalog imageStore,
    Sring jobId, 
	String lowerLeftLatitude, 
	String lowerLeftLongitude,
	String upperRightLatitude, 
	String upperRightLongitude, 
	String state,
	String userEmail,
    String jobLabel,
	Date startDate,
	Date endDate,
	int priority,
	List<String> taskIds,
	String inputDownloadingTag,
	String preProcessingTag,
	String processingTag, 
	Date updatedTime,
	Date creationTime) {
    this.imageStore = imageStore;
    this.userEmail = userEmail;
    this.userName = userName;
    this.userPass = userPass;
    this.isEnable = isEnable;
    this.userNotify = userNotify;
    this.adminRole = adminRole;
  }

  @Override
  public SapsUserJob run() {
    return imageStore.addJob(jobId,
        lowerLeftLatitude, 
	    lowerLeftLongitude, 
	    upperRightLatitude,
	    upperRightLongitude,
	    state,
	    userEmail,
	    jobLabel,
	    startDate,
	    endDate,
	    priority,
	    taskIds,
	    inputDownloadingTag,
	    preProcessingTag,
        processingTag
		);
  }
}

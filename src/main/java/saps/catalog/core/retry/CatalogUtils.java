/* (C)2020 */
package saps.catalog.core.retry;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import saps.catalog.core.Catalog;

import saps.common.core.model.SapsImage;
import saps.common.core.model.SapsLandsatImage;
import saps.common.core.model.SapsUser;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.ImageTaskState;
import saps.common.core.model.enums.JobState;

public class CatalogUtils {

  private static final Logger LOGGER = Logger.getLogger(CatalogUtils.class);
  private static final int CATALOG_DEFAULT_SLEEP_SECONDS = 5;

  /**
   * This function gets tasks in specific state in Catalog.
   *
   * @param imageStore catalog component
   * @param state      specific state for get tasks
   * @return tasks in specific state
   */
  public static List<SapsImage> getTasks(Catalog imageStore, ImageTaskState state) {
    return imageStore.getTasksByState(state);
  }

  /**
   * This function updates task state in catalog component.
   *
   * @param imageStore catalog component
   * @param task       task to be updated
   * @return boolean representation reporting success (true) or failure (false) in
   *         update state task
   *         in catalog
   */
  public static void updateState(Catalog imageStore, SapsImage task) {
    imageStore.updateImageTask(task);
  }

  /**
   * This function gets tasks in processing state in catalog component.
   *
   * @param imageStore catalog component
   * @param message    information message
   * @return processing tasks list
   */
  public static List<SapsImage> getProcessingTasks(Catalog imageStore, String message) {
    ImageTaskState[] states = {
      ImageTaskState.DOWNLOADING, ImageTaskState.PREPROCESSING, ImageTaskState.RUNNING
    };

    LOGGER.info(message);
    return imageStore.getTasksByState(states);
  }

  /**
   * This function add new tuple in time stamp table and updates task time stamp.
   *
   * @param imageStore catalog component
   * @param task       task to be update
   */
  public static void addTimestampTask(Catalog imageStore, SapsImage task) {
    //review this method. take a look at the older version
    imageStore.addStateChangeTime(task.getTaskId(), task.getState(), task.getUpdateTime());
  }

  /**
   * This function adds new user.
   *
   * @param imageStore catalog component
   * @param userEmail  user email
   * @param userName   user name
   * @param userPass   user password
   * @param userState  user state
   * @param userNotify user notify
   * @param adminRole  administrator role
   * @param message    information message
   */
  public static void addNewUser(
      Catalog imageStore,
      String userEmail,
      String userName,
      String userPass,
      boolean userState,
      boolean userNotify,
      boolean adminRole,
      String message) {
    LOGGER.info(message);
    imageStore.addUser(userEmail, userName, userPass, userState, userNotify, adminRole);
  }

  /**
   * This function gets user information.
   *
   * @param imageStore catalog component
   * @param userEmail  user email
   * @param message    information message
   */
  public static SapsUser getUser(Catalog imageStore, String userEmail, String message) {
    LOGGER.info(message);
    return imageStore.getUserByEmail(userEmail);
  }

  /**
   * This function gets tasks in specific state in Catalog.
   *
   * @param imageStore catalog component
   * @param state      specific state for get tasks
   * @param page       page number
   * @param pageSize   page size
   * @return tasks in specific state
   */
  public static SapsUserJob addNewUserJob(
      Catalog imageStore,
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
      List<String> tasksIds,
      String message) {
    LOGGER.info(message);
    return imageStore.addJob(jobId, lowerLeftLatitude, lowerLeftLongitude, upperRightLatitude, 
    upperRightLongitude, userEmail, jobLabel, startDate, endDate, priority, tasksIds);
  }
  /**
  @param imageStore                 catalog component
  @param taskId                     task id
  @param jobId                      job id
  @param message                    trash. we don't need that anymore.
  */
  public static void insertJobTask(Catalog imageStore, String taskId, String jobId, String message) {
    LOGGER.info(message);
    imageStore.insertJobTask(taskId, jobId);
  }
  /**
   * This function adds new task.
   *
   * @param imageStore               catalog component
   * @param taskId                   task id
   * @param dataset                  task dataset
   * @param region                   task region
   * @param date                     task region
   * @param priority                 task priority
   * @param userEmail                user email that is creating task
   * @param inputdownloadingPhaseTag inputdownloading phase tag
   * @param preprocessingPhaseTag    preprocessing phase tag
   * @param processingPhaseTag       processing phase tag
   * @param message                  information message
   * @return new SAPS image
   */
  public static SapsImage addNewTask(
      Catalog imageStore, String taskId, String dataset, String region, Date date, int priority, String userEmail,
      String inputdownloadingPhaseTag, String digestInputdownloading, String preprocessingPhaseTag, String digestPreprocessing,
      String processingPhaseTag, String digestProcessing, String message) {
  
    LOGGER.info(message);
    return imageStore.addTask(taskId, dataset, region, date, priority, userEmail, inputdownloadingPhaseTag, 
    digestInputdownloading, preprocessingPhaseTag, digestPreprocessing, processingPhaseTag, digestProcessing);
  }

  /**
   * This function checks if we got a valid image
   * 
   * @param region  region specified by the user submission
   * @param date    date specified by the user submission
   * @param landsat landsat who (perhaps) got the image
   * @param message
   * @return boolean indicating if the image does exist or not
   * 
   */
  public static SapsLandsatImage validateLandsatImage(Catalog imageStore, String region, Date date, String message) {
    // SQL QUERY, if we got the image return TRUE, else return FALSE
    LOGGER.info(message);
    return imageStore.getLandsatImages(region, date);
  }

  /**
   * This function gets a specific task with id.
   *
   * @param taskId task id to be searched
   * @return SAPS image with task id informed
   */
  public static SapsImage getTaskById(Catalog imageStore, String taskId, String message) {
    LOGGER.info(message);
    return imageStore.getTaskById(taskId);
  }

  /**
   * This function gets archived task.
   *
   * @param imageStore               catalog component
   * @param region                   task region
   * @param initDate                 initial date
   * @param endDate                  end date
   * @param inputdownloadingPhaseTag inputdownloading phase tag
   * @param preprocessingPhaseTag    preprocessing phase tag
   * @param processingPhaseTag       processing phase tag
   * @param message                  information message
   * @return SAPS image list with archived state
   */
  public static List<SapsImage> getProcessedTasks(
      Catalog imageStore,
      String region,
      Date initDate,
      Date endDate,
      String inputdownloadingPhaseTag,
      String preprocessingPhaseTag,
      String processingPhaseTag,
      String message) {

    LOGGER.info(message);
    return imageStore.filterTasks(ImageTaskState.ARCHIVED, region, initDate, endDate, message, preprocessingPhaseTag, processingPhaseTag);
  }

  /**
   * This function get all tasks.
   *
   * @param imageStore catalog component
   * @return SAPS image list
   */
  public static List<SapsImage> getAllTasks(Catalog imageStore, String message) {
    LOGGER.info(message);
    return imageStore.getAllTasks();
  }

  /**
   * This functions gets all jobs from catalog.
   * 
   * @param imageStore       catalog component
   * @param search           search query
   * @param page             page number
   * @param size             page size
   * @param sortField        sort field
   * @param sortOrder        sort order
   * @param withoutTasks     if true, return all jobs without tasks
   * @param recoverOngoing   if true, return all ongoing jobs
   * @param recoverCompleted if true, return all completed jobs
   * @param message          information message
   * @return SAPS user job list
   */
  public static List<SapsUserJob> getUserJobs(Catalog imageStore, JobState state, String search, Integer page,
      Integer size, String sortField, String sortOrder, boolean withoutTasks, boolean recoverOngoing,
      boolean recoverCompleted, String message) {
    LOGGER.info(message);
    return imageStore.getUserJobs(state, search, page, size, sortField, sortOrder, withoutTasks, recoverOngoing, recoverCompleted);
  }

  /**
   * This function return the amount of jobs in catalog.
   * 
   * @param imageStore catalog component
   * @param search     search query
   * @param message    information message
   * @return SAPS image list
   */
  public static Integer getUserJobsCount(Catalog imageStore, JobState state, String search, boolean recoverOngoing,
      boolean recoverCompleted, String message) {
    LOGGER.info(message);
    return imageStore.getUserJobsCount(state, search, recoverOngoing, recoverCompleted);
  }

  /**
   * This function gets all tasks from a specific job.
   * 
   * @param imageStore       catalog component
   * @param jobId            job id
   * @param state            task state
   * @param search           search query
   * @param page             page number
   * @param size             page size
   * @param sortField        sort field
   * @param sortOrder        sort order
   * @param recoverOngoing   if true, return all ongoing tasks
   * @param recoverCompleted if true, return all completed tasks
   * @param message          information message
   * @return SAPS image list
   */
  public static List<SapsImage> getUserJobTasks(Catalog imageStore, String jobId, ImageTaskState state, String search,
      Integer page, Integer size, String sortField, String sortOrder, boolean recoverOngoing,
      boolean recoverCompleted, String message) {
    LOGGER.info(message);
    return imageStore.getUserJobTasks(jobId, state, search, page, size, sortField, sortOrder, recoverOngoing, recoverCompleted);
  }

  /**
   * This function return the amount of tasks in catalog.
   * 
   * @param imageStore       catalog component
   * @param jobId            job id
   * @param state            task state
   * @param search           search query
   * @param recoverOngoing   if true, return all ongoing tasks count
   * @param recoverCompleted if true, return all completed tasks count
   * @param message          information message
   * @return SAPS image list
   */
  public static Integer getUserJobTasksCount(Catalog imageStore, String jobId, ImageTaskState state, String search,
      boolean recoverOngoing, boolean recoverCompleted, String message) {
    LOGGER.info(message);
    return imageStore.getUserJobTasksCount(jobId, state, search, recoverOngoing, recoverCompleted);
  }

  /**
   * This function updates a job state.
   * 
   * @param imageStore catalog component
   * @param jobId      job id
   * @param state      job state
   * @param message    information message
   * @return SAPS image list
   */
  public static void updateUserJob(Catalog imageStore, String jobId, JobState state, String message) {
    LOGGER.info(message);
    imageStore.updateUserJob(jobId, state); 
  }

}

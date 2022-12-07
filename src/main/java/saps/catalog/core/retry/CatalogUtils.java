/* (C)2020 */
package saps.catalog.core.retry;

import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import saps.catalog.core.Catalog;
import saps.catalog.core.retry.catalog.AddNewTask;
import saps.catalog.core.retry.catalog.AddNewUser;
import saps.catalog.core.retry.catalog.AddTimestampRetry;
import saps.catalog.core.retry.catalog.CatalogRetry;
import saps.catalog.core.retry.catalog.GetAllTasks;
import saps.catalog.core.retry.catalog.GetProcessedTasks;
import saps.catalog.core.retry.catalog.GetProcessingTasksRetry;
import saps.catalog.core.retry.catalog.GetTaskById;
import saps.catalog.core.retry.catalog.GetTasksRetry;
import saps.catalog.core.retry.catalog.GetUser;
import saps.catalog.core.retry.catalog.UpdateTaskRetry;
import saps.catalog.core.retry.catalog.exceptions.CatalogRetryException;

import saps.catalog.core.retry.catalog.GetTasksOngoingWithPagination;
import saps.catalog.core.retry.catalog.GetTasksCompletedWithPagination;
import saps.catalog.core.retry.catalog.GetCountOngoingTasks;
import saps.catalog.core.retry.catalog.GetCountCompletedTasks;

import saps.common.core.model.SapsImage;
import saps.common.core.model.SapsUser;
import saps.common.core.model.enums.ImageTaskState;

public class CatalogUtils {

  private static final Logger LOGGER = Logger.getLogger(CatalogUtils.class);
  private static final int CATALOG_DEFAULT_SLEEP_SECONDS = 5;

  /**
   * This function tries countless times to successfully execute the passed
   * function.
   *
   * @param <T>            Return type
   * @param function       Function passed for execute
   * @param sleepInSeconds Time sleep in seconds (case fail)
   * @param message        Information message about function passed
   * @return Function return
   */
  @SuppressWarnings("unchecked")
  private static <T> T retry(CatalogRetry<?> function, int sleepInSeconds, String message) {
    LOGGER.info(
        "[Retry Catalog function] Trying "
            + message
            + " using "
            + sleepInSeconds
            + " seconds with time sleep");

    while (true) {
      try {
        return (T) function.run();
      } catch (CatalogRetryException e) {
        LOGGER.error("Failed while " + message, e);
      }

      try {
        LOGGER.info("Sleeping for " + sleepInSeconds + " seconds");
        Thread.sleep(Long.valueOf(sleepInSeconds) * 1000);
      } catch (InterruptedException e) {
        LOGGER.error("Failed while " + message, e);
      }
    }
  }

  /**
   * This function gets tasks in specific state in Catalog.
   *
   * @param imageStore catalog component
   * @param state      specific state for get tasks
   * @return tasks in specific state
   */
  public static List<SapsImage> getTasks(Catalog imageStore, ImageTaskState state) {
    return retry(
        new GetTasksRetry(imageStore, state),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        "gets tasks with " + state.getValue() + " state");
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
  public static boolean updateState(Catalog imageStore, SapsImage task) {
    return retry(
        new UpdateTaskRetry(imageStore, task),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        "update task [" + task.getTaskId() + " state]");
  }

  /**
   * This function gets tasks in processing state in catalog component.
   *
   * @param imageStore catalog component
   * @param message    information message
   * @return processing tasks list
   */
  public static List<SapsImage> getProcessingTasks(Catalog imageStore, String message) {
    return retry(new GetProcessingTasksRetry(imageStore), CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }

  /**
   * This function add new tuple in time stamp table and updates task time stamp.
   *
   * @param imageStore catalog component
   * @param task       task to be update
   */
  public static void addTimestampTask(Catalog imageStore, SapsImage task) {
    retry(
        new AddTimestampRetry(imageStore, task),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        "add timestamp to task [" + task.getTaskId() + "]");
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
    retry(
        new AddNewUser(imageStore, userEmail, userName, userPass, userState, userNotify, adminRole),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        message);
  }

  /**
   * This function gets user information.
   *
   * @param imageStore catalog component
   * @param userEmail  user email
   * @param message    information message
   */
  public static SapsUser getUser(Catalog imageStore, String userEmail, String message) {
    return retry(new GetUser(imageStore, userEmail), CATALOG_DEFAULT_SLEEP_SECONDS, message);
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
      Catalog imageStore,
      String taskId,
      String dataset,
      String region,
      Date date,
      int priority,
      String userEmail,
      String inputdownloadingPhaseTag,
      String digestInputdownloading,
      String preprocessingPhaseTag,
      String digestPreprocessing,
      String processingPhaseTag,
      String digestProcessing,
      String message) {
    return retry(
        new AddNewTask(
            imageStore,
            taskId,
            dataset,
            region,
            date,
            priority,
            userEmail,
            inputdownloadingPhaseTag,
            digestInputdownloading,
            preprocessingPhaseTag,
            digestPreprocessing,
            processingPhaseTag,
            digestProcessing),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        message);
  }

  /**
   * This function gets a specific task with id.
   *
   * @param taskId task id to be searched
   * @return SAPS image with task id informed
   */
  public static SapsImage getTaskById(Catalog imageStore, String taskId, String message) {
    return retry(new GetTaskById(imageStore, taskId), CATALOG_DEFAULT_SLEEP_SECONDS, message);
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

    return retry(
        new GetProcessedTasks(
            imageStore,
            region,
            initDate,
            endDate,
            inputdownloadingPhaseTag,
            preprocessingPhaseTag,
            processingPhaseTag),
        CATALOG_DEFAULT_SLEEP_SECONDS,
        message);
  }

  /**
   * This function get all tasks.
   *
   * @param imageStore catalog component
   * @return SAPS image list
   */
  public static List<SapsImage> getAllTasks(Catalog imageStore, String message) {
    return retry(new GetAllTasks(imageStore), CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }

  /**
   * This function return all the tasks that have not finished all the pipeline.
   * @param imageStore  catalog component
   * @param search      search query
   * @param page        pagination page number
   * @param size        pagination page size
   * @param sortField   sort field
   * @param sortOrder   type of sort order
   * @param message     information message
   * @return SAPS image list
   */
  public static List<SapsImage> getTasksOngoingWithPagination(Catalog imageStore, String search, Integer page,
      Integer size, String sortField, String sortOrder, String message) {
    return retry(new GetTasksOngoingWithPagination(imageStore, search, page, size, sortField, sortOrder),
        CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }

  /**
   * This function return all the tasks that have finished all the pipeline.
   * @param imageStore  catalog component
   * @param search      search query
   * @param page        pagination page number
   * @param size        pagination page size
   * @param sortField   sort field
   * @param sortOrder   type of sort order
   * @param message     information message
   * @return SAPS image list
   */
  public static List<SapsImage> getTasksCompletedWithPagination(Catalog imageStore, String search, Integer page,
      Integer size, String sortField, String sortOrder, String message) {
    return retry(new GetTasksCompletedWithPagination(imageStore, search, page, size, sortField, sortOrder),
        CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }

  /**
   * This function return the amount of tasks that have not finished all the pipeline.
   * @param imageStore  catalog component
   * @param search      search query
   * @param message     information message
   * @return SAPS image list
   */
  public static Integer getCountOngoingTasks(Catalog imageStore, String search, String message) {
    return retry(new GetCountOngoingTasks(imageStore, search), CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }

  /**
   * This function return the amount of tasks that have finished all the pipeline.
   * @param imageStore  catalog component
   * @param search      search query
   * @param message     information message
   * @return SAPS image list
   */
  public static Integer getCountCompletedTasks(Catalog imageStore, String search, String message) {
    return retry(new GetCountCompletedTasks(imageStore, search), CATALOG_DEFAULT_SLEEP_SECONDS, message);
  }
}

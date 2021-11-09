package org.springframework.batch.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static org.springframework.util.Assert.notNull;

import java.util.Collection;
import java.util.Date;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

/**
 * Uses MongoTemplate to perform CRUD on Springbatch's Step Execution Data to
 * Mongo DB. <br/>MongoTemplate needs to be set as a property during bean definition
 * 
 * @author Baruch S.
 * @authoer vfouzdar
 */
@Repository
public class MongoStepExecutionDao extends AbstractMongoDao implements StepExecutionDao {
    
    private static final Logger LOG = LoggerFactory.getLogger(MongoStepExecutionDao.class);
    
	private MongoTemplate mongoTemplate;
	    
    public void setMongoTemplate(MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
	}

	@PostConstruct
    public void init() {
	    // db.StepExecution.createIndex( {jobExecutionId : 1});
        getCollection().createIndex(new BasicDBObject(JOB_EXECUTION_ID_KEY, 1L));
        getCollection().createIndex(new Document(STEP_EXECUTION_ID_KEY, 1).append(JOB_EXECUTION_ID_KEY, 1),
                new IndexOptions());

    }

    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");

        validateStepExecution(stepExecution);

        stepExecution.setId(getNextId(StepExecution.class.getSimpleName(), mongoTemplate));
        stepExecution.incrementVersion(); // should be 0 now
        Bson object = toDbObjectWithoutVersion(stepExecution);
        object = combine(object, set(VERSION_KEY, stepExecution.getVersion()));
        getCollection().updateOne(eq(STEP_EXECUTION_ID_KEY, stepExecution.getId()), object, new UpdateOptions().upsert(true));

    }

    private Bson toDbObjectWithoutVersion(StepExecution stepExecution) {
        return combine(
                set(STEP_EXECUTION_ID_KEY, stepExecution.getId()),
                set(STEP_NAME_KEY, stepExecution.getStepName()),
                set(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId()),
                set(START_TIME_KEY, stepExecution.getStartTime()),
                set(END_TIME_KEY, stepExecution.getEndTime()),
                set(STATUS_KEY, stepExecution.getStatus().toString()),
                set(COMMIT_COUNT_KEY, stepExecution.getCommitCount()),
                set(READ_COUNT_KEY, stepExecution.getReadCount()),
                set(FILTER_COUT_KEY, stepExecution.getFilterCount()),
                set(WRITE_COUNT_KEY, stepExecution.getWriteCount()),
                set(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode()),
                set(EXIT_MESSAGE_KEY, stepExecution.getExitStatus().getExitDescription()),
                set(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount()),
                set(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount()),
                set(PROCESS_SKIP_COUT_KEY, stepExecution.getProcessSkipCount()),
                set(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount()),
                set(LAST_UPDATED_KEY, stepExecution.getLastUpdated())
    );
    }

    public synchronized void updateStepExecution(StepExecution stepExecution) {
        // Attempt to prevent concurrent modification errors by blocking here if
        // someone is already trying to do it.
        Integer currentVersion = stepExecution.getVersion();
        Integer newVersion = currentVersion + 1;
        Bson object = toDbObjectWithoutVersion(stepExecution);
        object = combine(object, set(VERSION_KEY, newVersion));
        getCollection().updateOne(combine(
                eq(STEP_EXECUTION_ID_KEY, stepExecution.getId()),
                eq(VERSION_KEY, currentVersion)),
                object, new UpdateOptions().upsert(true));

        // Avoid concurrent modifications...
//        DBObject lastError = mongoTemplate.getDb().getLastError();
//        if (!((Boolean) lastError.get(UPDATED_EXISTING_STATUS))) {
//            LOG.error("Update returned status {}", lastError);
//            DBObject existingStepExecution = getCollection().findOne(stepExecutionIdObj(stepExecution.getId()), new BasicDBObject(VERSION_KEY, 1));
//            if (existingStepExecution == null) {
//                throw new IllegalArgumentException("Can't update this stepExecution, it was never saved.");
//            }
//            Integer curentVersion = ((Integer) existingStepExecution.get(VERSION_KEY));
//            throw new OptimisticLockingFailureException("Attempt to update job execution id="
//                    + stepExecution.getId() + " with wrong version (" + currentVersion
//                    + "), where current version is " + curentVersion);
//        }

        stepExecution.incrementVersion();
    }


    static BasicDBObject stepExecutionIdObj(Long id) {
        return new BasicDBObject(STEP_EXECUTION_ID_KEY, id);
    }


    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        return mapStepExecution(getCollection().find(combine(
                eq(STEP_EXECUTION_ID_KEY, stepExecutionId),
                eq(JOB_EXECUTION_ID_KEY, jobExecution.getId()))).first(), jobExecution);
    }

    private StepExecution mapStepExecution(Document object, JobExecution jobExecution) {
        if (object == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution((String) object.get(STEP_NAME_KEY), jobExecution, ((Long) object.get(STEP_EXECUTION_ID_KEY)));
        stepExecution.setStartTime((Date) object.get(START_TIME_KEY));
        stepExecution.setEndTime((Date) object.get(END_TIME_KEY));
        stepExecution.setStatus(BatchStatus.valueOf((String) object.get(STATUS_KEY)));
        stepExecution.setCommitCount((Integer) object.get(COMMIT_COUNT_KEY));
        stepExecution.setReadCount((Integer) object.get(READ_COUNT_KEY));
        stepExecution.setFilterCount((Integer) object.get(FILTER_COUT_KEY));
        stepExecution.setWriteCount((Integer) object.get(WRITE_COUNT_KEY));
        stepExecution.setExitStatus(new ExitStatus((String) object.get(EXIT_CODE_KEY), ((String) object.get(EXIT_MESSAGE_KEY))));
        stepExecution.setReadSkipCount((Integer) object.get(READ_SKIP_COUNT_KEY));
        stepExecution.setWriteSkipCount((Integer) object.get(WRITE_SKIP_COUNT_KEY));
        stepExecution.setProcessSkipCount((Integer) object.get(PROCESS_SKIP_COUT_KEY));
        stepExecution.setRollbackCount((Integer) object.get(ROLLBACK_COUNT_KEY));
        stepExecution.setLastUpdated((Date) object.get(LAST_UPDATED_KEY));
        stepExecution.setVersion((Integer) object.get(VERSION_KEY));
        return stepExecution;

    }

    public void addStepExecutions(JobExecution jobExecution) {
        FindIterable<Document> findIter =  getCollection().find(jobExecutionIdObj(jobExecution.getId())).sort(stepExecutionIdObj(1L));
        MongoCursor<Document> stepsCoursor = findIter.iterator();
        while (stepsCoursor.hasNext()) {
            Document stepObject = stepsCoursor.next();
            //Calls constructor of StepExecution, which adds the step; Wow, that's unclear code!
            mapStepExecution(stepObject, jobExecution);
        }
    }

    
    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(StepExecution.class.getSimpleName());
    }


    private void validateStepExecution(StepExecution stepExecution) {
        notNull(stepExecution);
        notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

	@Override
	public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
		Assert.notNull(stepExecutions,"Attempt to save an null collect of step executions");
		for (StepExecution stepExecution: stepExecutions) {
			saveStepExecution(stepExecution);
		}
		
	}

}

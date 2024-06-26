package org.springframework.batch.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

/**
 * Uses MongoTemplate to perform CRUD on Springbatch's Execution context to Mongo DB. <br/>
 * MongoTemplate needs to be set as a property during bean definition
 * 
 * @author Baruch S.
 * @author vfouzdar
 */
@Repository
public class MongoExecutionContextDao extends AbstractMongoDao implements ExecutionContextDao {

    private static final Logger LOG = LoggerFactory.getLogger(MongoExecutionContextDao.class);

    /**
     * mongoTemplate is used to CRUD Job execution data in Mongo db. This bean needs to be set during bean definition
     * for MongoExecutionContextDao
     */
    private MongoTemplate       mongoTemplate;

    public void setMongoTemplate(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void init() {
        // db.ExecutionContext.createIndex( {jobExecutionId : 1});
        getCollection().createIndex(jobExecutionIdObj(1L));
        getCollection().createIndex(new Document(STEP_EXECUTION_ID_KEY, 1).append(JOB_EXECUTION_ID_KEY, 1),
                new IndexOptions());
    }

    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return getExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId());
    }

    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return getExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId());
    }

    public void saveExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void saveExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    public void updateExecutionContext(JobExecution jobExecution) {
        saveOrUpdateExecutionContext(JOB_EXECUTION_ID_KEY, jobExecution.getId(), jobExecution.getExecutionContext());
    }

    public void updateExecutionContext(StepExecution stepExecution) {
        saveOrUpdateExecutionContext(STEP_EXECUTION_ID_KEY, stepExecution.getId(), stepExecution.getExecutionContext());
    }

    private void saveOrUpdateExecutionContext(String executionIdKey, Long executionId,
            ExecutionContext executionContext) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        Assert.notNull(executionContext, "The ExecutionContext must not be null.");

        DBObject dbObject = new BasicDBObject(executionIdKey, executionId);
        List<Bson> updates = new ArrayList<>();
        updates.add(set(executionIdKey, executionId));
        for (Map.Entry<String, Object> entry : executionContext.entrySet()) {
            Object value = entry.getValue();
            String key = entry.getKey();
            dbObject.put(key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING), value);
            if (value instanceof BigDecimal || value instanceof BigInteger) {
                dbObject.put(key + TYPE_SUFFIX, value.getClass().getName());
                updates.add(set(key + TYPE_SUFFIX, value.getClass().getName()));
            } else {
                updates.add(set(key.replaceAll(DOT_STRING, DOT_ESCAPE_STRING), value));
            }
        }
        Bson updateStatement = combine(updates);
        LOG.debug("Making update with statement: " + updateStatement);
        getCollection().updateOne(eq(executionIdKey, executionId), updateStatement, new UpdateOptions().upsert(true));
//		getCollection().updateMany(new BasicDBObject(executionIdKey, executionId),
//				dbObject, true, false);
    }

    @SuppressWarnings({ "unchecked" })
    private ExecutionContext getExecutionContext(String executionIdKey, Long executionId) {
        Assert.notNull(executionId, "ExecutionId must not be null.");
        Document result = getCollection().find(new BasicDBObject(executionIdKey, executionId)).first();
        ExecutionContext executionContext = new ExecutionContext();
        if (result != null) {
            result.remove(executionIdKey);
            removeSystemFields(result);
            for (String key : result.keySet()) {
                Object value = result.get(key);
                String type = (String) result.get(key + TYPE_SUFFIX);
                if (type != null && Number.class.isAssignableFrom(value.getClass())) {
                    try {
                        value = NumberUtils.convertNumberToTargetClass((Number) value,
                                (Class<? extends Number>) Class.forName(type));
                    } catch (Exception e) {
                        LOG.warn("Failed to convert {} to {}", key, type);
                    }
                }
                // Mongo db does not allow key name with "." character.
                executionContext.put(key.replaceAll(DOT_ESCAPE_STRING, DOT_STRING), value);
            }
        }
        return executionContext;
    }

    protected MongoCollection<Document> getCollection() {
        return mongoTemplate.getCollection(ExecutionContext.class.getSimpleName());
    }

    @Override
    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
        for (StepExecution stepExecution : stepExecutions) {
            saveExecutionContext(stepExecution);
            saveExecutionContext(stepExecution.getJobExecution());
        }

    }

}
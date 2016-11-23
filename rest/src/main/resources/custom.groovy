import com.sai.strawberry.api.EventStreamConfig
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query

// Available variables and their types.
// config: EventStreamConfig
// jsonIn: Map
// slowZoneMongoTemplate: MongoTemplate
// fastZoneMongoTemplate: MongoTemplate
// cache: Map<String, List<Map>>
// You must return a value back.
def recent20TransactionsForCard(cardNumber, amount) {
    def mongoTemplate = slowZoneMongoTemplate as MongoTemplate
    def conf = config as EventStreamConfig
    def query = new Query()
    query.addCriteria(Criteria.where("cardNumber").is(cardNumber))
    query.with(new Sort(Sort.Direction.DESC, "timestamp")).limit(20)
    query.fields().include("amount")
    def docs = mongoTemplate.find(query, Map, conf.configId)
    def total = docs.collect { it -> it['amount'] }.sum()
    def avg = total / docs.size()
    return (amount >= (avg * 2))
}

return recent20TransactionsForCard(jsonIn['cardNumber'], jsonIn['amount'])
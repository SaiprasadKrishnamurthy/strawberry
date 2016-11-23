// Available variables and their types.
// config: EventStreamConfig
// jsonIn: Map
// slowZoneMongoTemplate: MongoTemplate
// fastZoneMongoTemplate: MongoTemplate
// cache: Map<String, List<Map>>
// You must return a value back.
println("I'm inside the script now --- " + jsonIn)
return [customAttr: "customAttrValue"]
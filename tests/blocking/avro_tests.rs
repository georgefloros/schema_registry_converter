extern crate schema_registry_converter;
use crate::blocking::avro_consumer::{consume_avro, DeserializedAvroRecord};
use crate::blocking::kafka_producer::get_producer;
use apache_avro::types::Value;

use rand::Rng;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::blocking::{
    avro::AvroEncoder, schema_registry::get_schema_by_subject,
};
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde_json::json;

fn get_schema_registry_url() -> String {
    String::from("http://localhost:8081")
}

fn get_brokers() -> &'static str {
    "127.0.0.1:9092"
}

fn get_heartbeat_schema() -> SuppliedSchema {
    SuppliedSchema {
        name: Some(String::from("nl.openweb.data.Heartbeat")),
        schema_type: SchemaType::Avro,
        schema: String::from(
            r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
        ),
        references: vec![],
    }
}

fn test_beat_value(key_value: i64, value_value: i64) -> Box<dyn Fn(DeserializedAvroRecord)> {
    Box::new(move |rec: DeserializedAvroRecord| {
        println!("testing record {:#?}", rec);
        let key_values = match rec.key {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let beat_key = match &key_values[0] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value of while that was expected"),
        };
        assert_eq!(&key_value, beat_key, "compare key values");
        let value_values = match rec.value {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let beat_value = match &value_values[0] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value of while that was expected"),
        };
        assert_eq!(&value_value, beat_value, "compare value values");
    })
}

fn do_avro_test(
    topic: &str,
    key_strategy: SubjectNameStrategy,
    value_strategy: SubjectNameStrategy,
) {
    let mut rng = rand::thread_rng();
    let key_value = rng.gen::<i64>();
    let value_value = rng.gen::<i64>();
    let mut producer = get_producer(get_brokers(), get_schema_registry_url());
    let key_values = vec![("beat", Value::Long(key_value))];
    let value_values = vec![("beat", Value::Long(value_value))];
    producer.send_avro(
        topic,
        key_values,
        value_values,
        key_strategy,
        value_strategy,
    );
    consume_avro(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        true,
        test_beat_value(key_value, value_value),
    )
}

#[test]
fn test1_topic_name_strategy_with_schema() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        String::from(topic),
        true,
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
        String::from(topic),
        false,
        get_heartbeat_schema(),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test2_record_name_strategy_with_schema() {
    let topic = "recordnamestrategy";
    let key_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    let value_strategy = SubjectNameStrategy::RecordNameStrategyWithSchema(get_heartbeat_schema());
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test3_topic_record_name_strategy_with_schema() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        String::from(topic),
        get_heartbeat_schema(),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategyWithSchema(
        String::from(topic),
        get_heartbeat_schema(),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test4_topic_name_strategy_schema_now_available() {
    let topic = "topicnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicNameStrategy(String::from(topic), true);
    let value_strategy = SubjectNameStrategy::TopicNameStrategy(String::from(topic), false);
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test5_record_name_strategy_schema_now_available() {
    let topic = "recordnamestrategy";
    let key_strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    let value_strategy =
        SubjectNameStrategy::RecordNameStrategy(String::from("nl.openweb.data.Heartbeat"));
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test6_topic_record_name_strategy_schema_now_available() {
    let topic = "topicrecordnamestrategy";
    let key_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        String::from(topic),
        String::from("nl.openweb.data.Heartbeat"),
    );
    let value_strategy = SubjectNameStrategy::TopicRecordNameStrategy(
        String::from(topic),
        String::from("nl.openweb.data.Heartbeat"),
    );
    do_avro_test(topic, key_strategy, value_strategy)
}

#[test]
fn test7_test_avro_from_java_test_app() {
    let topic = "testavro";
    let test = Box::new(move |rec: DeserializedAvroRecord| {
        println!("testing record {:#?}", rec);
        match rec.key {
            Value::String(s) => assert_eq!("testkey", s, "check string key"),
            _ => panic!("Keys wasn't a string"),
        };
        let value_values = match rec.value {
            Value::Record(v) => v,
            _ => panic!("Not a record, while only only those expected"),
        };
        let id_key = match &value_values[0] {
            (_id, Value::Fixed(16, _v)) => _id,
            _ => panic!("Not a fixed value of 16 bytes while that was expected"),
        };
        assert_eq!("id", id_key, "expected id key to be id");
        let enum_value = match &value_values[1] {
            (_id, Value::Enum(0, v)) => v,
            _ => panic!("Not an enum value for by while that was expected"),
        };
        assert_eq!("Java", enum_value, "expect message from Java");
        let counter_value = match &value_values[2] {
            (_id, Value::Long(v)) => v,
            _ => panic!("Not a long value for counter while that was expected"),
        };
        assert_eq!(&1i64, counter_value, "counter is 1");
        let input_value = match &value_values[3] {
            (_id, Value::Union(_, v)) => v,
            _ => panic!("Not an unions value for input while that was expected"),
        };
        assert_eq!(
            &Box::new(Value::String(String::from("String"))),
            input_value,
            "Optional string is string"
        );
        let results = match &value_values[4] {
            (_id, Value::Array(v)) => v,
            _ => panic!("Not an array value for results while that was expected"),
        };
        let result = match results.get(0).expect("one item to be present") {
            Value::Record(v) => v,
            _ => panic!("Not record for first of results while that was expected"),
        };
        let up_result = match &result[0] {
            (_id, Value::String(v)) => v,
            _ => panic!("First result value wasn't a string"),
        };
        assert_eq!("STRING", up_result, "expected upper case string");
        let down_result = match &result[1] {
            (_id, Value::String(v)) => v,
            _ => panic!("Second result value wasn't a string"),
        };
        assert_eq!("string", down_result, "expected upper case string");
    });
    consume_avro(
        get_brokers(),
        "test",
        get_schema_registry_url(),
        &[topic],
        false,
        test,
    )
}
#[test]
fn test_br() {
    // let _v = match get_schema_by_subject(
    //     &SrSettings::new(String::from("http://localhost:8081")),
    //     &key_strategy,
    // ) {
    //     Ok(registered_schema) => registered_schema,
    //     Err(e) => panic!("Failed to get schema: {:?}", e),
    // };
    // // println!("registered_schema = {:?}", _v);
    // println!("Start testBR iterations");
    let encoder = AvroEncoder::new(SrSettings::new(String::from("http://localhost:8081")));

    // for i in 0..1 {
    // println!("Running testBR iteration = {:?}", i);
    let event = json!({"status":"SUCCESS","requestDto":{"paymentMethodDto":"CC_TEMP_TOKEN","paymentType":"TOPUP","paymentToken":"81994313-1b11-44ca-8ead-1858b4601943","walletTransactions":[{"currency":"EUR","amount":"1","transactionType":"AMOUNT","reservationId":null,"referenceNumber":"711000000033067347","data":"{\"transactionId\":\"711000000033067347\",\"paymentMethod\":\"CC_TEMP_TOKEN\",\"paymentToken\":\"81994313-1b11-44ca-8ead-1858b4601943\",\"customerId\":\"ID-1741\"}"}],"sessionToken":"4a6b8c5e-bd7e-4200-8ece-8230f71909f3","currency":"EUR","amount":100.01,"ipAddress":"127.0.0.1","accountId":"ID-1361","customerId":"ID-1741","cardTokenDetailsDto":null,"externalMPIDto":{"eci":"5","cavv":"ak5uWXJmRXhZV2pqa00wVkJCWjQ=","dsTransID":"163d96f9-7572-4df5-a3eb-098cb55bdcab","challengePreference":"None","exemptionRequestReason":"None"},"rememberCard":true},"responseDto":{"paymentMethodDto":"CC_TEMP_TOKEN","paymentType":"TOPUP","paymentToken":"81994313-1b11-44ca-8ead-1858b4601943","sessionToken":"4a6b8c5e-bd7e-4200-8ece-8230f71909f3","status":"SUCCESS","transactionStatus":"APPROVED","transactionType":"Sale","transactionId":"711000000033067347","internalRequestId":"947203978","customData":"","errCode":0,"reason":"","merchantId":"1721538546134299870","merchantSiteId":"244288","version":"1.0","clientRequestId":"ID-1741","orderId":"425988788","gwErrorCode":0,"gwExtendedErrorCode":0,"issuerDeclineCode":"","issuerDeclineReason":"","externalTransactionId":"","authCode":"111177","externalSchemeTransactionId":"","merchantAdviceCode":""},"problemDetailsDto":null});

    let key_strategy = SubjectNameStrategy::RecordNameStrategy(String::from(
        "eu.qualco.kite.avro.wallet.events.VPOSActionStatusEvent",
    ));

    let _bytes = match encoder.encode_struct(event, &key_strategy) {
        Ok(v) => v,
        Err(e) => {
            panic!("Failed to encode struct: {:?}", e);
        }
    };

    let event_c = json!({"data":{"step":"CUSTOMER","additionalContacts":["sss.doe@example2.com","+306990230113"],"birthPlace":"Athens","bornOn":1710427852606_i64,"contactAddress":{"city":"Athens","country":"GR","id":"test","postalCode":"12505","region":"karditsa","street":"Street 21","streetNumber":"21"},"documents":[{"expiresOn":1710427852606_i64,"id":" ","issuedOn":1710427852606_i64,"issuer":"Papagou P.D.","issuingCountry":"GR","number":"LR066069","type":"IDENTITY_CARD"},{"expiresOn":1710427852606_i64,"id":" ","issuedOn":1710427852606_i64,"issuer":"Papagou P.D.","issuingCountry":"GR","number":"PASTEST7","type":"PASSPORT"}],"email":"sss.doe@example.com","fatherName":"Jdack","fatherNameLatin":"Jadck","firstName":"sss","firstNameLatin":"sss","gender":"MALE","lastName":"Doe","lastNameLatin":"Doe","mobilePhoneNumber":"+306971234123","motherName":"Jane","motherNameLatin":"Jane","residenceAddress":{"city":"Athens","country":"GR","id":" ","postalCode":"12345","region":"Papagou","street":"Street 23","streetNumber":"23"},"tin":"999405119","settings":[{"category":"SEARCH_TERM","setting":"EMAIL","value":"\"true\""}],"processInstanceId":"string","clientReferenceId":"clientTest"}});

    let key_strategy_c = SubjectNameStrategy::RecordNameStrategy(String::from(
        "eu.qualco.kite.avro.customer.events.CreateCustomerEvent",
    ));

    let _bytes_c = match encoder.encode_struct(event_c, &key_strategy_c) {
        Ok(v) => v,
        Err(e) => {
            panic!("Failed to encode struct: {:?}", e);
        }
    };
    // }
    assert_eq!(2 + 2, 4);
}

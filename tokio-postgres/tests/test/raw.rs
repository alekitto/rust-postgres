use crate::connect;
use futures::TryStreamExt;
use postgres_protocol::message::backend::Message;
use tokio_postgres::raw::{prepare, simple_query, SimpleColumn, SimpleQueryRow};
use tokio_postgres::types::Type;
use tokio_postgres::Error;

#[cfg(feature = "raw")]
#[tokio::test]
async fn t_simple_query() {
    let client = connect("user=postgres").await;

    let messages: Vec<Message> = simple_query::<Error>(
        &client,
        "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('steven'), ('joe');
            SELECT * FROM foo ORDER BY id;",
    )
    .unwrap()
    .try_collect()
    .await
    .unwrap();

    assert_eq!(messages.len(), 7);

    let mut itr = messages.into_iter();
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::RowDescription(body) => {
            let columns = SimpleColumn::from_row_description_body(body).unwrap();
            assert_eq!(columns[0].name(), "id");
            assert_eq!(columns[0].type_(), &Some(Type::INT4));
            assert_eq!(columns[1].name(), "name");
            assert_eq!(columns[1].type_(), &Some(Type::TEXT));
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = SimpleQueryRow::new(body).unwrap();
            assert_eq!(row.try_get(0).unwrap(), Some("1"));
            assert_eq!(row.try_get(1).unwrap(), Some("steven"));
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = SimpleQueryRow::new(body).unwrap();
            assert_eq!(row.try_get(0).unwrap(), Some("2"));
            assert_eq!(row.try_get(1).unwrap(), Some("joe"));
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
}

#[cfg(feature = "raw")]
#[tokio::test]
async fn query_prepare() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)")
        .await
        .unwrap();

    let insert = prepare::<tokio_postgres::Error>(
        &client,
        "INSERT INTO foo (name) VALUES ($1), ($2)",
        "i",
        &[],
    )
    .await;
    let select =
        prepare::<tokio_postgres::Error>(&client, "SELECT id, name FROM foo ORDER BY id", "s", &[])
            .await;

    assert!(matches!(insert, Ok(_)));
    assert!(matches!(select, Ok(_)));
}

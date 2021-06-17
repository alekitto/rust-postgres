use crate::connect;
use bytes::BytesMut;
use futures::TryStreamExt;
use postgres_protocol::message::backend::Message;
use std::convert::TryInto;
use tokio_postgres::raw::{
    bind, execute, prepare, simple_query, sync, Row, SimpleColumn, SimpleQueryRow,
};
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
    );
    let select =
        prepare::<tokio_postgres::Error>(&client, "SELECT id, name FROM foo ORDER BY id", "s", &[]);

    assert!(matches!(insert, Ok(_)));
    assert!(matches!(select, Ok(_)));
}

#[cfg(feature = "raw")]
#[tokio::test]
async fn query_bind() {
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
    .unwrap();

    let portal = bind::<&[Option<BytesMut>; 2], tokio_postgres::Error>(
        &client,
        insert,
        "i",
        &[0],
        &[Some(BytesMut::from("foo")), Some(BytesMut::from("foo"))],
        &[0],
    );

    assert!(matches!(portal, Ok(_)));
}

#[cfg(feature = "raw")]
#[tokio::test]
async fn query_execute_no_data() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)")
        .await
        .unwrap();

    let insert = prepare::<tokio_postgres::Error>(
        &client,
        "INSERT INTO foo (name) VALUES ($1), ($2)",
        "i",
        &[Type::INT4.oid()],
    )
    .unwrap();

    let id = 1_i32.to_be_bytes();
    let portal = bind::<&[Option<BytesMut>; 2], tokio_postgres::Error>(
        &client,
        insert,
        "i",
        &[1, 0],
        &[
            Some(BytesMut::from(id.as_ref())),
            Some(BytesMut::from("bar")),
        ],
        &[],
    )
    .unwrap();

    execute::<tokio_postgres::Error>(&client, &portal, 0).unwrap();
    let messages: Vec<Message> = sync::<tokio_postgres::Error>(&client)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 4);

    let mut itr = messages.into_iter();
    match itr.next().unwrap() {
        Message::ParseComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::BindComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::ReadyForQuery(_) => {}
        _ => panic!("unexpected message"),
    }
}

#[cfg(feature = "raw")]
#[tokio::test]
async fn query_execute_with_data_result_binary_format() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT); INSERT INTO foo (name) VALUES ('foo'), ('foobar');")
        .await
        .unwrap();

    let select =
        prepare::<tokio_postgres::Error>(&client, "SELECT * FROM foo WHERE name LIKE $1", "", &[])
            .unwrap();

    let portal = bind::<&[Option<BytesMut>; 1], tokio_postgres::Error>(
        &client,
        select,
        "",
        &[0],
        &[Some(BytesMut::from("foo%"))],
        &[1],
    )
    .unwrap();

    execute::<tokio_postgres::Error>(&client, &portal, 0).unwrap();
    let messages: Vec<Message> = sync::<tokio_postgres::Error>(&client)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 6);

    let mut itr = messages.into_iter();
    match itr.next().unwrap() {
        Message::ParseComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::BindComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = Row::new(body).unwrap();
            assert_eq!(
                i32::from_be_bytes(row.get(0).unwrap().try_into().unwrap()),
                1
            );
            assert_eq!(
                String::from_utf8(row.get(1).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "foo"
            );
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = Row::new(body).unwrap();
            assert_eq!(
                i32::from_be_bytes(row.get(0).unwrap().try_into().unwrap()),
                2
            );
            assert_eq!(
                String::from_utf8(row.get(1).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "foobar"
            );
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::ReadyForQuery(_) => {}
        _ => panic!("unexpected message"),
    }
}

#[cfg(feature = "raw")]
#[tokio::test]
async fn query_execute_with_data_result_text_format() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT); INSERT INTO foo (name) VALUES ('foo'), ('foobar');")
        .await
        .unwrap();

    let select =
        prepare::<tokio_postgres::Error>(&client, "SELECT * FROM foo WHERE name LIKE $1", "", &[])
            .unwrap();

    let portal = bind::<&[Option<BytesMut>; 1], tokio_postgres::Error>(
        &client,
        select,
        "",
        &[0],
        &[Some(BytesMut::from("foo%"))],
        &[0],
    )
    .unwrap();

    execute::<tokio_postgres::Error>(&client, &portal, 0).unwrap();
    let messages: Vec<Message> = sync::<tokio_postgres::Error>(&client)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(messages.len(), 6);

    let mut itr = messages.into_iter();
    match itr.next().unwrap() {
        Message::ParseComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::BindComplete => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = Row::new(body).unwrap();
            assert_eq!(
                String::from_utf8(row.get(0).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "1"
            );
            assert_eq!(
                String::from_utf8(row.get(1).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "foo"
            );
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::DataRow(body) => {
            let row = Row::new(body).unwrap();
            assert_eq!(
                String::from_utf8(row.get(0).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "2"
            );
            assert_eq!(
                String::from_utf8(row.get(1).unwrap().to_vec())
                    .unwrap()
                    .as_str(),
                "foobar"
            );
        }
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::CommandComplete(_) => {}
        _ => panic!("unexpected message"),
    }
    match itr.next().unwrap() {
        Message::ReadyForQuery(_) => {}
        _ => panic!("unexpected message"),
    }
}

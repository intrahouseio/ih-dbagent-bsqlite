
const sqlite3 = require('better-sqlite3');
const util = require("util");
const db = new sqlite3("./test.db", { verbose: console.log });

let sql = "";
db.exec("CREATE TABLE IF NOT EXISTS customtable (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, ts INTEGER, payload TEXT)");
const date = Date.now();

sql = `INSERT INTO customtable (name, ts, payload) VALUES ('mytable', ${date}, json('{"col1":"name", "col2":"lastName", "col3":4567}'));`
db.prepare(sql).run();
db.exec("CREATE INDEX IF NOT EXISTS idx_name ON customtable (name);");
//Вывести всю таблицу
//sql = "SELECT json(customtable.payload) FROM customtable"
sql = "SELECT * FROM customtable WHERE name = 'mytable'";
console.log("sql1_request_All: " + util.inspect(db.prepare(sql).all()));
/*[{
        id: 1,
        name: 'mytable',
        ts: 1651049599131,
        payload: '{"col1":"name","col2":"lastName","col3":9876}'
    }]
*/

//Обновить несколько свойств в JSON по конкретному ID. Нужно указать ID записи, название свойства и значение
sql = `UPDATE customtable SET payload = json_set(payload, '$.col3', 4567, '$.col2', 'CCCC') WHERE ID = 1`
console.log("sql3_update: " + util.inspect(db.prepare(sql).run()));

//Найти JSON в котором есть поле с конкретным значением. Нужно указать имя пользовательской таблицы, название свойства и значение
sql = "SELECT customtable.id, name, ts, json(customtable.payload) AS payload "+
      "FROM customtable, json_each(customtable.payload) "+ 
      "WHERE name = 'mytable' AND ((json_each.key = 'col3' AND json_each.value = 4567))" ;
/*
[{
    'json(customtable.payload)': '{"col1":"name","col2":"lastName","col3":4567}'
  }]
*/
console.log("sql4_request_each: " + util.inspect(db.prepare(sql).all()));
db.close();
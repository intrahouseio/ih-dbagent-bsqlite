/**
 * sqlite3 client
 */

const path = require('path');
const  fs  = require('fs');
let sqlite3;

module.exports = {
  pool: null,
  init() {
    try {
      sqlite3 = require('better-sqlite3');
    } catch (err) {
      return err; // Не установлен npm модуль - больше не перезагружать
    }
  },

  createPoolToDatabase(dbopt) {
    const folder = path.dirname(dbopt.dbPath);
    if (!fs.existsSync(folder)) {
      fs.mkdirSync(folder, { recursive: true });
    }
    
    this.pool = this.connect(dbopt.dbPath);
    
  },

  connect(dbPath) {
      return new sqlite3(dbPath, { verbose: console.log });
  },

  run(sql) {
   return this.pool.prepare(sql).run();
  },

  exec(sql) {
    return new Promise((resolve, reject) => {
      this.pool.exec(sql);
      resolve();
    });
  },

  pragma(sql) {
      return this.pool.pragma(sql);
  },

  insert(sql, values) {
      const insert = this.pool.prepare(sql);
      const insertMany = this.pool.transaction((data) => {
        for (const obj of data) insert.run(obj);
      });
      insertMany(values);
  },

  readraw(sql) {
    return this.pool.prepare(sql).raw(true).all();
  },
  read(sql) {
    return this.pool.prepare(sql).all();
  },

  query(sql) {
    return this.pool.prepare(sql).all();
  },

  createTable(query, tableName) {
    return new Promise((resolve, reject) => {
      const stmt = this.pool.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name= ?`);
      const table = stmt.all(tableName);
        if (table.length == 1) {
          resolve();
        } else {
          const createTable = this.pool.prepare(query);
          const info = createTable.run();
          info.changes ? resolve() : reject();  
        }
    });
  }
};

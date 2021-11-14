/**
 * sqlite3 client
 */

// const util = require('util');
const path = require('path');
const fs = require('fs');

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

  async createPoolToDatabase(dbopt) {
    const folder = path.dirname(dbopt.dbPath);
    if (!fs.existsSync(folder)) {
      fs.mkdirSync(folder, { recursive: true });
    }
    
    this.pool = await this.connect(dbopt.dbPath);
    
  },

  connect(dbPath) {
    return new Promise((resolve, reject) => {
      const db = new sqlite3(dbPath, { verbose: console.log })
      resolve(db);
    });
  },

  run(sql) {
    return new Promise((resolve, reject) => {
      const result = this.pool.prepare(sql).run();
      resolve(result);
    });
  },

  exec(sql) {
    return new Promise((resolve, reject) => {
      this.pool.exec(sql);
      resolve();
    });
  },

  pragma(sql) {
    return new Promise((resolve, reject) => {
      this.pool.pragma(sql);
      resolve();
    });
  },

  insert(sql, values) {
    return new Promise((resolve, reject) => {
      const insert = this.pool.prepare(sql);
      const insertMany = this.pool.transaction((data) => {
        for (const obj of data) insert.run(obj);
      });
      insertMany(values);
      resolve();
    });
  },

  read(sql) {
    return new Promise((resolve, reject) => {
      const result = this.pool.prepare(sql).raw(true).all();
      resolve(result);
    });
  },

  query(sql) {
    return new Promise((resolve, reject) => {
      this.pool.all(sql, (err, records) => {
        if (!err) {
          resolve(records);
        } else reject(err);
      });
    });
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

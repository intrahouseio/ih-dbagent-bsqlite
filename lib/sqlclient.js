/**
 * sqlclient.js
 *
 * Объект вызывается из другого дочернего процесса для чтения из БД
 *   (данные для отчетов)
 *  - Выполняет подключение к БД
 *  - Формирует sql запрос (при необходимости)
 *  - Выполняет запрос, возвращает массив данных
 */

 const util = require('util');

 const sqlite3 = require('better-sqlite3');
 
 const utils = require('./utils');
 
 // Входной параметр
 //   dbPath: <полный путь к БД, включая имя файла>
 class Sqlclient {
   constructor(opt) {
     this.opt = opt;
     this.pool = null;
   }
 
   connect() {
     const dbPath = this.opt.dbPath;
     //const dbPath = '/var/lib/ih-v5/projects/test_db1/db/hist.db';
    
     return new Promise((resolve, reject) => {
       this.pool = new sqlite3(dbPath, { verbose: console.log })
       
       resolve();
     });
   }
   
   prepareQuery(queryObj) {
     let queryStr;
     if (typeof queryObj == 'string') {
       queryStr = queryObj;
     } else if (queryObj.sql) {
       queryStr = queryObj.sql;
     } else {
       if (!queryObj.ids) return ''; // Нет запроса - просто пустая строка
 
       const idarr = queryObj.ids.split(',');
       queryStr = utils.getQueryStrId(queryObj, idarr);
     }
 
     console.log('SQLClient queryStr='+queryStr)
     return queryStr;
   }
 
 
   query(queryStr) {
    
     if (!queryStr) return ('Empty queryStr! ');
     if (typeof queryStr != 'string') return ('Expected query as SQL string! ');
     return this.pool.prepare(queryStr).all();

   }
 
   close() {
     if (this.pool) {
       this.pool.close();
       this.pool = null;
     }
   }
 }
 
 module.exports = Sqlclient;
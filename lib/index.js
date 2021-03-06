/**
 * dbagent - client for Sqlite3
 */
const util = require('util');
const schedule = require('node-schedule');
const { promises: fs } = require('fs');
const client = require('./client');
const utils = require('./utils');

module.exports = function(channel, opt, logger) {
  const initErr = client.init();
  if (initErr) processExit(0, initErr); // Модуль sqlite3 не установлен

  const options = getOptions(opt);
  let overflow = 0;
  let lastOverflow = 0;
  let maxTimeRead = 0;
  let maxTimeWrite = 0;

  let hoursRule = new schedule.RecurrenceRule();
  // hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); //Get settings for retention policy
  });

  logger.log('Options: ' + JSON.stringify(options), 2);
  
  setInterval(async () => getDBSize(), 60000); //Get db size

  async function getDBSize() {
    let stats = await fs.stat(opt.dbPath);
    let fileSize = stats['size'] / 1048576;
    stats = await fs.stat(opt.dbPath + '-shm');
    fileSize = fileSize + stats['size'] / 1048576;
    stats = await fs.stat(opt.dbPath + '-wal');
    fileSize = fileSize + stats['size'] / 1048576;
    if (process.connected) process.send({ type: 'procinfo', data: { size: Math.round(fileSize * 100) / 100 } });
    overflow = fileSize > opt.dbLimit ? 1 : 0;
    if (process.connected) process.send({ type: 'procinfo', data: { overflow: overflow } });
    maxTimeRead = 0;
    maxTimeWrite = 0;
  }

  try {
    client.createPoolToDatabase(options, logger);
    async () => getDBSize();

    if (!client.pool) throw { message: 'Client creation Failed!' };

    client.pragma('journal_mode = WAL');
    client.pragma('auto_vacuum = FULL');
    
    client.createTable(getCreateTableStr('records'), 'records');
    client.exec('CREATE INDEX IF NOT EXISTS idx_records_idts ON records (id, ts);');

    client.createTable(getCreateTableStr('timeline'), 'timeline');
    client.exec('CREATE INDEX IF NOT EXISTS idx_records_start ON timeline (start);');

    client.createTable(getCreateTableStr('customtable'), 'customtable');
    client.exec('CREATE INDEX IF NOT EXISTS idx_name ON customtable (name);');
    //logger.log("customtable" + util.inspect(client.pragma('table_info(customtable)')));


    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ id, dn, prop, ts, val }] or [{ name, ts, payload }]
   */
  function write(id, payload, table) {
    const beginTime = Date.now();
    let tableName = table || 'records';
    const columns = getColumns(tableName);
    let values1 = '';
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;
    
    //let query = 'INSERT INTO ' + tableName + ' (' + columns.join(', ') + ') VALUES '; 
    //query += '(' + columns.map(item => '@' + item).join(', ') + ')';
    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ';
    if (table == 'customtable') {
      values1 = `(${values[0][0]}, ${values[0][1]}, json(${values[0][2]}))`;
    } else {
      values1 = values.map(i => `(${i})`).join(', ');
    } 
    let sql = query + ' ' + values1;

    logger.log('sql=' + sql, 2);
    try {
      client.run(sql);
      const endTime = Date.now();
      if (maxTimeWrite < endTime - beginTime) {
        maxTimeWrite = endTime - beginTime;
        if (process.connected)
          process.send({
            type: 'procinfo',
            data: { lastMaxTimeWrite: maxTimeWrite, lastMaxCountWrite: payload.length }
          });
      }
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  function del(options) {
    let archDays = [1, 7, 15, 30, 90, 180, 360, 500];

    let tableName = 'records';
    for (const archDay of archDays) {
      let arrId = options.rp.filter(object => object.days == archDay);
      deletePoints(tableName, archDay, arrId);
    }
  } 

  function deletePoints(tableName, archDay, arrId) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrId), 1);
    let archDepth = archDay * 86400000;
    let delTime = Date.now() - archDepth;
    if (!arrId.length) return;
    while (arrId.length>0) {
      let chunk = arrId.splice(0,500);
      let values = chunk.map(i => `(id='${i.id}')`).join(' OR ');
      logger.log('Map=' + values, 1);
      let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
      try {
        const changes = client.run(sql);
        logger.log(`Row(s) deleted ${changes}`, 1);
      } catch (err) {
        sendError('delete', err);
      }
    }
  }

  function read(id, queryObj) {
    const beginTime  = Date.now();

    let idarr, dnarr, result;
    logger.log('Read query id=' + id + util.inspect(queryObj), 1);
    try {
      firstTime = Date.now();
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
        result = client.read(queryStr);
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        if (queryObj.table == 'timeline') {
          dnarr = queryObj.dn_prop.split(',');
          queryStr = utils.getQueryStrDn(queryObj, dnarr);
          result = client.read(queryStr);
        } else {
          idarr = queryObj.ids.split(',');
          queryStr = utils.getQueryStrId(queryObj, idarr);
          result = client.readraw(queryStr);
        }
      }
      logger.log('SQL: ' + queryStr, 1);
      const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected) process.send({type:'procinfo', data:{lastMaxTimeRead: maxTimeRead, lastMaxCountRead:result.length}});
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.sql || queryObj.table == 'timeline') {
        payload = result;
      } else {
        payload = queryObj.target == 'trend' ? formForTrend(result) : utils.recordsForRaw(result, queryObj);
      }
      
      logger.log("Payload" +  util.inspect(payload));
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return idarr.length == 1 ? res.map(item => [item[1], Number(item[3])]) : utils.recordsForTrendRaw(res, idarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    //
    const res = {};

    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.close();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
        'dn TEXT NOT NULL,prop TEXT,' +
        'start INTEGER  NOT NULL,' +
        'end INTEGER NOT NULL,' +
        'state char(8)';
      break;
    case 'customtable':
      result = 
      'id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
      'name TEXT NOT NULL, ' +
      'ts INTEGER NOT NULL, ' +
      'payload TEXT'
      break;
    default:
      result = 'uid INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, id INTEGER NOT NULL, val REAL';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', 'end', 'state'];
    case 'customtable':
      return ['name', 'ts', 'payload'];
    default:
      return ['id', 'ts', 'val'];
  }
}
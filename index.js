/**
 * Created by Kyriakos Barbounakis<k.barbounakis@gmail.com> on 26/11/2014.
 *
 * Copyright (c) 2014, Kyriakos Barbounakis k.barbounakis@gmail.com
 Anthi Oikonomou anthioikonomou@gmail.com
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
 list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

 * Neither the name of most-query nor the names of its
 contributors may be used to endorse or promote products derived from
 this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
var util = require('util'), pg = require('pg'), qry = require('most-query'), async = require('async');
/**
 * @class PGSqlAdapter
 * @param {*} options
 * @constructor
 * @augments {DataAdapter}
 */
function PGSqlAdapter(options) {
    this.rawConnection = null;
    /**
     * @type {*}
     */
    this.transaction = null;
    /**
     * @type {{host: string}|{port: number}|{username: string}|{password: string}|{database: string}}
     */
    this.options = options || { };
    if (typeof this.options.port === 'undefined')
        this.options.port = 5432;
    if (typeof this.options.host === 'undefined')
        this.options.port = 'localhost';
    //define connection string
    var self = this;
    Object.defineProperty(this, 'connectionString', { get: function() {
        return util.format('postgres://%s:%s@%s:%s/%s',
            self.options.user,
            self.options.password,
            self.options.host,
            self.options.port,
            self.options.database);
    }, enumerable:false, configurable:false});
}
/**
 * Opens a new database connection
 * @param {function(Error=)} callback
 */
PGSqlAdapter.prototype.open = function(callback) {
    var self = this;
    callback = callback || function() {};
    if (self.rawConnection) {
        callback();
        return;
    }
    self.rawConnection = new pg.Client(this.connectionString);

    /*self.rawConnection.on('error', function(err) {
        console.log('Connection Error');
        console.log(err.message);
        if (err.stack) {
            console.log(err.stack);
        }
    });*/

    //try to connection
    this.rawConnection.connect(function(err) {
        if(err) {
            //set connection to null
            this.rawConnection = null;
        }
        //and return
        callback(err);
    });
};

/**
 * Closes the underlying database connection
 * @param {function(Error=)} callback
 */
PGSqlAdapter.prototype.close = function(callback) {
    callback = callback || function() {};
    if (typeof this.rawConnection === 'undefined' || this.rawConnection===null) {
        callback();
        return;
    }
    try {
        //try to close connection
        this.rawConnection.end();
        this.rawConnection = null;
        callback();
    }
    catch(e) {
        console.log('An error occurred while trying to close database connection. ' + e.message);
        this.rawConnection = null;
        //do nothing (do not raise an error)
        callback();
    }
};
/**
 * @param {string} query
 * @param {*=} values
 */
PGSqlAdapter.prototype.prepare = function(query,values) {
    return qry.prepare(query,values)
}

/**
 * Executes a query against the underlying database
 * @param query {QueryExpression|string|*}
 * @param values {*=}
 * @param {function(Error=,*=)} callback
 */
PGSqlAdapter.prototype.execute = function(query, values, callback) {
    var self = this, sql = null;
    try {

        if (typeof query == 'string') {
            //get raw sql statement
            //todo: this operation may be obsolete (for security reasons)
            sql = query;
        }
        else {
            //format query expression or any object that may be act as query expression
            var formatter = new PGSqlFormatter();
            sql = formatter.format(query);
        }
        //validate sql statement
        if (typeof sql !== 'string') {
            callback.call(self, new Error('The executing command is of the wrong type or empty.'));
            return;
        }
        //ensure connection
        self.open(function(err) {
            if (err) {
                callback.call(self, err);
            }
            else {
                //todo: validate statement for sql injection (e.g single statement etc)
                //log statement (optional)
                if (process.env.NODE_ENV==='development')
                    console.log(util.format('SQL:%s, Parameters:%s', sql, JSON.stringify(values)));
                var prepared = self.prepare(sql, values);
                //execute raw command
                self.rawConnection.query(prepared, null, function(err, result) {
                    if (err) {
                        callback(err);
                    }
                    else {
                        callback(null, result.rows);
                    }
                });
            }
        });
    }
    catch (e) {
        callback.call(self, e);
    }

};

PGSqlAdapter.prototype.lastIndentity = function(callback) {
    var self = this;
    self.open(function(err) {
        if (err) {
            callback(err);
        }
        else {
            //execute lastval (for sequence)
            self.rawConnection.query('SELECT lastval()', null, function(err, lastval) {
                if (err) {
                    callback(null, { insertId: null });
                }
                else {
                    lastval.rows = lastval.rows || [];
                    if (lastval.rows.length>0)
                        callback(null, { insertId:lastval.rows[0]['lastval'] });
                    else
                        callback(null, { insertId: null });
                }
            });
        }
    });
};

/**
 * Begins a database transaction and executes the given function
 * @param {function(Error=)} fn
 * @param {function(Error=)} callback
 */
PGSqlAdapter.prototype.executeInTransaction = function(fn, callback) {
    var self = this;
    //ensure parameters
    fn = fn || function() {}; callback = callback || function() {};
    self.open(function(err) {
        if (err) {
            callback(err);
        }
        else {
            if (self.transaction) {
                fn.call(self, function(err) {
                    callback(err);
                });
            }
            else {
                //begin transaction
                self.rawConnection.query('BEGIN TRANSACTION;', null, function(err) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    //initialize dummy transaction object (for future use)
                    self.transaction = { };
                    //execute function
                    fn.call(self, function(err) {
                        if (err) {
                            //rollback transaction
                            self.rawConnection.query('ROLLBACK TRANSACTION;', null, function() {
                                self.transaction = null;
                                callback(err);
                            });
                        }
                        else {
                            //commit transaction
                            self.rawConnection.query('COMMIT TRANSACTION;', null, function(err) {
                                self.transaction = null;
                                callback(err);
                            });
                        }
                    });
                });
            }
        }
    });

};

/**
 * Produces a new identity value for the given entity and attribute.
 * @param entity {String} The target entity name
 * @param attribute {String} The target attribute
 * @param callback {Function=}
 */
PGSqlAdapter.prototype.selectIdentity = function(entity, attribute , callback) {

    var self = this;

    var migration = {
        appliesTo:'increment_id',
        model:'increments',
        description:'Increments migration (version 1.0)',
        version:'1.0',
        add:[
            { name:'id', type:'Counter', primary:true },
            { name:'entity', type:'Text', size:120 },
            { name:'attribute', type:'Text', size:120 },
            { name:'value', type:'Integer' }
        ]
    }
    //ensure increments entity
    self.migrate(migration, function(err)
    {
        //throw error if any
        if (err) { callback.call(self,err); return; }
        self.execute('SELECT * FROM increment_id WHERE entity=? AND attribute=?', [entity, attribute], function(err, result) {
            if (err) { callback.call(self,err); return; }
            if (result.length==0) {
                //get max value by querying the given entity
                var q = qry.query(entity).select([qry.fields.max(attribute)]);
                self.execute(q,null, function(err, result) {
                    if (err) { callback.call(self, err); return; }
                    var value = 1;
                    if (result.length>0) {
                        value = parseInt(result[0][attribute]) + 1;
                    }
                    self.execute('INSERT INTO increment_id(entity, attribute, value) VALUES (?,?,?)',[entity, attribute, value], function(err) {
                        //throw error if any
                        if (err) { callback.call(self, err); return; }
                        //return new increment value
                        callback.call(self, err, value);
                    });
                });
            }
            else {
                //get new increment value
                var value = parseInt(result[0].value) + 1;
                self.execute('UPDATE increment_id SET value=? WHERE id=?',[value, result[0].id], function(err) {
                    //throw error if any
                    if (err) { callback.call(self, err); return; }
                    //return new increment value
                    callback.call(self, err, value);
                });
            }
        });
    });
};

/**
 * Executes an operation against database and returns the results.
 * @param batch {DataModelBatch}
 * @param callback {Function}
 */
PGSqlAdapter.prototype.executeBatch = function(batch, callback) {
    callback = callback || function() {};
    callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
};

/**
 *
 * @param {*|{type:string, size:number, nullable:boolean}} field
 * @param {string=} format
 * @returns {string}
 */
PGSqlAdapter.formatType = function(field, format)
{
    var size = parseInt(field.size);
    var s = 'varchar(512) NULL';
    var type=field.type;
    switch (type)
    {
        case 'Boolean':
            s = 'boolean';
            break;
        case 'Byte':
            s = 'smallint';
            break;
        case 'Number':
        case 'Float':
            s = 'real';
            break;
        case 'Counter':
            return 'SERIAL';
        case 'Currency':
        case 'Decimal':
            s =  util.format('decimal(%s,0)', size>0? size: 10);
            break;
        case 'Date':
            s = 'date';
            break;
        case 'DateTime':
            s = 'timestamp';
            break;
        case 'Time':
            s = 'time';
            break;
        case 'Integer':
            s = 'bigint';
            break;
        case 'Duration':
            s = 'integer';
            break;
        case 'URL':
            if (size>0)
                s =  util.format('varchar(%s)', size);
            else
                s =  'varchar';
            break;
        case 'Text':
            if (size>0)
                s =  util.format('varchar(%s)', size);
            else
                s =  'varchar';
            break;
        case 'Note':
            if (size>0)
                s =  util.format('varchar(%s)', size);
            else
                s =  'text';
            break;
        case 'Image':
        case 'Binary':
            s = size > 0 ? util.format('bytea(%s)', size) : 'bytea';
            break;
        case 'Guid':
            s = 'uuid';
            break;
        case 'Short':
            s = 'smallint';
            break;
        default:
            s = 'integer';
            break;
    }
    if (format==='alter')
        s += (typeof field.nullable === 'undefined') ? ' DROP NOT NULL': (field.nullable ? ' DROP NOT NULL': ' SET NOT NULL');
    else
        s += (typeof field.nullable === 'undefined') ? ' NULL': (field.nullable ? ' NULL': ' NOT NULL');
    return s;
}

/**
 * @param query {QueryExpression}
 */
PGSqlAdapter.prototype.createView = function(name, query, callback) {
    var self = this;
    //open database
    self.open(function(err) {
        if (err) {
            callback.call(self, err);
            return;
        }
        //begin transaction
        self.executeInTransaction(function(tr)
        {
            async.waterfall([
                function(cb) {
                    self.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'VIEW\' AND table_name=?', [ name ],function(err, result) {
                        if (err) { throw err; }
                        if (result.length==0)
                            return cb(null, 0);
                        cb(null, result[0].count);
                    });
                },
                function(arg, cb) {
                    if (arg==0) { cb(null, 0); return; }
                    //format query
                    var sql = util.format("DROP VIEW \"%s\"",name);
                    self.execute(sql, null, function(err, result) {
                        if (err) { throw err; }
                        cb(null, 0);
                    });
                },
                function(arg, cb) {
                    //format query
                    var formatter = new PGSqlFormatter();
                    formatter.settings.nameFormat = PGSqlAdapter.NAME_FORMAT;
                    var sql = util.format("CREATE VIEW \"%s\" AS %s", name, formatter.format(query));
                    self.execute(sql, null, function(err, result) {
                        if (err) { throw err; }
                        cb(null, 0);
                    });
                }
            ], function(err) {
                if (err) { tr(err); return; }
                tr(null);
            })
        }, function(err) {
            callback(err);
        });
    });

};

/**
 * @class DataModelMigration
 * @property {string} name
 * @property {string} description
 * @property {string} model
 * @property {string} appliesTo
 * @property {string} version
 * @property {array} add
 * @property {array} remove
 * @property {array} change
 */
/**
 * @param {string} name
 * @returns {{exists: Function}}
 */
PGSqlAdapter.prototype.table = function(name) {
    var self = this;
    return {
        /**
         * @param {function(Error,Boolean=)} callback
         */
        exists: function(callback) {
            callback = callback || function() {};
            self.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\'public\' AND table_type=\'BASE TABLE\' AND table_name=?',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
        },
        /**
         * @param {function(Error,string=)} callback
         */
        version:function(callback) {
            self.execute('SELECT MAX("version") AS version FROM migrations WHERE "appliesTo"=?',
                [name], function(err, result) {
                    if (err) { cb(err); return; }
                    if (result.length==0)
                        callback(null, '0.0');
                    else
                        callback(null, result[0].version || '0.0');
                });
        },
        /**
         * @param {function(Error,Boolean=)} callback
         */
        has_sequence:function(callback) {
            callback = callback || function() {};
            self.execute('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND table_schema=\'public\' AND ("column_default" ~ \'^nextval\((.*?)\)$\')',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
        },
        /**
         * @param {function(Error,{columnName:string,ordinal:number,dataType:*, maxLength:number,isNullable:number }[]=)} callback
         */
        columns:function(callback) {
            callback = callback || function() {};
            self.execute('SELECT column_name AS "columnName", ordinal_position as "ordinal", data_type as "dataType",' +
                'character_maximum_length as "maxLength", is_nullable AS  "isNullable", column_default AS "defaultValue"' +
                ' FROM information_schema.columns WHERE table_name=?',
                [name], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, result);
                });
        }
    }
}

 /*
 * @param obj {DataModelMigration|*} An Object that represents the data model scheme we want to migrate
 * @param callback {Function}
 */
PGSqlAdapter.prototype.migrate = function(obj, callback) {
    if (obj==null)
        return;
    var self = this;
    /**
     * @type {DataModelMigration|*}
     */
    var migration = obj;

    var format = function(format, obj)
    {
        var result = format;
        if (/%t/.test(format))
            result = result.replace(/%t/g,PGSqlAdapter.formatType(obj));
        if (/%f/.test(format))
            result = result.replace(/%f/g,obj.name);
        return result;
    }

    if (migration.appliesTo==null)
        throw new Error("Model name is undefined");
    self.open(function(err) {
        if (err) {
            callback.call(self, err);
        }
        else {
            var db = self.rawConnection;
            async.waterfall([
                //1. Check migrations table existence
                function(cb) {
                    if (PGSqlAdapter.supportMigrations) {
                        cb(null, 1);
                        return;
                    }
                    self.table('migrations').exists(function(err, exists) {
                        if (err) { cb(err); return; }
                        cb(null, exists);
                    });
                },
                //2. Create migrations table if not exists
                function(arg, cb) {
                    if (arg>0) { cb(null, 0); return; }
                    //create migrations table
                    self.execute('CREATE TABLE migrations(id SERIAL NOT NULL, ' +
                            '"appliesTo" varchar(80) NOT NULL, "model" varchar(120) NULL, "description" varchar(512),"version" varchar(40) NOT NULL)',
                        ['migrations'], function(err) {
                            if (err) { cb(err); return; }
                            PGSqlAdapter.supportMigrations=true;
                            cb(null, 0);
                        });
                },
                //3. Check if migration has already been applied
                function(arg, cb) {
                    self.table(migration.appliesTo).version(function(err, version) {
                        if (err) { cb(err); return; }
                        cb(null, (version>=migration.version));
                    });

                },
                //4a. Check table existence
                function(arg, cb) {
                    //migration has already been applied (set migration.updated=true)
                    if (arg) {
                        obj['updated']=true;
                        cb(null, -1);
                        return;
                    }
                    else {
                        self.table(migration.appliesTo).exists(function(err, exists) {
                            if (err) { cb(err); return; }
                            cb(null, exists ? 1 : 0);
                        });

                    }
                },
                //4b. Get table columns
                function(arg, cb) {
                    //migration has already been applied
                    if (arg<0) { cb(null, [arg, null]); return; }
                    self.table(migration.appliesTo).columns(function(err, columns) {
                        if (err) { cb(err); return; }
                        cb(null, [arg, columns]);
                    });
                },
                //5. Migrate target table (create or alter)
                function(args, cb)
                {
                    //migration has already been applied
                    if (args[0]<0) { cb(null, args[0]); return; }
                    var columns = args[1];
                    if (args[0]==0) {
                        //create table and
                        var strFields = migration.add.filter(function(x) {
                            return !x.oneToMany
                        }).map(
                            function(x) {
                                return format('\"%f\" %t', x);
                            }).join(', ');
                        var key = migration.add.filter(function(x) { return x.primary; })[0];
                        var sql = util.format('CREATE TABLE \"%s\" (%s, PRIMARY KEY(\"%s\"))', migration.appliesTo, strFields, key.name);
                        self.execute(sql, null, function(err)
                        {
                            if (err) { cb(err); return; }
                            cb(null, 1);
                            return;
                        });

                    }
                    else {
                        var expressions = [], column, fname;
                        //1. enumerate fields to delete
                        if (migration.remove) {
                            for(i=0;i<migration.remove.length;i++) {
                                fname=migration.remove[i].name;
                                column = columns.filter(function(x) { return x.columnName === fname; })[0];
                                if (typeof column !== 'undefined') {
                                    var k= 1, deletedColumnName =util.format('xx%s1_%s', k.toString(), column.columnName);
                                    while(typeof columns.filter(function(x) { return x.columnName === deletedColumnName; })[0] !=='undefined') {
                                        k+=1;
                                        deletedColumnName =util.format('xx%s_%s', k.toString(), column.columnName);
                                    }
                                    expressions.push(util.format('ALTER TABLE \"%s\" RENAME COLUMN \"%s\" TO %s', migration.appliesTo, column.columnName, deletedColumnName));
                                }
                            }
                        }
                        //2. enumerate fields to add
                        var newSize, originalSize, fieldName, nullable;
                        if (migration.add)
                        {
                            for(i=0;i<migration.add.length;i++)
                            {
                                //get field name
                                fieldName = migration.add[i].name;
                                //check if field exists or not
                                column = columns.filter(function(x) { return x.columnName === fieldName; })[0];
                                if (typeof column !== 'undefined') {
                                    //get original field size
                                    originalSize = column.maxLength;
                                    //and new foeld size
                                    newSize = migration.add[i].size;
                                    //add expression for modifying column (size)
                                    if ((typeof newSize !== 'undefined') && (originalSize!=newSize)) {
                                        expressions.push(util.format('UPDATE pg_attribute SET atttypmod = %s+4 WHERE attrelid = \'"%s"\'::regclass AND attname = \'%s\';',newSize, migration.appliesTo,  fieldName));
                                    }
                                    //update nullable attribute
                                    nullable = (typeof migration.add[i].nullable !=='undefined') ? migration.add[i].nullable  : true;
                                    expressions.push(util.format('ALTER TABLE \"%s\" ALTER COLUMN \"%s\" %s', migration.appliesTo, fieldName, (nullable ? 'DROP NOT NULL' : 'SET NOT NULL')));
                                }
                                else {
                                    //add expression for adding column
                                    expressions.push(util.format('ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s', migration.appliesTo, fieldName, PGSqlAdapter.formatType(migration.add[i])));
                                }
                            }
                        }

                        //3. enumerate fields to update
                        if (migration.change) {
                            for(var i=0;i<migration.change.length;i++) {
                                var change = migration.change[i];
                                column = columns.filter(function(x) { return x.columnName == change.name })[0];
                                if (typeof column !== 'undefined') {
                                    //important note: Alter column operation is not supported for column types
                                    //todo:validate basic column altering exceptions (based on database engine rules)
                                    expressions.push(util.format('ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s', migration.appliesTo, migration.add[i].name, PGSqlAdapter.formatType(migration.change[i])));
                                }
                            }
                        }

                        if (expressions.length>0) {
                            self.execute(expressions.join(';'), null, function(err)
                            {
                                if (err) { cb(err); return; }
                                cb(null, 1);
                                return;
                            });
                        }
                        else
                            cb(null, 2);
                    }
                }, function(arg, cb) {

                    if (arg>0) {
                        //log migration to database
                        self.execute('INSERT INTO migrations("appliesTo", "model", "version", "description") VALUES (?,?,?,?)', [migration.appliesTo,
                            migration.model,
                            migration.version,
                            migration.description ], function(err, result)
                        {
                            if (err) throw err;
                            cb(null, 1);
                            return;
                        });
                    }
                    else {
                        migration['updated'] = true;
                        cb(null, arg);
                    }
                }
            ], function(err, result) {
                callback(err, result);
            });
        }
    });
};

PGSqlAdapter.NAME_FORMAT = '"$1"';

/**
 * @class PGSqlFormatter
 * @constructor
 * @augments {SqlFormatter}
 */
function PGSqlFormatter() {
    this.settings = {
        nameFormat:PGSqlAdapter.NAME_FORMAT
    }
}
util.inherits(PGSqlFormatter, qry.classes.SqlFormatter);

/**
 * Implements indexOf(str,substr) expression formatter.
 * @param {String} p0 The source string
 * @param {String} p1 The string to search for
 */
PGSqlFormatter.prototype.$indexof = function(p0, p1)
{

    var result = util.format('POSITION(%s IN %s)', this.escape(p1), this.escape(p0));
    return result;
};

/**
 * Implements startsWith(a,b) expression formatter.
 * @param p0 {*}
 * @param p1 {*}
 */
PGSqlFormatter.prototype.$startswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return util.format('(%s ~ \'^%s\')', this.escape(p0), this.escape(p1, true));
};

/**
 * Implements endsWith(a,b) expression formatter.
 * @param p0 {*}
 * @param p1 {*}
 */
PGSqlFormatter.prototype.$endswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    var result = util.format('(%s ~ \'%s$$\')', this.escape(p0), this.escape(p1, true));
    return result;
};

/**
 * Implements substring(str,pos) expression formatter.
 * @param {String} p0 The source string
 * @param {Number} pos The starting position
 * @param {Number=} length The length of the resulted string
 * @returns {string}
 */
PGSqlFormatter.prototype.$substring = function(p0, pos, length)
{
    if (length)
        return util.format('SUBSTRING(%s FROM %s FOR %s)', this.escape(p0), pos.valueOf()+1, length.valueOf());
    else
        return util.format('SUBSTRING(%s FROM %s)', this.escape(p0), pos.valueOf()+1);
};

/**
 * Implements contains(a,b) expression formatter.
 * @param p0 {*}
 * @param p1 {*}
 */
PGSqlFormatter.prototype.$contains = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    if (p1.valueOf().toString().length==0)
        return '';
    return util.format('(%s ~ \'%s\')', this.escape(p0), this.escape(p1, true));
};

PGSqlFormatter.prototype.escapeName = function(name) {
    if (typeof name === 'string')
        return name.replace(/(\w+)/ig, this.settings.nameFormat);
    return name;
};

/**
 * Implements length(a) expression formatter.
 * @param p0 {*}
 */
PGSqlFormatter.prototype.$length = function(p0)
{
    return util.format('LENGTH(%s)', this.escape(p0));
};

PGSqlFormatter.prototype.$day = function(p0) { return util.format('DATE_PART(\'day\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$month = function(p0) { return util.format('DATE_PART(\'month\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$year = function(p0) { return util.format('DATE_PART(\'year\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$hour = function(p0) { return util.format('DATE_PART(\'hour\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$minute = function(p0) { return util.format('DATE_PART(\'minute\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$second = function(p0) { return util.format('DATE_PART(\'second\',%s)', this.escape(p0)); };
PGSqlFormatter.prototype.$date = function(p0) {
    return util.format('CAST(%s AS DATE)', this.escape(p0));
};

var pgsql = {
    /**
     * @class PGSqlAdapter
     * */
    PGSqlAdapter : PGSqlAdapter,
    /**
     * Creates an instance of PGSqlAdapter object that represents a Postgres database connection.
     * @param options An object that represents the properties of the underlying database connection.
     * @returns {DataAdapter}
     */
    createInstance: function(options) {
        return new PGSqlAdapter(options);
    }
}

if (typeof exports !== 'undefined')
{
    module.exports = pgsql;
}

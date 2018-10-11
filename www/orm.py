import logging
import asyncio
import aiomysql

def log(sql, args):
    logging.info(sql.replace('?', '%s') % args)

async def create_pool(loop, **kw):
    ' create mysql connection pool '
    global __pool
    logging.info('create mysql connection pool ....')
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit',True),
        minsize = kw.get('minsize', 1),
        maxsize = kw.get('maxsize', 10),
        loop = loop
    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(sql.replace('?', '%s'), args)
            if size:
                rs = cursor.fetchmany(size)
            else :
                rs = cursor.fetchall()
        logging.info("fetched rows:%s" % len(rs))
    return rs

async def execute(sql, args, autocommit=True):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql.replace('?', '%s'), args)
                affected = cursor.rowcount
            if not autocommit:
                cursor.commit()
        except BaseException:
            if not autocommit:
                cursor.rollback()
            raise
        return affected
                

def create_args_string(num):
    L = []
    for i in range(num):
        L.append('?')
    return ','.join(L)

class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaClass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        mappings = dict()
        fields = []
        primarykey = None
        for k,v in attrs.items():
            if isinstance(v, Field):
                mappings[k] = v
                if v.primary_key:
                    if primarykey:
                        raise BaseException('Duplicate primary key for field:%s' % k)
                    else :
                        primaryKey = k
                else :
                    fields.append(k)
        
        if not primaryKey:
            raise BaseException('primary key not found')
        
        for k in mappings.keys():
            attrs.pop(k)
        
        escaped_fields = list(map(lambda f:'`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__fields__'] = fields
        attrs['__primary_key__'] = primaryKey
        attrs['__select__'] = 'select `%s`,%s from %s' % (primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s,`%s`) values (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f:'`%s`=?' % f, fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaClass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    
    def __setattr__(self, key, value):
        self[key] = value
    
    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self['__mappings'][key]
            if field.default:
                value = field.default() if callable(field.default) else field.default
                setattr(self, key, value)
        return value

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)

        orderBy = kw.get('orderBy', None)
        if orderBy is not None:
            sql.append('order by')
            sql.append(orderBy)
        
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('%s,%s' % limit)
            else:
                raise ValueError('Invalid limit value:%s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField,where=None, args=None):
        ' calculat number by select and where '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return 0
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.info('failed to insert into table:%s, args:%s' % (self.__table__, str(args)))
    
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.info('failed to update rows in table:%s, args:%s' % (self.__table__, str(args)))
    
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.info('failed to delete rows from table:%s, args:%s' % (self.__table__, str(args)))
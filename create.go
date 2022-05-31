package redisql

import (
	"errors"
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

//how to use?
//eg: 	create table user
//		TABLE("user").FIELDS("name, age, city").TYPES("xzj",26,"sh").CREATE()

//eg:	create index on user(name)
//		TABLE("user").FIELDS("name").INDEX()

func CreateDatabase(dbname string) error {
	db := strings.ToLower(strings.Trim(dbname, " "))
	if len(db) <= 0 {
		return errors.New("database name can not be null.")
	}

	if existsDatabase(db) == true {
		return errors.New(fmt.Sprintf("database:%s exists.\n", db))
	}

	conn := getConn()
	defer conn.Close()

	_, err := conn.Do("HSET", REDISQL_DATABASES, db, 0)
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", fmt.Sprintf(REDISQL_CONDITION_SN, db), 0)
	if err != nil {
		return err
	}
	return nil
}

type Table struct {
	Name   string
	Fields []string
	Types  []string
}

func TABLE(tablename string) *Table {
	if len(database) <= 0 {
		panic("you have not choose database, please call func 'ChangeDatabase'.")
	}
	if len(strings.Trim(tablename, "")) <= 0 {
		panic("tablename can not be null.")
	}
	return &Table{
		Name: strings.ToLower(strings.Trim(tablename, "")),
	}
}

// FIELDS 解析传入的 fields ，并填入 tab.Fields 中。
func (tab *Table) FIELDS(fields ...string) *Table {


	var tmpFields []string
	for _, f := range fields {
		tmpf := strings.Split(f, ",")
		for _, ff := range tmpf {
			tmpFields = append(tmpFields, strings.ToLower(strings.Trim(ff, " ")))
		}
	}

	if len(tmpFields) <= 0 {
		panic("can not call this func without fields.")
	}

	return &Table{
		Name:   tab.Name,
		Fields: tmpFields,
	}
}

// TYPES 解析传入的 types ，并填入 tab.Types 中；只支持 "NUMBER"、"STRING"、"DATA" 三种字段类型。
func (tab *Table) TYPES(types ...string) *Table {
	if len(tab.Fields) <= 0 {
		panic("fields is null, please call func 'FIELDS'.")
	}
	var tmpTypes []string
	for _, t := range types {
		tmpt := strings.Split(t, ",")
		for _, tt := range tmpt {
			if strings.ToLower(strings.Trim(tt, " ")) != REDISQL_TYPE_NUMBER &&
				strings.ToLower(strings.Trim(tt, " ")) != REDISQL_TYPE_STRING &&
				strings.ToLower(strings.Trim(tt, " ")) != REDISQL_TYPE_DATE {
				panic("Redisql not such type like " + strings.ToLower(strings.Trim(tt, " ")))
			}
			tmpTypes = append(tmpTypes, strings.ToLower(strings.Trim(tt, " ")))
		}
	}
	if len(tab.Fields) != len(tmpTypes) {
		panic("Field and types are not correspondence, please check.")
	}
	return &Table{
		Name:   tab.Name,
		Fields: tab.Fields,
		Types:  tmpTypes,
	}
}

func (tab *Table) CREATE() error {
	conn := getConn()
	defer conn.Close()

	//judge table is exists?
	//
	// 检查 {database}.tables 下是否存在 {table}
	exists := existsTable(tab.Name)
	if exists == true {
		return errors.New(fmt.Sprintf("table %s is exist.", tab.Name))
	}

	//add table info
	//
	// 建表参数列表: {database}.{table}.fields, "id", "NUMBER", {field}, {type}, ... ,  {field}, {type}
	var params []interface{}
	params = append(params, fmt.Sprintf(REDISQL_FIELDS, database, tab.Name))
	//add field 'id' default.
	params = append(params, "id")
	params = append(params, REDISQL_TYPE_NUMBER)
	for i := 0; i < len(tab.Fields); i++ {
		if tab.Fields[i] == "id" {
			continue
		}
		params = append(params, tab.Fields[i])
		params = append(params, tab.Types[i])
	}

	// 启动 Multi 事务
	_, err := conn.Do("MULTI")
	if err != nil {
		return err
	}

	// 创建 table
	// 	HMSET {database}.{table}.fields FIELD1 VALUE1 ...FIELDN VALUEN
	_, err = conn.Do("HMSET", params...)
	if err != nil {
		return err
	}

	// 将 table 注册到 database
	//	HSET {database}.tables {table} 0 ，这里 0 是占位值，无意义
	_, err = conn.Do("HSET", fmt.Sprintf(REDISQL_TABLES, database), tab.Name, 0)
	if err != nil {
		conn.Do("DISCARD")
		return err
	}

	// 将 count(*) 注册到 database
	// HSET {database}.tables.count
	_, err = conn.Do("HSET", fmt.Sprintf(REDISQL_COUNT, database), tab.Name, 0)
	if err != nil {
		conn.Do("DISCARD")
		return err
	}

	// 增加 database 内表总数
	// 	HINCRBY KEY_NAME FIELD_NAME INCR_BY_NUMBER
	//
	//tablenumber +1
	_, err = conn.Do("HINCRBY", REDISQL_DATABASES, database, 1)
	if err != nil {
		conn.Do("DISCARD")
		return err
	}

	// 执行
	_, err = conn.Do("EXEC")
	if err != nil {
		conn.Do("DISCARD")
		return err
	}

	//default create index on id
	//
	//
	err = TABLE(tab.Name).FIELDS("id").INDEX()
	if err != nil {
		return err
	}

	return nil
}

func (tab *Table) INDEX() error {
	conn := getConn()
	defer conn.Close()

	//judge table is exists?
	exists := existsTable(tab.Name)
	if exists == false {
		return errors.New(fmt.Sprintf("table %s is not exist.", tab.Name))
	}

	// 联合索引名: index_key := "index_field1_field2_field3"
	indexname := "index"
	for _, f := range tab.Fields {
		// 字段不存在，报错
		if existsField(tab.Name, f) == false {
			return errors.New(fmt.Sprintf("no field %s in table %s.", f, tab.Name))
		}
		indexname += "_"
		indexname += f
	}

	//judge index is exists?
	//
	// 检查索引是否已存在
	if existsIndex(tab.Name, indexname) == true {
		return errors.New(fmt.Sprintf("index %s is exist.", tab.Name))
	}

	//save index
	//
	// 将 index => Fields 保存到 {database}.{table}.indexs 。
	_, err := conn.Do("HSET", fmt.Sprintf(REDISQL_INDEXS, database, tab.Name), indexname, tab.Fields)
	if err != nil {
		return err
	}

	// 字段类型
	var fieldtype string
	if len(tab.Fields) == 1 {
		// 根据 field 读取 {database}.{table}.fields ，获取字段类型
		fieldtype, err = getFieldType(tab.Name, tab.Fields[0])
		if err != nil {
			return err
		}
	}

	//add index
	//get all data
	rows, err := redigo.Strings(conn.Do("KEYS", fmt.Sprintf(REDISQL_DATAS, database, tab.Name, "*")))
	if err != nil {
		return err
	}

	for _, r := range rows {
		// 读取 r 的所有待索引的字段，拼接成 field.value.field.value.field.value 的字符串。
		var indexField string
		for i := 0; i < len(tab.Fields); i++ {
			value, err := redigo.String(conn.Do("HGET", r, tab.Fields[i]))
			if err != nil {
				return err
			}
			if i >= 1 {
				indexField += "."
			}
			indexField += fmt.Sprintf("%s.%s", tab.Fields[i], value)
		}

		ids := strings.Split(r, ".")

		// 字符串
		if fieldtype == "" || fieldtype == REDISQL_TYPE_STRING {
			// 把 ids 添加到 {database}.{table}.{index_key} 的 set 中。
			_, err := conn.Do("SADD", fmt.Sprintf(REDISQL_INDEX_DATAS, database, tab.Name, indexField), ids[len(ids)-1])
			if err != nil {
				return err
			}
		// 其它，如整数、日期
		} else {

			kv := strings.Split(indexField, ".")
			var score string
			if fieldtype == REDISQL_TYPE_DATE {
				t, err := time.Parse("2006-01-02 15:04:05", kv[1])
				if err != nil {
					return err
				}
				score = t.Format("20060102150405")
			} else {
				//
				score = kv[1]
			}

			// 把 ids 添加到 {database}.{table}.{index_key} 的 zset 中。
			_, err := conn.Do("ZADD", fmt.Sprintf(REDISQL_INDEX_DATAS, database, tab.Name, kv[0]), score, ids[len(ids)-1])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

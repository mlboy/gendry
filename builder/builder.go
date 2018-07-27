package builder

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var (
	errSplitEmptyKey = errors.New("[builder] couldn't split a empty string")
	errSplitOrderBy  = errors.New(`[builder] the value of _orderby should be "fieldName direction [,fieldName direction]"`)
	// ErrUnsupportedOperator reports there's unsupported operators in where-condition
	ErrUnsupportedOperator       = errors.New("[builder] unsupported operator")
	errWhereInType               = errors.New(`[builder] the value of "xxx in" must be of []interface{} type`)
	errGroupByValueType          = errors.New(`[builder] the value of "_groupby" must be of string type`)
	errLimitValueType            = errors.New(`[builder] the value of "_limit" must be of []uint type`)
	errLimitValueLength          = errors.New(`[builder] the value of "_limit" must contain one or two uint elements`)
	errEmptyINCondition          = errors.New(`[builder] the value of "in" must contain at least one element`)
	errHavingValueType           = errors.New(`[builder] the value of "_having" must be of map[string]interface{}`)
	errHavingUnsupportedOperator = errors.New(`[builder] "_having" contains unsupported operator`)
)
var tagKey = "db"
var identKey = "`"

//Table ...
type Table struct {
	Name  string
	Alias string
}

//SQLSegments ...
type SQLSegments struct {
	table   []Table
	fields  []string
	flags   []string
	join    []map[string]string
	where   Clause
	groupBy []string
	having  Clause
	orderBy []string
	limit   struct {
		limit  int
		offset int
	}
	union     []func(*SQLSegments)
	forUpdate bool
	returning bool
	params    []interface{}
	render    struct {
		args []interface{}
	}
}

//NewSQLSegment ...
func NewSQLSegment() *SQLSegments {
	return &SQLSegments{}
}

//Table SQLSegments
func (s *SQLSegments) Table(name interface{}) *SQLSegments {
	switch v := name.(type) {
	case Table:
		s.table = append(s.table, v)
	case []Table:
		s.table = append(s.table, v...)
	case string:
		var t = &Table{v, ""}
		s.table = append(s.table, *t)
	}
	return s
}

//Field SQLSegments
func (s *SQLSegments) Field(fields ...string) *SQLSegments {
	if len(fields) > 0 {
		s.fields = append(s.fields, fields...)
	}
	return s
}

//Flag SQLSegments
func (s *SQLSegments) Flag(flags ...string) *SQLSegments {
	if len(flags) > 0 {
		s.flags = append(s.flags, flags...)
	}
	return s
}

//Join SQLSegments
func (s *SQLSegments) Join(table string, conditionA, logic, conditionB string) *SQLSegments {
	s.addJoin("", table, conditionA, logic, conditionB)
	return s
}

//LeftJoin SQLSegments
func (s *SQLSegments) LeftJoin(table string, conditionA, logic, conditionB string) *SQLSegments {
	s.addJoin("LEFT", table, conditionA, logic, conditionB)
	return s
}

//RightJoin SQLSegments
func (s *SQLSegments) RightJoin(table string, conditionA, logic, conditionB string) *SQLSegments {
	s.addJoin("RIGHT", table, conditionA, logic, conditionB)
	return s
}

//InnerJoin SQLSegments
func (s *SQLSegments) InnerJoin(table string, conditionA, logic, conditionB string) *SQLSegments {
	s.addJoin("INNER", table, conditionA, logic, conditionB)
	return s
}

//CorssJoin SQLSegments
func (s *SQLSegments) CorssJoin(table string, conditionA, logic, conditionB string) *SQLSegments {
	s.addJoin("CROSS", table, conditionA, logic, conditionB)
	return s
}

//addJoin SQLSegments
func (s *SQLSegments) addJoin(typ string, table string, conditionA, logic, conditionB string) *SQLSegments {
	var t = make(map[string]string)
	t["type"] = typ
	t["table"] = table
	t["logic"] = logic
	t["conditionA"] = conditionA
	t["conditionB"] = conditionB
	s.join = append(s.join, t)
	return s
}

//OrderBy SQLSegments
func (s *SQLSegments) OrderBy(fields ...string) *SQLSegments {
	if len(fields) > 0 {
		s.orderBy = append(s.orderBy, fields...)
	}
	return s
}

//GroupBy SQLSegments
func (s *SQLSegments) GroupBy(fields ...string) *SQLSegments {
	if len(fields) > 0 {
		s.groupBy = append(s.groupBy, fields...)
	}
	return s
}

//Offset SQLSegments
func (s *SQLSegments) Offset(n int) *SQLSegments {
	s.limit.offset = n
	return s
}

//Limit SQLSegments
func (s *SQLSegments) Limit(n int) *SQLSegments {
	s.limit.limit = n
	return s
}

//ForUpdate SQLSegments
func (s *SQLSegments) ForUpdate() *SQLSegments {
	s.forUpdate = true
	return s
}

//todo Union SQLSegments

//Clause ...
type Clause struct {
	key    interface{}
	val    interface{}
	logic  string
	clause []*Clause
}

func (p *Clause) addClause(logic string, key interface{}, vals ...interface{}) *Clause {
	var c = &Clause{}
	c.logic = logic
	switch k := key.(type) {
	case func(*Clause):
		k(c)
		// p.clause = append(p.clause, c)
	default:
		c.key = key
		if len(vals) > 0 {
			c.val = vals[0]
		}
	}
	// fmt.Println(p.clause)
	p.clause = append(p.clause, c)
	return p
}

//Where ..
func (p *Clause) Where(key interface{}, vals ...interface{}) *Clause {
	p.addClause("AND", key, vals...)
	return p
}

//OrWhere ..
func (p *Clause) OrWhere(key interface{}, vals ...interface{}) *Clause {
	p.addClause("OR", key, vals...)
	return p
}

//Build ...
func (p *Clause) Build(i int) (string, []interface{}) {
	var sql = ""
	var args []interface{}
	if p.logic != "" && i > 0 {
		sql += " " + p.logic
	}
	switch k := p.key.(type) {
	case string:
		r, _ := regexp.Compile(`\[(\>\=|\<\=|\>|\<|\<\>|\!\=|\=|\~|\!\~|like|!like|in|!in|is|!is|exists|!exists|#)\]?([a-zA-Z0-9_.\-\=\s\?\(\)]*)`)
		match := r.FindStringSubmatch(k)
		var context string
		if len(match) > 0 {
			// fmt.Println(len(match), match[1])
			switch match[1] {
			case "~", "like":
				context = buildIdent(match[2]) + " LIKE ?"
				args = append(args, p.val)
			case "!~", "!like":
				context = buildIdent(match[2]) + "` NOT LIKE ?"
				args = append(args, p.val)
			case ">":
				context = buildIdent(match[2]) + " > ?"
				args = append(args, p.val)
			case ">=":
				context = buildIdent(match[2]) + "` >= ?"
				args = append(args, p.val)
			case "<":
				context = buildIdent(match[2]) + " < ?"
				args = append(args, p.val)
			case "<=":
				context = buildIdent(match[2]) + " <= ?"
				args = append(args, p.val)
			case "<>", "!=":
				context = buildIdent(match[2]) + " != ?"
				args = append(args, p.val)
			case "=":
				context = buildIdent(match[2]) + " = ?"
				args = append(args, p.val)
			case "in":
				context = buildIdent(match[2]) + " IN (" + buildPlaceholder(p.val) + ")"
				if reflect.TypeOf(p.val).Kind() == reflect.Slice {
					v := reflect.ValueOf(p.val)
					for n := 0; n < v.Len(); n++ {
						args = append(args, v.Index(n).Interface())
					}
				} else {
					args = append(args, p.val)
				}
			case "!in":
				context = buildIdent(match[2]) + " NOT IN (" + buildPlaceholder(p.val) + ")"
				if reflect.TypeOf(p.val).Kind() == reflect.Slice {
					v := reflect.ValueOf(p.val)
					for n := 0; n < v.Len(); n++ {
						args = append(args, v.Index(n).Interface())
					}
				} else {
					args = append(args, p.val)
				}
			case "exists":
				switch p.val.(type) {
				case string:
					context = "EXISTS (" + p.val.(string) + ")"
				case func(s *SQLSegments):
					s := NewSQLSegment()
					p.val.(func(s *SQLSegments))(s)
					context = "EXISTS (" + s.BuildSelect() + ")"
					args = append(args, s.render.args...)
				}
			case "!exists":
				switch p.val.(type) {
				case string:
					context = "NOT EXISTS (" + p.val.(string) + ")"
				case func(s *SQLSegments):
					s := NewSQLSegment()
					p.val.(func(s *SQLSegments))(s)
					context = "NOT EXISTS (" + s.BuildSelect() + ")"
					args = append(args, s.render.args...)
				}
			case "is":
				if p.val == nil {
					context = buildIdent(match[2]) + " IS NULL"
				} else {
					context = buildIdent(match[2]) + " IS ?"
					args = append(args, p.val)
				}
			case "!is":
				if p.val == nil {
					context = buildIdent(match[2]) + " IS NOT NULL"
				} else {
					context = buildIdent(match[2]) + " IS NOT ?"
					args = append(args, p.val)
				}
			case "#":
				context = match[2]
				if reflect.TypeOf(p.val).Kind() == reflect.Slice {
					v := reflect.ValueOf(p.val)
					for n := 0; n < v.Len(); n++ {
						args = append(args, v.Index(n).Interface())
					}
				} else {
					args = append(args, p.val)
				}
			}
			sql += " " + context
		} else {
			if p.val != nil {
				sql += " " + buildIdent(k) + " = ?"
				args = append(args, p.val)
			} else {
				sql += " " + k
			}
		}
	case nil:
		sql += " ("
		for j, c := range p.clause {
			part, arg := c.Build(j)
			sql += part
			args = append(args, arg...)
		}
		sql += ")"
	}
	return sql, args
}

//Where ..
func (s *SQLSegments) Where(key interface{}, vals ...interface{}) *SQLSegments {
	s.where.Where(key, vals...)
	return s
}

//OrWhere ..
func (s *SQLSegments) OrWhere(key interface{}, vals ...interface{}) *SQLSegments {
	s.where.OrWhere(key, vals...)
	return s
}

//BuildWhereClause ...
func (s *SQLSegments) buildWhereClause() string {
	var sql string
	if len(s.where.clause) > 0 {
		sql = " WHERE"
		for i, c := range s.where.clause {
			part, args := c.Build(i)
			sql += part
			s.render.args = append(s.render.args, args...)
		}
	}
	return sql
}

//Having ...
func (s *SQLSegments) Having(key interface{}, vals ...interface{}) *SQLSegments {
	s.having.Where(key, vals...)
	return s
}

//buildHavingClause ...
func (s *SQLSegments) buildHavingClause() string {
	var sql string
	if len(s.having.clause) > 0 {
		sql = " WHERE"
		for i, c := range s.having.clause {
			part, args := c.Build(i)
			sql += part
			s.render.args = append(s.render.args, args...)
		}
	}
	return sql
}
func (s *SQLSegments) buildFlags() string {
	var sql string
	for _, v := range s.flags {
		sql += " " + v
	}
	return sql

}
func (s *SQLSegments) buildField() string {
	var sql string
	if len(s.fields) == 0 {
		sql += " *"
	} else {
		for i, v := range s.fields {
			if i > 0 {
				sql += ","
			}
			if v == "*" {
				sql += " " + v
			} else {
				sql += " " + buildIdent(v)
			}

		}
	}
	return sql
}
func (s *SQLSegments) buildTable() string {
	var sql string
	for i, v := range s.table {
		if i > 0 {
			sql += ","
		}
		sql += " " + buildIdent(v.Name)
		if v.Alias != "" {
			sql += " AS " + buildIdent(v.Alias)
		}
	}
	return sql
}
func (s *SQLSegments) buildJoin() string {
	var sql string
	for _, t := range s.join {
		sql += " " + t["type"] + "JOIN " + buildIdent(t["table"]) + " ON " + buildIdent(t["conditionA"]) + " " + t["logic"] + " " + buildIdent(t["conditionB"])
	}
	return sql
}
func (s *SQLSegments) buildGroupBy() string {
	var sql string
	if len(s.groupBy) > 0 {
		sql += " GROUP BY"
	}
	for i, v := range s.groupBy {
		if i > 0 {
			sql += ","
		}
		sql += " " + buildIdent(v)
	}
	return sql
}
func (s *SQLSegments) buildOrderBy() string {
	var sql string
	if len(s.orderBy) > 0 {
		sql += " ORDER BY"
	}
	for i, v := range s.orderBy {
		if i > 0 {
			sql += ","
		}
		sql += " " + buildIdent(v)
	}
	return sql
}
func (s *SQLSegments) buildLimit() string {
	var sql string
	if s.limit.offset != 0 {
		sql += fmt.Sprintf(" OFFSET %d", s.limit.offset)
	}
	if s.limit.offset != 0 {
		sql += fmt.Sprintf(" LIMIT %d", s.limit.limit)
	}
	return sql
}

//Union ...
func (s *SQLSegments) Union(f func(*SQLSegments)) *SQLSegments {
	s.union = append(s.union, f)
	return s
}
func (s *SQLSegments) buildUnion() string {
	var sql string
	if len(s.union) > 0 {
		sql += " UNION ("
	}
	for _, f := range s.union {
		var ss = &SQLSegments{}
		f(ss)
		sql += ss.BuildSelect()
	}
	if len(s.union) > 0 {
		sql += ")"
	}
	return sql
}
func (s *SQLSegments) buildForUpdate() string {
	if s.forUpdate == true {
		return " FOR UPDATE"
	}
	return ""
}

//BuildSelect ...
func (s *SQLSegments) BuildSelect() string {
	var sql = fmt.Sprintf("SELECT%s%s FORM%s%s%s%s%s%s%s%s%s",
		s.buildFlags(),
		s.buildField(),
		s.buildTable(),
		s.buildJoin(),
		s.buildWhereClause(),
		s.buildGroupBy(),
		s.buildHavingClause(),
		s.buildOrderBy(),
		s.buildLimit(),
		s.buildUnion(),
		s.buildForUpdate(),
	)
	fmt.Println(s.render.args)
	return sql
}

//Insert ...
func (s *SQLSegments) Insert(params ...interface{}) *SQLSegments {
	s.params = append(s.params, params...)
	return s
}

//BuildInsert ...
func (s *SQLSegments) BuildInsert() string {
	var sql = fmt.Sprintf("INSERT%s INTO%s%s%s",
		s.buildFlags(),
		s.buildTable(),
		s.buildValuesForInsert(),
		s.buildReturning(),
	)
	return sql
}

//BuildReplace ...
func (s *SQLSegments) BuildReplace() string {
	var sql = fmt.Sprintf("REPLACE%s INTO%s%s%s",
		s.buildFlags(),
		s.buildTable(),
		s.buildValuesForInsert(),
		s.buildReturning(),
	)
	return sql
}

//BuildInsert ...
func (s *SQLSegments) buildValuesForInsert() string {
	var fields string
	var values string
	for i, param := range s.params {
		v := reflect.ValueOf(param).Elem()
		t := reflect.TypeOf(param).Elem()
		if i == 0 {
			values += " ("
			fields += " ("
		} else {
			values += ",("
		}
		for j := 0; j < v.NumField(); j++ {
			if v.Interface() == nil {
				continue
			}
			var arg string
			if t.Field(j).Tag.Get(tagKey) == "" {
				arg = t.Field(j).Name
			} else {
				arg = t.Field(j).Tag.Get(tagKey)
			}
			s.render.args = append(s.render.args, v.Field(j).Interface())
			// if v.Field(j).Kind() == reflect.String {
			// 	// fmt.Printf(3"t:%v      v:%+v", arg, v.Field(j).Interface().(string))
			// }
			if j > 0 {
				values += ","
			}
			values += "?"
			if i == 0 {
				if j > 0 {
					fields += ","
				}
				fields += arg
			}
		}
		if i == 0 {
			fields += ")"
		}
		values += ")"
	}
	var sql = fields + " VALUES" + values
	return sql
}

//Update ...
func (s *SQLSegments) Update(params ...interface{}) *SQLSegments {
	s.params = append(s.params, params...)
	return s
}

//buildReturning ...
func (s *SQLSegments) buildReturning() string {
	if s.returning == true {
		return " RETURNING"
	}
	return ""
}

//BuildUpdate ...
func (s *SQLSegments) BuildUpdate() string {
	var sql = fmt.Sprintf("UPDATE%s%s%s%s%s%s%s",
		s.buildFlags(),
		s.buildTable(),
		s.buildValuesForUpdate(),
		s.buildWhereClause(),
		s.buildOrderBy(),
		s.buildLimit(),
		s.buildReturning(),
	)
	// fmt.Println(s.render.args)
	return sql
}

//buildValuesForUpdate ...
func (s *SQLSegments) buildValuesForUpdate() string {
	var sql = " SET"
	for i, param := range s.params {
		v := reflect.ValueOf(param).Elem()
		t := reflect.TypeOf(param).Elem()
		if i == 0 {
			for j := 0; j < v.NumField(); j++ {
				if v.Interface() == nil {
					continue
				}
				var arg string
				if t.Field(j).Tag.Get(tagKey) == "" {
					arg = t.Field(j).Name
				} else {
					arg = t.Field(j).Tag.Get(tagKey)
				}
				s.render.args = append(s.render.args, v.Field(j).Interface())
				if j > 0 {
					sql += ","
				} else {
					sql += " "
				}
				sql += arg + " = ?"
			}
		} else {
			break
		}
	}
	return sql
}

//Delete ...
func (s *SQLSegments) Delete() *SQLSegments {
	return s
}

//BuildDelete ...
func (s *SQLSegments) BuildDelete() string {
	var sql = fmt.Sprintf("DELETE%s FROM%s%s%s%s%s",
		s.buildFlags(),
		s.buildTable(),
		s.buildWhereClause(),
		s.buildOrderBy(),
		s.buildLimit(),
		s.buildReturning(),
	)
	// fmt.Println(s.render.args)
	return sql
}

//buildIdent
func buildIdent(name string) string {
	return identKey + strings.Replace(name, ".", identKey+"."+identKey, -1) + identKey
}

//buildPlaceholder
func buildPlaceholder(val interface{}) string {
	var sql string
	if reflect.TypeOf(val).Kind() == reflect.Slice {
		for n := 0; n < reflect.ValueOf(val).Len(); n++ {
			if n > 0 {
				sql += ", ?"
			} else {
				sql += "?"
			}
		}
	} else {
		sql += "?"
	}
	return sql
}

type whereMapSet struct {
	set map[string]map[string]interface{}
}

func (w *whereMapSet) add(op, field string, val interface{}) {
	if nil == w.set {
		w.set = make(map[string]map[string]interface{})
	}
	s, ok := w.set[op]
	if !ok {
		s = make(map[string]interface{})
		w.set[op] = s
	}
	s[field] = val
}

type eleOrderBy struct {
	field, order string
}

type eleLimit struct {
	begin, step uint
}

// BuildSelect work as its name says.
// supported operators including: =,in,>,>=,<,<=,<>,!=.
// key without operator will be regarded as =.
// special key begin with _: _orderby,_groupby,_limit,_having.
// the value of _orderby must be a string separated by a space(ie:map[string]interface{}{"_orderby": "fieldName desc"}).
// the value of _limit must be a slice whose type should be []uint and must contain two uints(ie: []uint{0, 100}).
// the value of _having must be a map just like where but only support =,in,>,>=,<,<=,<>,!=
// for more examples,see README.md or open a issue.
func BuildSelect(table string, where map[string]interface{}, selectField []string) (cond string, vals []interface{}, err error) {
	var orderBy []eleOrderBy
	var limit *eleLimit
	var groupBy string
	var having map[string]interface{}
	copiedWhere := copyWhere(where)
	if val, ok := copiedWhere["_orderby"]; ok {
		eleOrderBy, e := splitOrderBy(val.(string))
		if e != nil {
			err = e
			return
		}
		orderBy = eleOrderBy
		delete(copiedWhere, "_orderby")
	}
	if val, ok := copiedWhere["_groupby"]; ok {
		s, ok := val.(string)
		if !ok {
			err = errGroupByValueType
			return
		}
		groupBy = s
		delete(copiedWhere, "_groupby")
		if h, ok := copiedWhere["_having"]; ok {
			having, err = resolveHaving(h)
			if nil != err {
				return
			}
		}
	}
	if _, ok := copiedWhere["_having"]; ok {
		delete(copiedWhere, "_having")
	}
	if val, ok := copiedWhere["_limit"]; ok {
		arr, ok := val.([]uint)
		if !ok {
			err = errLimitValueType
			return
		}
		if len(arr) != 2 {
			if len(arr) == 1 {
				arr = []uint{0, arr[0]}
			} else {
				err = errLimitValueLength
				return
			}
		}
		begin, step := arr[0], arr[1]
		limit = &eleLimit{
			begin: begin,
			step:  step,
		}
		delete(copiedWhere, "_limit")
	}
	conditions, release, err := getWhereConditions(copiedWhere)
	if nil != err {
		return
	}
	defer release()
	if having != nil {
		havingCondition, release1, err1 := getWhereConditions(having)
		if nil != err1 {
			err = err1
			return
		}
		defer release1()
		conditions = append(conditions, nilComparable(0))
		conditions = append(conditions, havingCondition...)
	}
	return buildSelect(table, selectField, groupBy, orderBy, limit, conditions...)
}

func copyWhere(src map[string]interface{}) (target map[string]interface{}) {
	target = make(map[string]interface{})
	for k, v := range src {
		target[k] = v
	}
	return
}

func resolveHaving(having interface{}) (map[string]interface{}, error) {
	var havingMap map[string]interface{}
	var ok bool
	if havingMap, ok = having.(map[string]interface{}); !ok {
		return nil, errHavingValueType
	}
	copiedMap := make(map[string]interface{})
	for key, val := range havingMap {
		_, operator, err := splitKey(key)
		if nil != err {
			return nil, err
		}
		if !isStringInSlice(operator, opOrder) {
			return nil, errHavingUnsupportedOperator
		}
		copiedMap[key] = val
	}
	return copiedMap, nil
}

// BuildUpdate work as its name says
func BuildUpdate(table string, where map[string]interface{}, update map[string]interface{}) (string, []interface{}, error) {
	conditions, release, err := getWhereConditions(where)
	if nil != err {
		return "", nil, err
	}
	defer release()
	return buildUpdate(table, update, conditions...)
}

// BuildDelete work as its name says
func BuildDelete(table string, where map[string]interface{}) (string, []interface{}, error) {
	conditions, release, err := getWhereConditions(where)
	if nil != err {
		return "", nil, err
	}
	defer release()
	return buildDelete(table, conditions...)
}

// BuildInsert work as its name says
func BuildInsert(table string, data []map[string]interface{}) (string, []interface{}, error) {
	return buildInsert(table, data)
}

var (
	cpPool = sync.Pool{
		New: func() interface{} {
			return make([]Comparable, 0)
		},
	}
)

func getCpPool() ([]Comparable, func()) {
	obj := cpPool.Get().([]Comparable)
	return obj[:0], func() { cpPool.Put(obj) }
}

func emptyFunc() {}

func isStringInSlice(str string, arr []string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

func getWhereConditions(where map[string]interface{}) ([]Comparable, func(), error) {
	if len(where) == 0 {
		return nil, emptyFunc, nil
	}
	wms := &whereMapSet{}
	var field, operator string
	var err error
	for key, val := range where {
		field, operator, err = splitKey(key)
		if !isStringInSlice(operator, opOrder) {
			return nil, emptyFunc, ErrUnsupportedOperator
		}
		if nil != err {
			return nil, emptyFunc, err
		}
		if _, ok := val.(NullType); ok {
			operator = opNull
		}
		wms.add(operator, field, val)
	}

	return buildWhereCondition(wms)
}

const (
	opEq   = "="
	opNe1  = "!="
	opNe2  = "<>"
	opIn   = "in"
	opGt   = ">"
	opGte  = ">="
	opLt   = "<"
	opLte  = "<="
	opLike = "like"
	// special
	opNull = "null"
)

type compareProducer func(m map[string]interface{}) (Comparable, error)

var op2Comparable = map[string]compareProducer{
	opEq: func(m map[string]interface{}) (Comparable, error) {
		return Eq(m), nil
	},
	opNe1: func(m map[string]interface{}) (Comparable, error) {
		return Ne(m), nil
	},
	opNe2: func(m map[string]interface{}) (Comparable, error) {
		return Ne(m), nil
	},
	opIn: func(m map[string]interface{}) (Comparable, error) {
		wp, err := convertWhereMapToWhereMapSlice(m)
		if nil != err {
			return nil, err
		}
		return In(wp), nil
	},
	opGt: func(m map[string]interface{}) (Comparable, error) {
		return Gt(m), nil
	},
	opGte: func(m map[string]interface{}) (Comparable, error) {
		return Gte(m), nil
	},
	opLt: func(m map[string]interface{}) (Comparable, error) {
		return Lt(m), nil
	},
	opLte: func(m map[string]interface{}) (Comparable, error) {
		return Lte(m), nil
	},
	opLike: func(m map[string]interface{}) (Comparable, error) {
		return Like(m), nil
	},
	opNull: func(m map[string]interface{}) (Comparable, error) {
		return nullCompareble(m), nil
	},
}

var opOrder = []string{opEq, opIn, opNe1, opNe2, opGt, opGte, opLt, opLte, opLike, opNull}

func buildWhereCondition(mapSet *whereMapSet) ([]Comparable, func(), error) {
	cpArr, release := getCpPool()
	for _, operator := range opOrder {
		whereMap, ok := mapSet.set[operator]
		if !ok {
			continue
		}
		f, ok := op2Comparable[operator]
		if !ok {
			release()
			return nil, emptyFunc, ErrUnsupportedOperator
		}
		cp, err := f(whereMap)
		if nil != err {
			release()
			return nil, emptyFunc, err
		}
		cpArr = append(cpArr, cp)
	}
	return cpArr, release, nil
}

func convertWhereMapToWhereMapSlice(where map[string]interface{}) (map[string][]interface{}, error) {
	result := make(map[string][]interface{})
	for key, val := range where {
		vals, ok := convertInterfaceToMap(val)
		if !ok {
			return nil, errWhereInType
		}
		if 0 == len(vals) {
			return nil, errEmptyINCondition
		}
		result[key] = vals
	}
	return result, nil
}

func convertInterfaceToMap(val interface{}) ([]interface{}, bool) {
	s := reflect.ValueOf(val)
	if s.Kind() != reflect.Slice {
		return nil, false
	}
	interfaceSlice := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		interfaceSlice[i] = s.Index(i).Interface()
	}
	return interfaceSlice, true
}

func splitKey(key string) (field string, operator string, err error) {
	key = strings.Trim(key, " ")
	if "" == key {
		err = errSplitEmptyKey
		return
	}
	idx := strings.IndexByte(key, ' ')
	if idx == -1 {
		field = key
		operator = "="
	} else {
		field = key[:idx]
		operator = strings.Trim(key[idx+1:], " ")
	}
	return
}

func splitOrderBy(orderby string) ([]eleOrderBy, error) {
	var err error
	var eleOrder []eleOrderBy
	for _, val := range strings.Split(orderby, ",") {
		val = strings.Trim(val, " ")
		idx := strings.IndexByte(val, ' ')
		if idx == -1 {
			err = errSplitOrderBy
			return eleOrder, err
		}
		field := val[:idx]
		direction := strings.Trim(val[idx+1:], " ")
		eleOrder = append(eleOrder, eleOrderBy{
			field: field,
			order: direction,
		})
	}
	return eleOrder, err
}

const (
	paramPlaceHolder = "?"
)

var searchHandle = regexp.MustCompile(`{{\S+}}`)

// NamedQuery is used for expressing complex query
func NamedQuery(sql string, data map[string]interface{}) (string, []interface{}, error) {
	length := len(data)
	if length == 0 {
		return sql, nil, nil
	}
	vals := make([]interface{}, 0, length)
	var err error
	cond := searchHandle.ReplaceAllStringFunc(sql, func(paramName string) string {
		paramName = strings.TrimRight(strings.TrimLeft(paramName, "{"), "}")
		val, ok := data[paramName]
		if !ok {
			err = fmt.Errorf("%s not found", paramName)
			return ""
		}
		v := reflect.ValueOf(val)
		if v.Type().Kind() != reflect.Slice {
			vals = append(vals, val)
			return paramPlaceHolder
		}
		length := v.Len()
		for i := 0; i < length; i++ {
			vals = append(vals, v.Index(i).Interface())
		}
		return createMultiPlaceholders(length)
	})
	if nil != err {
		return "", nil, err
	}
	return cond, vals, nil
}

func createMultiPlaceholders(num int) string {
	if 0 == num {
		return ""
	}
	length := (num << 1) | 1
	buff := make([]byte, length)
	buff[0], buff[length-1] = '(', ')'
	ll := length - 2
	for i := 1; i <= ll; i += 2 {
		buff[i] = '?'
	}
	ll = length - 3
	for i := 2; i <= ll; i += 2 {
		buff[i] = ','
	}
	return string(buff)
}
